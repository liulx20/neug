#ifndef NEUG_ENABLE_JIT_EXPRESSION

#include "neug/execution/expression/codegen/expr_codegen.h"
#include "neug/execution/expression/accessors/const_accessor.h"
#include "neug/execution/expression/accessors/edge_accessor.h"
#include "neug/execution/expression/accessors/record_accessor.h"
#include "neug/execution/expression/accessors/vertex_accessor.h"
#include "neug/execution/expression/codegen/codegen_types.h"
#include "neug/execution/expression/codegen/jit_engine.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/exprs/arith_expr.h"
#include "neug/execution/expression/exprs/case_when.h"
#include "neug/execution/expression/exprs/extract_expr.h"
#include "neug/execution/expression/exprs/logical_expr.h"
#include "neug/execution/expression/exprs/udfs.h"

#include <glog/logging.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Verifier.h>

namespace neug {
namespace execution {
namespace codegen {

// ============================================================================
// Extern "C" trampoline functions called from JIT code to evaluate leaf nodes.
// These bridge from JIT-compiled native code back into the C++ interpreter.
// ============================================================================

// Trampoline: evaluate a leaf expression and extract a primitive value.
// The JIT code calls this with:
//   - leaf_slots_ptr: pointer to LeafExprSlots
//   - leaf_index: index into the leaves vector
//   - ctx_ptr: pointer to Context
//   - idx: row index
// Returns: the primitive value (or 0 if null)
// The is_null flag is written to *out_is_null.

#define DEFINE_LEAF_TRAMPOLINE(SUFFIX, CPP_TYPE)                            \
  extern "C" void neug_jit_eval_leaf_##SUFFIX(                              \
      const void* leaf_slots_ptr, uint64_t leaf_index, const void* ctx_ptr, \
      uint64_t idx, CPP_TYPE* out_value, int8_t* out_is_null) {             \
    const auto* slots =                                                     \
        reinterpret_cast<const LeafExprSlots*>(leaf_slots_ptr);             \
    const auto& leaf = slots->leaves[leaf_index];                           \
    const auto& ctx = *reinterpret_cast<const Context*>(ctx_ptr);           \
    CPP_TYPE result{};                                                      \
    bool is_null =                                                          \
        leaf->Cast<RecordExprBase>().typed_eval_record(ctx, idx, &result);  \
    *out_value = result;                                                    \
    *out_is_null = is_null ? 1 : 0;                                         \
  }

DEFINE_LEAF_TRAMPOLINE(bool, bool)
DEFINE_LEAF_TRAMPOLINE(i32, int32_t)
DEFINE_LEAF_TRAMPOLINE(i64, int64_t)
DEFINE_LEAF_TRAMPOLINE(u32, uint32_t)
DEFINE_LEAF_TRAMPOLINE(u64, uint64_t)
DEFINE_LEAF_TRAMPOLINE(f32, float)
DEFINE_LEAF_TRAMPOLINE(f64, double)
DEFINE_LEAF_TRAMPOLINE(datetime, DateTime)
DEFINE_LEAF_TRAMPOLINE(date, Date)

#undef DEFINE_LEAF_TRAMPOLINE

// Vertex trampoline: same 6-arg signature as record trampoline, but
// interprets ctx_ptr as label (ptrtoint -> uint8) and idx as vid (uint32).
// This allows generateLeafCallIR to be reused unchanged.
#define DEFINE_VERTEX_LEAF_TRAMPOLINE(SUFFIX, CPP_TYPE)                      \
  extern "C" void neug_jit_eval_vertex_leaf_##SUFFIX(                        \
      const void* leaf_slots_ptr, uint64_t leaf_index,                       \
      const void* label_as_ptr, uint64_t vid_as_u64, CPP_TYPE* out_value,    \
      int8_t* out_is_null) {                                                 \
    const auto* slots =                                                      \
        reinterpret_cast<const LeafExprSlots*>(leaf_slots_ptr);              \
    const auto& leaf = slots->leaves[leaf_index];                            \
    label_t label =                                                          \
        static_cast<label_t>(reinterpret_cast<uintptr_t>(label_as_ptr));     \
    vid_t vid = static_cast<vid_t>(vid_as_u64);                              \
    CPP_TYPE result{};                                                       \
    bool is_null =                                                           \
        leaf->Cast<VertexExprBase>().typed_eval_vertex(label, vid, &result); \
    *out_value = result;                                                     \
    *out_is_null = is_null ? 1 : 0;                                          \
  }

DEFINE_VERTEX_LEAF_TRAMPOLINE(bool, bool)
DEFINE_VERTEX_LEAF_TRAMPOLINE(i32, int32_t)
DEFINE_VERTEX_LEAF_TRAMPOLINE(i64, int64_t)
DEFINE_VERTEX_LEAF_TRAMPOLINE(u32, uint32_t)
DEFINE_VERTEX_LEAF_TRAMPOLINE(u64, uint64_t)
DEFINE_VERTEX_LEAF_TRAMPOLINE(f32, float)
DEFINE_VERTEX_LEAF_TRAMPOLINE(f64, double)
DEFINE_VERTEX_LEAF_TRAMPOLINE(datetime, DateTime)
DEFINE_VERTEX_LEAF_TRAMPOLINE(date, Date)

#undef DEFINE_VERTEX_LEAF_TRAMPOLINE

// Edge trampoline: same 6-arg signature as record trampoline, but
// interprets ctx_ptr as pointer to a stack-allocated EdgeArgs struct:
//   struct { void* triplet_ptr; uint32_t src; uint32_t dst; void* edata_ptr; }
// idx is unused (always 0).
struct EdgeArgs {
  const void* triplet_ptr;
  uint32_t src;
  uint32_t dst;
  const void* edata_ptr;
};

#define DEFINE_EDGE_LEAF_TRAMPOLINE(SUFFIX, CPP_TYPE)                      \
  extern "C" void neug_jit_eval_edge_leaf_##SUFFIX(                        \
      const void* leaf_slots_ptr, uint64_t leaf_index,                     \
      const void* edge_args_ptr, uint64_t /*unused*/, CPP_TYPE* out_value, \
      int8_t* out_is_null) {                                               \
    const auto* slots =                                                    \
        reinterpret_cast<const LeafExprSlots*>(leaf_slots_ptr);            \
    const auto& leaf = slots->leaves[leaf_index];                          \
    const auto* ea = reinterpret_cast<const EdgeArgs*>(edge_args_ptr);     \
    const auto& triplet =                                                  \
        *reinterpret_cast<const LabelTriplet*>(ea->triplet_ptr);           \
    CPP_TYPE result{};                                                     \
    bool is_null = leaf->Cast<EdgeExprBase>().typed_eval_edge(             \
        triplet, static_cast<vid_t>(ea->src), static_cast<vid_t>(ea->dst), \
        ea->edata_ptr, &result);                                           \
    *out_value = result;                                                   \
    *out_is_null = is_null ? 1 : 0;                                        \
  }

DEFINE_EDGE_LEAF_TRAMPOLINE(bool, bool)
DEFINE_EDGE_LEAF_TRAMPOLINE(i32, int32_t)
DEFINE_EDGE_LEAF_TRAMPOLINE(i64, int64_t)
DEFINE_EDGE_LEAF_TRAMPOLINE(u32, uint32_t)
DEFINE_EDGE_LEAF_TRAMPOLINE(u64, uint64_t)
DEFINE_EDGE_LEAF_TRAMPOLINE(f32, float)
DEFINE_EDGE_LEAF_TRAMPOLINE(f64, double)
DEFINE_EDGE_LEAF_TRAMPOLINE(datetime, DateTime)
DEFINE_EDGE_LEAF_TRAMPOLINE(date, Date)

#undef DEFINE_EDGE_LEAF_TRAMPOLINE

// ============================================================================
// ExprCodegen implementation
// ============================================================================

std::atomic<uint64_t> ExprCodegen::func_counter_{0};

ExprCodegen::ExprCodegen() = default;

bool ExprCodegen::isTypeSupported(DataTypeId type_id) {
  switch (type_id) {
  case DataTypeId::kBoolean:
  case DataTypeId::kInt32:
  case DataTypeId::kInt64:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
  case DataTypeId::kTimestampMs:
  case DataTypeId::kDate:
    return true;
  default:
    return false;
  }
}

bool ExprCodegen::canCodegen(const ExprBase* expr) {
  DataTypeId type_id = expr->type().id();

  // ArithExpr: recursively check children
  if (auto* arith = dynamic_cast<const ArithExpr*>(expr)) {
    return isTypeSupported(type_id) && canCodegen(arith->lhs()) &&
           canCodegen(arith->rhs());
  }

  // BinaryLogicalExpr: recursively check children
  if (auto* logic = dynamic_cast<const BinaryLogicalExpr*>(expr)) {
    return canCodegen(logic->lhs()) && canCodegen(logic->rhs());
  }

  // UnaryLogicalExpr: recursively check operand
  if (auto* unary = dynamic_cast<const UnaryLogicalExpr*>(expr)) {
    // ISNULL can work on any type (result is always bool)
    if (unary->logical() == ::common::Logical::ISNULL) {
      return true;
    }
    return canCodegen(unary->operand());
  }

  // CaseWhenExpr: recursively check all branches
  if (auto* case_when = dynamic_cast<const CaseWhenExpr*>(expr)) {
    if (!isTypeSupported(type_id))
      return false;
    for (const auto& wt : case_when->when_then_exprs()) {
      if (!canCodegen(wt.first.get()) || !canCodegen(wt.second.get())) {
        return false;
      }
    }
    return canCodegen(case_when->else_expr());
  }

  // WithInExpr: result is bool, but involves list — treat as leaf trampoline
  if (dynamic_cast<const WithInExpr*>(expr)) {
    return true;  // result is always bool, trampoline handles list internally
  }

  // ExtractExpr: result is int64, treat as leaf trampoline
  if (dynamic_cast<const ExtractExpr*>(expr)) {
    return isTypeSupported(type_id);
  }

  // ScalarFunctionExpr: treat as leaf trampoline if result is primitive
  if (dynamic_cast<const ScalarFunctionExpr*>(expr)) {
    return isTypeSupported(type_id);
  }

  // ConstExpr: always supported for primitive types
  if (dynamic_cast<const ConstExpr*>(expr)) {
    return isTypeSupported(type_id);
  }

  // ParamExpr: always supported for primitive types
  if (dynamic_cast<const ParamExpr*>(expr)) {
    return isTypeSupported(type_id);
  }

  // All accessor types (RecordAccessor, RecordVertexAccessor,
  // RecordEdgeAccessor, RecordPathAccessor, VertexAccessor, EdgeAccessor):
  // supported for primitive types via trampoline
  if (dynamic_cast<const RecordAccessor*>(expr) ||
      dynamic_cast<const RecordVertexAccessor*>(expr) ||
      dynamic_cast<const RecordEdgeAccessor*>(expr) ||
      dynamic_cast<const RecordPathAccessor*>(expr) ||
      dynamic_cast<const VertexAccessor*>(expr) ||
      dynamic_cast<const EdgeAccessor*>(expr)) {
    return isTypeSupported(type_id);
  }

  // Unknown expression type: not supported
  return false;
}

llvm::Type* ExprCodegen::getLLVMType(llvm::LLVMContext& ctx,
                                     DataTypeId type_id) {
  switch (type_id) {
  case DataTypeId::kBoolean:
    return llvm::Type::getInt1Ty(ctx);
  case DataTypeId::kInt32:
    return llvm::Type::getInt32Ty(ctx);
  case DataTypeId::kInt64:
    return llvm::Type::getInt64Ty(ctx);
  case DataTypeId::kUInt32:
    return llvm::Type::getInt32Ty(ctx);
  case DataTypeId::kUInt64:
    return llvm::Type::getInt64Ty(ctx);
  case DataTypeId::kFloat:
    return llvm::Type::getFloatTy(ctx);
  case DataTypeId::kDouble:
    return llvm::Type::getDoubleTy(ctx);
  case DataTypeId::kTimestampMs:
    return llvm::Type::getInt64Ty(ctx);  // DateTime { int64_t milli_second }
  case DataTypeId::kDate:
    return llvm::Type::getInt32Ty(ctx);  // Date { uint32_t integer }
  default:
    return nullptr;
  }
}

CodegenValue ExprCodegen::createNull(llvm::IRBuilder<>& builder,
                                     DataTypeId type_id) {
  llvm::Type* value_type = getLLVMType(builder.getContext(), type_id);
  return {llvm::Constant::getNullValue(value_type), builder.getInt8(1)};
}

CodegenValue ExprCodegen::createNonNull(llvm::IRBuilder<>& builder,
                                        llvm::Value* value) {
  return {value, builder.getInt8(0)};
}

// Trampoline function pointer type: all trampolines share the same C signature
// void(const void*, uint64_t, const void*, uint64_t, void*, int8_t*)
using TrampolineFnPtr = void (*)(const void*, uint64_t, const void*, uint64_t,
                                 void*, int8_t*);

// Get the trampoline function address for a given type and eval mode.
// Returns the absolute address of the trampoline function, which will be
// embedded directly into the JIT IR as an integer constant (inttoptr).
// This avoids any JIT symbol resolution issues.
static TrampolineFnPtr getTrampolineAddress(DataTypeId type_id, EvalMode mode) {
  if (mode == EvalMode::kVertex) {
    switch (type_id) {
    case DataTypeId::kBoolean:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_bool);
    case DataTypeId::kInt32:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_i32);
    case DataTypeId::kInt64:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_i64);
    case DataTypeId::kUInt32:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_u32);
    case DataTypeId::kUInt64:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_u64);
    case DataTypeId::kFloat:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_f32);
    case DataTypeId::kDouble:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_f64);
    case DataTypeId::kTimestampMs:
      return reinterpret_cast<TrampolineFnPtr>(
          &neug_jit_eval_vertex_leaf_datetime);
    case DataTypeId::kDate:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_vertex_leaf_date);
    default:
      return nullptr;
    }
  } else if (mode == EvalMode::kEdge) {
    switch (type_id) {
    case DataTypeId::kBoolean:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_bool);
    case DataTypeId::kInt32:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_i32);
    case DataTypeId::kInt64:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_i64);
    case DataTypeId::kUInt32:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_u32);
    case DataTypeId::kUInt64:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_u64);
    case DataTypeId::kFloat:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_f32);
    case DataTypeId::kDouble:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_f64);
    case DataTypeId::kTimestampMs:
      return reinterpret_cast<TrampolineFnPtr>(
          &neug_jit_eval_edge_leaf_datetime);
    case DataTypeId::kDate:
      return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_edge_leaf_date);
    default:
      return nullptr;
    }
  }
  // Default: record mode
  switch (type_id) {
  case DataTypeId::kBoolean:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_bool);
  case DataTypeId::kInt32:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_i32);
  case DataTypeId::kInt64:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_i64);
  case DataTypeId::kUInt32:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_u32);
  case DataTypeId::kUInt64:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_u64);
  case DataTypeId::kFloat:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_f32);
  case DataTypeId::kDouble:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_f64);
  case DataTypeId::kTimestampMs:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_datetime);
  case DataTypeId::kDate:
    return reinterpret_cast<TrampolineFnPtr>(&neug_jit_eval_leaf_date);
  default:
    return nullptr;
  }
}

CodegenValue ExprCodegen::generateLeafCallIR(const ExprBase* expr,
                                             llvm::IRBuilder<>& builder,
                                             llvm::Value* slots_ptr,
                                             llvm::Value* context_ptr,
                                             llvm::Value* idx_val,
                                             UnboundLeafSlots& unbound_leaves) {
  DataTypeId type_id = expr->type().id();
  llvm::LLVMContext& ctx = builder.getContext();

  // Record the unbound leaf expression pointer
  size_t leaf_index = unbound_leaves.leaves.size();
  unbound_leaves.leaves.push_back(expr);

  // Get the trampoline function's absolute address
  TrampolineFnPtr trampoline_addr = getTrampolineAddress(type_id, eval_mode_);
  llvm::Type* value_type = getLLVMType(ctx, type_id);

  // Trampoline signature:
  // void trampoline(const void* slots, uint64_t leaf_idx,
  //                 const void* ctx, uint64_t idx,
  //                 T* out_value, int8_t* out_is_null)
  llvm::Type* void_type = llvm::Type::getVoidTy(ctx);
  llvm::Type* ptr_type = llvm::Type::getInt8PtrTy(ctx);
  llvm::Type* i64_type = llvm::Type::getInt64Ty(ctx);

  llvm::FunctionType* trampoline_fn_type = llvm::FunctionType::get(
      void_type, {ptr_type, i64_type, ptr_type, i64_type, ptr_type, ptr_type},
      false);

  // Embed the trampoline address as an integer constant and convert to
  // a function pointer via inttoptr. This completely bypasses JIT symbol
  // resolution — the address is baked directly into the generated code.
  llvm::Value* addr_int = builder.getInt64(
      static_cast<uint64_t>(reinterpret_cast<uintptr_t>(trampoline_addr)));
  llvm::Value* fn_ptr = builder.CreateIntToPtr(
      addr_int, llvm::PointerType::get(trampoline_fn_type, 0),
      "trampoline_ptr");

  // Allocate stack space for output value and is_null flag
  llvm::Value* out_value_ptr =
      builder.CreateAlloca(value_type, nullptr, "out_val");
  llvm::Value* out_is_null_ptr =
      builder.CreateAlloca(llvm::Type::getInt8Ty(ctx), nullptr, "out_null");

  // Bitcast output pointers to i8* for the trampoline call
  llvm::Value* out_value_cast =
      builder.CreateBitCast(out_value_ptr, ptr_type, "out_val_cast");
  llvm::Value* out_null_cast =
      builder.CreateBitCast(out_is_null_ptr, ptr_type, "out_null_cast");

  // Call the trampoline via the embedded function pointer
  builder.CreateCall(trampoline_fn_type, fn_ptr,
                     {slots_ptr, builder.getInt64(leaf_index), context_ptr,
                      idx_val, out_value_cast, out_null_cast});

  // Load the results
  llvm::Value* result_value =
      builder.CreateLoad(value_type, out_value_ptr, "leaf_val");
  llvm::Value* result_is_null = builder.CreateLoad(
      llvm::Type::getInt8Ty(ctx), out_is_null_ptr, "leaf_null");

  return {result_value, result_is_null};
}

CodegenValue ExprCodegen::generateArithIR(const CodegenValue& lhs,
                                          const CodegenValue& rhs,
                                          DataTypeId result_type, int arith_op,
                                          llvm::IRBuilder<>& builder) {
  // Null propagation: if either operand is null, result is null
  llvm::Value* either_null = builder.CreateOr(
      builder.CreateICmpNE(lhs.is_null, builder.getInt8(0)),
      builder.CreateICmpNE(rhs.is_null, builder.getInt8(0)), "either_null");

  llvm::Value* result_value = nullptr;
  bool is_float =
      (result_type == DataTypeId::kFloat || result_type == DataTypeId::kDouble);

  switch (static_cast<::common::Arithmetic>(arith_op)) {
  case ::common::Arithmetic::ADD:
    result_value = is_float ? builder.CreateFAdd(lhs.value, rhs.value, "fadd")
                            : builder.CreateAdd(lhs.value, rhs.value, "add");
    break;
  case ::common::Arithmetic::SUB:
    result_value = is_float ? builder.CreateFSub(lhs.value, rhs.value, "fsub")
                            : builder.CreateSub(lhs.value, rhs.value, "sub");
    break;
  case ::common::Arithmetic::MUL:
    result_value = is_float ? builder.CreateFMul(lhs.value, rhs.value, "fmul")
                            : builder.CreateMul(lhs.value, rhs.value, "mul");
    break;
  case ::common::Arithmetic::DIV:
    result_value = is_float ? builder.CreateFDiv(lhs.value, rhs.value, "fdiv")
                            : builder.CreateSDiv(lhs.value, rhs.value, "sdiv");
    break;
  case ::common::Arithmetic::MOD:
    result_value = is_float ? builder.CreateFRem(lhs.value, rhs.value, "frem")
                            : builder.CreateSRem(lhs.value, rhs.value, "srem");
    break;
  default:
    return createNull(builder, result_type);
  }

  // Select: if either is null, return null value; otherwise return computed
  llvm::Type* value_type = getLLVMType(builder.getContext(), result_type);
  llvm::Value* null_val = llvm::Constant::getNullValue(value_type);
  llvm::Value* final_value =
      builder.CreateSelect(either_null, null_val, result_value, "arith_val");
  llvm::Value* final_null = builder.CreateSelect(
      either_null, builder.getInt8(1), builder.getInt8(0), "arith_null");

  return {final_value, final_null};
}

CodegenValue ExprCodegen::generateCompareIR(const CodegenValue& lhs,
                                            const CodegenValue& rhs,
                                            int logical_op,
                                            DataTypeId operand_type,
                                            llvm::IRBuilder<>& builder) {
  // Null propagation: if either is null, result is null (boolean)
  llvm::Value* either_null = builder.CreateOr(
      builder.CreateICmpNE(lhs.is_null, builder.getInt8(0)),
      builder.CreateICmpNE(rhs.is_null, builder.getInt8(0)), "cmp_either_null");

  bool is_float = (operand_type == DataTypeId::kFloat ||
                   operand_type == DataTypeId::kDouble);

  llvm::Value* cmp_result = nullptr;
  auto op = static_cast<::common::Logical>(logical_op);

  switch (op) {
  case ::common::Logical::LT:
    cmp_result = is_float ? builder.CreateFCmpOLT(lhs.value, rhs.value, "flt")
                          : builder.CreateICmpSLT(lhs.value, rhs.value, "lt");
    break;
  case ::common::Logical::LE:
    cmp_result = is_float ? builder.CreateFCmpOLE(lhs.value, rhs.value, "fle")
                          : builder.CreateICmpSLE(lhs.value, rhs.value, "le");
    break;
  case ::common::Logical::GT:
    cmp_result = is_float ? builder.CreateFCmpOGT(lhs.value, rhs.value, "fgt")
                          : builder.CreateICmpSGT(lhs.value, rhs.value, "gt");
    break;
  case ::common::Logical::GE:
    cmp_result = is_float ? builder.CreateFCmpOGE(lhs.value, rhs.value, "fge")
                          : builder.CreateICmpSGE(lhs.value, rhs.value, "ge");
    break;
  case ::common::Logical::EQ:
    cmp_result = is_float ? builder.CreateFCmpOEQ(lhs.value, rhs.value, "feq")
                          : builder.CreateICmpEQ(lhs.value, rhs.value, "eq");
    break;
  case ::common::Logical::NE:
    cmp_result = is_float ? builder.CreateFCmpONE(lhs.value, rhs.value, "fne")
                          : builder.CreateICmpNE(lhs.value, rhs.value, "ne");
    break;
  default:
    return createNull(builder, DataTypeId::kBoolean);
  }

  // Select: if either null, result is null boolean
  llvm::Value* false_val = builder.getFalse();
  llvm::Value* final_value =
      builder.CreateSelect(either_null, false_val, cmp_result, "cmp_val");
  llvm::Value* final_null = builder.CreateSelect(
      either_null, builder.getInt8(1), builder.getInt8(0), "cmp_null");

  return {final_value, final_null};
}

CodegenValue ExprCodegen::generateNotIR(const CodegenValue& operand,
                                        llvm::IRBuilder<>& builder) {
  // NOT null = null
  llvm::Value* is_null =
      builder.CreateICmpNE(operand.is_null, builder.getInt8(0), "not_chk");
  llvm::Value* negated = builder.CreateNot(operand.value, "not_val");
  llvm::Value* final_value =
      builder.CreateSelect(is_null, builder.getFalse(), negated, "not_result");
  llvm::Value* final_null = builder.CreateSelect(
      is_null, builder.getInt8(1), builder.getInt8(0), "not_null");
  return {final_value, final_null};
}

CodegenValue ExprCodegen::generateIsNullIR(const CodegenValue& operand,
                                           llvm::IRBuilder<>& builder) {
  // ISNULL always returns non-null boolean
  llvm::Value* result =
      builder.CreateICmpNE(operand.is_null, builder.getInt8(0), "isnull");
  return {result, builder.getInt8(0)};
}

CodegenValue ExprCodegen::generateAndIR(const ExprBase* lhs_expr,
                                        const ExprBase* rhs_expr,
                                        llvm::IRBuilder<>& builder,
                                        llvm::Value* slots_ptr,
                                        llvm::Value* context_ptr,
                                        llvm::Value* idx_val,
                                        UnboundLeafSlots& unbound_leaves) {
  llvm::Function* function = builder.GetInsertBlock()->getParent();
  llvm::LLVMContext& ctx = builder.getContext();

  // Evaluate LHS
  CodegenValue lhs = generateExprIR(lhs_expr, builder, slots_ptr, context_ptr,
                                    idx_val, unbound_leaves);

  // Short-circuit: if LHS is not true (null or false), skip RHS
  llvm::Value* lhs_is_true =
      builder.CreateAnd(builder.CreateICmpEQ(lhs.is_null, builder.getInt8(0)),
                        lhs.value, "lhs_is_true");

  llvm::BasicBlock* eval_rhs_bb =
      llvm::BasicBlock::Create(ctx, "and_eval_rhs", function);
  llvm::BasicBlock* merge_bb =
      llvm::BasicBlock::Create(ctx, "and_merge", function);

  llvm::BasicBlock* current_bb = builder.GetInsertBlock();
  builder.CreateCondBr(lhs_is_true, eval_rhs_bb, merge_bb);

  // Evaluate RHS
  builder.SetInsertPoint(eval_rhs_bb);
  CodegenValue rhs = generateExprIR(rhs_expr, builder, slots_ptr, context_ptr,
                                    idx_val, unbound_leaves);
  llvm::BasicBlock* rhs_end_bb = builder.GetInsertBlock();
  builder.CreateBr(merge_bb);

  // Merge
  builder.SetInsertPoint(merge_bb);
  llvm::PHINode* phi_value =
      builder.CreatePHI(llvm::Type::getInt1Ty(ctx), 2, "and_val");
  phi_value->addIncoming(builder.getFalse(), current_bb);
  phi_value->addIncoming(rhs.value, rhs_end_bb);

  llvm::PHINode* phi_null =
      builder.CreatePHI(llvm::Type::getInt8Ty(ctx), 2, "and_null");
  phi_null->addIncoming(lhs.is_null, current_bb);
  phi_null->addIncoming(rhs.is_null, rhs_end_bb);

  return {phi_value, phi_null};
}

CodegenValue ExprCodegen::generateOrIR(const ExprBase* lhs_expr,
                                       const ExprBase* rhs_expr,
                                       llvm::IRBuilder<>& builder,
                                       llvm::Value* slots_ptr,
                                       llvm::Value* context_ptr,
                                       llvm::Value* idx_val,
                                       UnboundLeafSlots& unbound_leaves) {
  llvm::Function* function = builder.GetInsertBlock()->getParent();
  llvm::LLVMContext& ctx = builder.getContext();

  // Evaluate LHS
  CodegenValue lhs = generateExprIR(lhs_expr, builder, slots_ptr, context_ptr,
                                    idx_val, unbound_leaves);

  // Short-circuit: if LHS is true (non-null and true), skip RHS
  llvm::Value* lhs_is_true =
      builder.CreateAnd(builder.CreateICmpEQ(lhs.is_null, builder.getInt8(0)),
                        lhs.value, "lhs_true_or");

  llvm::BasicBlock* eval_rhs_bb =
      llvm::BasicBlock::Create(ctx, "or_eval_rhs", function);
  llvm::BasicBlock* merge_bb =
      llvm::BasicBlock::Create(ctx, "or_merge", function);

  llvm::BasicBlock* current_bb = builder.GetInsertBlock();
  builder.CreateCondBr(lhs_is_true, merge_bb, eval_rhs_bb);

  // Evaluate RHS
  builder.SetInsertPoint(eval_rhs_bb);
  CodegenValue rhs = generateExprIR(rhs_expr, builder, slots_ptr, context_ptr,
                                    idx_val, unbound_leaves);
  llvm::BasicBlock* rhs_end_bb = builder.GetInsertBlock();
  builder.CreateBr(merge_bb);

  // Merge
  builder.SetInsertPoint(merge_bb);
  llvm::PHINode* phi_value =
      builder.CreatePHI(llvm::Type::getInt1Ty(ctx), 2, "or_val");
  phi_value->addIncoming(builder.getTrue(), current_bb);
  phi_value->addIncoming(rhs.value, rhs_end_bb);

  llvm::PHINode* phi_null =
      builder.CreatePHI(llvm::Type::getInt8Ty(ctx), 2, "or_null");
  phi_null->addIncoming(builder.getInt8(0), current_bb);
  phi_null->addIncoming(rhs.is_null, rhs_end_bb);

  return {phi_value, phi_null};
}

CodegenValue ExprCodegen::generateExprIR(const ExprBase* expr,
                                         llvm::IRBuilder<>& builder,
                                         llvm::Value* slots_ptr,
                                         llvm::Value* context_ptr,
                                         llvm::Value* idx_val,
                                         UnboundLeafSlots& unbound_leaves) {
  // ArithExpr: generate IR for lhs and rhs, then apply arithmetic
  if (auto* arith = dynamic_cast<const ArithExpr*>(expr)) {
    CodegenValue lhs = generateExprIR(arith->lhs(), builder, slots_ptr,
                                      context_ptr, idx_val, unbound_leaves);
    CodegenValue rhs = generateExprIR(arith->rhs(), builder, slots_ptr,
                                      context_ptr, idx_val, unbound_leaves);
    return generateArithIR(lhs, rhs, expr->type().id(),
                           static_cast<int>(arith->arith()), builder);
  }

  // BinaryLogicalExpr: handle AND/OR with short-circuit, others as comparison
  if (auto* logic = dynamic_cast<const BinaryLogicalExpr*>(expr)) {
    auto op = logic->logical();
    if (op == ::common::Logical::AND) {
      return generateAndIR(logic->lhs(), logic->rhs(), builder, slots_ptr,
                           context_ptr, idx_val, unbound_leaves);
    }
    if (op == ::common::Logical::OR) {
      return generateOrIR(logic->lhs(), logic->rhs(), builder, slots_ptr,
                          context_ptr, idx_val, unbound_leaves);
    }
    // Comparison operators
    CodegenValue lhs = generateExprIR(logic->lhs(), builder, slots_ptr,
                                      context_ptr, idx_val, unbound_leaves);
    CodegenValue rhs = generateExprIR(logic->rhs(), builder, slots_ptr,
                                      context_ptr, idx_val, unbound_leaves);
    DataTypeId operand_type = logic->lhs()->type().id();
    return generateCompareIR(lhs, rhs, static_cast<int>(op), operand_type,
                             builder);
  }

  // UnaryLogicalExpr: NOT or ISNULL
  if (auto* unary = dynamic_cast<const UnaryLogicalExpr*>(expr)) {
    CodegenValue operand = generateExprIR(unary->operand(), builder, slots_ptr,
                                          context_ptr, idx_val, unbound_leaves);
    if (unary->logical() == ::common::Logical::NOT) {
      return generateNotIR(operand, builder);
    }
    if (unary->logical() == ::common::Logical::ISNULL) {
      return generateIsNullIR(operand, builder);
    }
    return createNull(builder, DataTypeId::kBoolean);
  }

  // CaseWhenExpr: generate if-then-else chain in IR
  if (auto* case_when = dynamic_cast<const CaseWhenExpr*>(expr)) {
    llvm::Function* function = builder.GetInsertBlock()->getParent();
    llvm::LLVMContext& llvm_ctx = builder.getContext();
    DataTypeId result_type_id = expr->type().id();
    llvm::Type* result_llvm_type = getLLVMType(llvm_ctx, result_type_id);

    // Create the final merge block
    llvm::BasicBlock* merge_bb =
        llvm::BasicBlock::Create(llvm_ctx, "case_merge", function);

    // PHI nodes will collect results from each branch
    llvm::IRBuilder<> merge_builder(merge_bb);
    llvm::PHINode* phi_value = merge_builder.CreatePHI(
        result_llvm_type, case_when->when_then_exprs().size() + 1, "case_val");
    llvm::PHINode* phi_null = merge_builder.CreatePHI(
        llvm::Type::getInt8Ty(llvm_ctx),
        case_when->when_then_exprs().size() + 1, "case_null");

    for (size_t i = 0; i < case_when->when_then_exprs().size(); ++i) {
      const auto& wt = case_when->when_then_exprs()[i];

      // Evaluate the WHEN condition
      CodegenValue when_val =
          generateExprIR(wt.first.get(), builder, slots_ptr, context_ptr,
                         idx_val, unbound_leaves);

      // Check if WHEN is true (non-null and true)
      llvm::Value* is_true = builder.CreateAnd(
          builder.CreateICmpEQ(when_val.is_null, builder.getInt8(0)),
          when_val.value, "when_true");

      llvm::BasicBlock* then_bb =
          llvm::BasicBlock::Create(llvm_ctx, "case_then", function);
      llvm::BasicBlock* next_bb =
          llvm::BasicBlock::Create(llvm_ctx, "case_next", function);

      builder.CreateCondBr(is_true, then_bb, next_bb);

      // THEN branch: evaluate the THEN expression
      builder.SetInsertPoint(then_bb);
      CodegenValue then_val =
          generateExprIR(wt.second.get(), builder, slots_ptr, context_ptr,
                         idx_val, unbound_leaves);
      llvm::BasicBlock* then_end_bb = builder.GetInsertBlock();
      builder.CreateBr(merge_bb);

      phi_value->addIncoming(then_val.value, then_end_bb);
      phi_null->addIncoming(then_val.is_null, then_end_bb);

      // Continue to next WHEN or ELSE
      builder.SetInsertPoint(next_bb);
    }

    // ELSE branch
    CodegenValue else_val =
        generateExprIR(case_when->else_expr(), builder, slots_ptr, context_ptr,
                       idx_val, unbound_leaves);
    llvm::BasicBlock* else_end_bb = builder.GetInsertBlock();
    builder.CreateBr(merge_bb);

    phi_value->addIncoming(else_val.value, else_end_bb);
    phi_null->addIncoming(else_val.is_null, else_end_bb);

    // Continue from merge block
    builder.SetInsertPoint(merge_bb);
    return {phi_value, phi_null};
  }

  // ConstExpr: emit the constant value directly as IR, avoiding trampoline.
  // ConstExpr::bind() returns a copy of itself (no dependency on
  // storage/params), so we can safely call bind(nullptr, {}) at codegen time to
  // extract the value.
  if (auto* const_expr = dynamic_cast<const ConstExpr*>(expr)) {
    auto binded = const_expr->bind(nullptr, {});
    auto* binded_const = dynamic_cast<const ConstExpr*>(binded.get());
    if (binded_const) {
      Value val = binded_const->eval_record({}, 0);
      DataTypeId type_id = expr->type().id();
      if (val.IsNull()) {
        return createNull(builder, type_id);
      }
      llvm::Value* llvm_val = nullptr;
      switch (type_id) {
      case DataTypeId::kBoolean:
        llvm_val = builder.getInt1(val.GetValue<bool>());
        break;
      case DataTypeId::kInt32:
        llvm_val = builder.getInt32(val.GetValue<int32_t>());
        break;
      case DataTypeId::kInt64:
        llvm_val = builder.getInt64(val.GetValue<int64_t>());
        break;
      case DataTypeId::kUInt32:
        llvm_val = builder.getInt32(val.GetValue<uint32_t>());
        break;
      case DataTypeId::kUInt64:
        llvm_val = builder.getInt64(val.GetValue<uint64_t>());
        break;
      case DataTypeId::kFloat:
        llvm_val =
            llvm::ConstantFP::get(builder.getFloatTy(), val.GetValue<float>());
        break;
      case DataTypeId::kDouble:
        llvm_val = llvm::ConstantFP::get(builder.getDoubleTy(),
                                         val.GetValue<double>());
        break;
      default:
        break;
      }
      if (llvm_val) {
        return createNonNull(builder, llvm_val);
      }
    }
  }

  // Default: treat as leaf node (ParamExpr, all Accessors,
  // ScalarFunctionExpr, ExtractExpr, WithInExpr, etc.), call trampoline.
  return generateLeafCallIR(expr, builder, slots_ptr, context_ptr, idx_val,
                            unbound_leaves);
}

void* ExprCodegen::compileRecordExpr(
    const ExprBase* expr, std::unique_ptr<UnboundLeafSlots>& unbound_leaves) {
  DataTypeId result_type = expr->type().id();
  if (!isTypeSupported(result_type)) {
    return nullptr;
  }

  eval_mode_ = EvalMode::kRecord;

  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("neug_expr_jit", *context);

  // Function signature:
  //   uint8_t fn(const void* slots, const void* ctx, uint64_t idx,
  //              T* out_value)
  //   Returns: 0 = not null, 1 = null
  llvm::Type* ptr_type = llvm::Type::getInt8PtrTy(*context);
  llvm::Type* i64_type = llvm::Type::getInt64Ty(*context);
  llvm::Type* i8_type = llvm::Type::getInt8Ty(*context);
  llvm::Type* value_type = getLLVMType(*context, result_type);
  if (!value_type) {
    return nullptr;
  }

  llvm::FunctionType* fn_type = llvm::FunctionType::get(
      i8_type, {ptr_type, ptr_type, i64_type, ptr_type}, false);

  std::string func_name = "neug_jit_expr_" + std::to_string(func_counter_++);
  llvm::Function* function = llvm::Function::Create(
      fn_type, llvm::Function::ExternalLinkage, func_name, module.get());

  // Name the arguments
  auto args = function->arg_begin();
  llvm::Value* slots_ptr = &*args++;
  slots_ptr->setName("slots");
  llvm::Value* context_ptr = &*args++;
  context_ptr->setName("ctx");
  llvm::Value* idx_val = &*args++;
  idx_val->setName("idx");
  llvm::Value* out_value_arg = &*args++;
  out_value_arg->setName("out_value");

  // Create entry basic block
  llvm::BasicBlock* entry =
      llvm::BasicBlock::Create(*context, "entry", function);
  llvm::IRBuilder<> builder(entry);

  // Generate IR for the expression tree
  unbound_leaves = std::make_unique<UnboundLeafSlots>();
  CodegenValue result = generateExprIR(expr, builder, slots_ptr, context_ptr,
                                       idx_val, *unbound_leaves);

  // Write value through out pointer, return is_null as uint8_t
  llvm::Value* out_val_typed = builder.CreateBitCast(
      out_value_arg, llvm::PointerType::get(value_type, 0), "out_val_typed");
  builder.CreateStore(result.value, out_val_typed);
  builder.CreateRet(result.is_null);

  // Verify the function
  if (llvm::verifyFunction(*function, &llvm::errs())) {
    LOG(ERROR) << "JIT function verification failed for " << func_name;
    return nullptr;
  }

  // Register trampoline symbols with the JIT engine so it can resolve them
  auto& engine = JitEngine::instance();

  // We need to add symbol mappings for the trampoline functions.
  // The LLJIT will resolve external symbols via the process symbol table,
  // so as long as the trampolines are extern "C" and linked into the binary,
  // they should be resolvable automatically.

  return engine.compile(std::move(context), std::move(module), func_name);
}

// ============================================================================
// Vertex expression compilation
// ============================================================================

void* ExprCodegen::compileVertexExpr(
    const ExprBase* expr, std::unique_ptr<UnboundLeafSlots>& unbound_leaves) {
  DataTypeId result_type = expr->type().id();
  if (!isTypeSupported(result_type)) {
    return nullptr;
  }

  eval_mode_ = EvalMode::kVertex;

  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("neug_vertex_jit", *context);

  // Function signature:
  //   uint8_t fn(slots_ptr, label_u8, vid_u32, T* out_value)
  //   Returns: 0 = not null, 1 = null
  llvm::Type* ptr_type = llvm::Type::getInt8PtrTy(*context);
  llvm::Type* i8_type = llvm::Type::getInt8Ty(*context);
  llvm::Type* i32_type = llvm::Type::getInt32Ty(*context);
  llvm::Type* value_type = getLLVMType(*context, result_type);
  if (!value_type)
    return nullptr;

  llvm::FunctionType* fn_type = llvm::FunctionType::get(
      i8_type, {ptr_type, i8_type, i32_type, ptr_type}, false);

  std::string func_name = "neug_jit_vexpr_" + std::to_string(func_counter_++);
  llvm::Function* function = llvm::Function::Create(
      fn_type, llvm::Function::ExternalLinkage, func_name, module.get());

  auto args = function->arg_begin();
  llvm::Value* slots_ptr = &*args++;
  slots_ptr->setName("slots");
  llvm::Value* label_val = &*args++;
  label_val->setName("label");
  llvm::Value* vid_val = &*args++;
  vid_val->setName("vid");
  llvm::Value* out_value_arg = &*args++;
  out_value_arg->setName("out_value");

  llvm::BasicBlock* entry =
      llvm::BasicBlock::Create(*context, "entry", function);
  llvm::IRBuilder<> builder(entry);

  unbound_leaves = std::make_unique<UnboundLeafSlots>();

  // Reuse generateExprIR by passing label and vid through context_ptr/idx_val.
  // The vertex trampoline has the same 6-arg signature as record trampoline,
  // but interprets context_ptr as label (inttoptr) and idx_val as vid.
  llvm::Value* label_ext =
      builder.CreateZExt(label_val, builder.getInt64Ty(), "label_ext");
  llvm::Value* label_as_ptr =
      builder.CreateIntToPtr(label_ext, ptr_type, "label_ptr");
  llvm::Value* vid_ext =
      builder.CreateZExt(vid_val, llvm::Type::getInt64Ty(*context), "vid_ext");

  CodegenValue result = generateExprIR(expr, builder, slots_ptr, label_as_ptr,
                                       vid_ext, *unbound_leaves);

  // Write value through out pointer, return is_null as uint8_t
  llvm::Value* out_val_typed = builder.CreateBitCast(
      out_value_arg, llvm::PointerType::get(value_type, 0), "out_val_typed");
  builder.CreateStore(result.value, out_val_typed);
  builder.CreateRet(result.is_null);

  if (llvm::verifyFunction(*function, &llvm::errs())) {
    LOG(ERROR) << "JIT vertex function verification failed for " << func_name;
    return nullptr;
  }

  auto& engine = JitEngine::instance();
  return engine.compile(std::move(context), std::move(module), func_name);
}

// ============================================================================
// Edge expression compilation
// ============================================================================

void* ExprCodegen::compileEdgeExpr(
    const ExprBase* expr, std::unique_ptr<UnboundLeafSlots>& unbound_leaves) {
  DataTypeId result_type = expr->type().id();
  if (!isTypeSupported(result_type)) {
    return nullptr;
  }

  eval_mode_ = EvalMode::kEdge;

  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("neug_edge_jit", *context);

  // Function signature:
  //   uint8_t fn(slots_ptr, label_triplet_ptr, src_u32, dst_u32, edata_ptr,
  //              T* out_value)
  //   Returns: 0 = not null, 1 = null
  llvm::Type* ptr_type = llvm::Type::getInt8PtrTy(*context);
  llvm::Type* i32_type = llvm::Type::getInt32Ty(*context);
  llvm::Type* i8_type = llvm::Type::getInt8Ty(*context);
  llvm::Type* value_type = getLLVMType(*context, result_type);
  if (!value_type)
    return nullptr;

  llvm::FunctionType* fn_type = llvm::FunctionType::get(
      i8_type, {ptr_type, ptr_type, i32_type, i32_type, ptr_type, ptr_type},
      false);

  std::string func_name = "neug_jit_eexpr_" + std::to_string(func_counter_++);
  llvm::Function* function = llvm::Function::Create(
      fn_type, llvm::Function::ExternalLinkage, func_name, module.get());

  auto args = function->arg_begin();
  llvm::Value* slots_ptr = &*args++;
  slots_ptr->setName("slots");
  llvm::Value* triplet_ptr = &*args++;
  triplet_ptr->setName("triplet");
  llvm::Value* src_val = &*args++;
  src_val->setName("src");
  llvm::Value* dst_val = &*args++;
  dst_val->setName("dst");
  llvm::Value* edata_ptr = &*args++;
  edata_ptr->setName("edata");
  // Write value through out pointer, return is_null as uint8_t
  llvm::Value* out_val_typed = builder.CreateBitCast(
      out_value_arg, llvm::PointerType::get(value_type, 0), "out_val_typed");
  builder.CreateStore(result.value, out_val_typed);
  builder.CreateRet(result.is_null);

  if (llvm::verifyFunction(*function, &llvm::errs())) {
    LOG(ERROR) << "JIT edge function verification failed for " << func_name;
    return nullptr;
  }

  auto& engine = JitEngine::instance();
  return engine.compile(std::move(context), std::move(module), func_name);
}

}  // namespace codegen
}  // namespace execution
}  // namespace neug

#endif  // NEUG_ENABLE_JIT_EXPRESSION
