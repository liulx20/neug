
#ifdef NEUG_ENABLE_JIT_EXPRESSION

#include "neug/execution/expression/codegen/jit_compiled_expr.h"
#include "neug/execution/expression/codegen/expr_codegen.h"
#include "neug/execution/expression/codegen/codegen_types.h"
#include <glog/logging.h>

namespace neug {
namespace execution {
namespace codegen {
// All JIT functions return uint8_t (0 = not null, 1 = null) and write the
// value through an out pointer. This avoids ABI issues with large return types.

// Record: uint8_t fn(slots_ptr, ctx_ptr, idx, out_value)
using JitRecordFn = uint8_t (*)(const void*, const void*, uint64_t, void*);

// Vertex: uint8_t fn(slots_ptr, label_u8, vid_u32, out_value)
using JitVertexFn = uint8_t (*)(const void*, uint8_t, uint32_t, void*);

// Edge: uint8_t fn(slots_ptr, triplet_ptr, src_u32, dst_u32, edata_ptr, out_value)
using JitEdgeFn = uint8_t (*)(const void*, const void*, uint32_t, uint32_t,
                               const void*, void*);
// ============================================================================
// JitCompiledExpr eval implementations
// ============================================================================

// Helper: convert typed value to Value based on DataTypeId.
// Used by eval_record/eval_vertex/eval_edge to construct Value from out buffer.
static Value makeValueFromBuffer(DataTypeId type_id, const void* buf,
                                 int8_t is_null) {
  if (is_null) {
    switch (type_id) {
    case DataTypeId::kBoolean:    return Value(DataType::BOOLEAN);
    case DataTypeId::kInt32:      return Value(DataType::INT32);
    case DataTypeId::kInt64:      return Value(DataType::INT64);
    case DataTypeId::kUInt32:     return Value(DataType::UINT32);
    case DataTypeId::kUInt64:     return Value(DataType::UINT64);
    case DataTypeId::kFloat:      return Value(DataType::FLOAT);
    case DataTypeId::kDouble:     return Value(DataType::DOUBLE);
    case DataTypeId::kTimestampMs: return Value(DataType::TIMESTAMP_MS);
    case DataTypeId::kDate:       return Value(DataType::DATE);
    default:                      return Value(DataType::SQLNULL);
    }
  }
  switch (type_id) {
  case DataTypeId::kBoolean:    return Value::BOOLEAN(*static_cast<const bool*>(buf));
  case DataTypeId::kInt32:      return Value::INT32(*static_cast<const int32_t*>(buf));
  case DataTypeId::kInt64:      return Value::INT64(*static_cast<const int64_t*>(buf));
  case DataTypeId::kUInt32:     return Value::UINT32(*static_cast<const uint32_t*>(buf));
  case DataTypeId::kUInt64:     return Value::UINT64(*static_cast<const uint64_t*>(buf));
  case DataTypeId::kFloat:      return Value::FLOAT(*static_cast<const float*>(buf));
  case DataTypeId::kDouble:     return Value::DOUBLE(*static_cast<const double*>(buf));
  case DataTypeId::kTimestampMs: return Value::TIMESTAMPMS(*static_cast<const DateTime*>(buf));
  case DataTypeId::kDate:       return Value::DATE(*static_cast<const Date*>(buf));
  default:
    LOG(FATAL) << "Unsupported JIT result type";
    return Value(DataType::SQLNULL);
  }
}

Value JitCompiledExpr::eval_record(const Context& ctx, size_t idx) const {
  if (!record_fn_) {
    LOG(FATAL) << "JIT record function not compiled";
    return Value(DataType::SQLNULL);
  }
  alignas(16) char buf[16];
  uint8_t is_null = reinterpret_cast<JitRecordFn>(record_fn_)(
      record_slots_.get(), &ctx, static_cast<uint64_t>(idx), buf);
  return makeValueFromBuffer(result_type_, buf, is_null);
}

Value JitCompiledExpr::eval_vertex(label_t v_label, vid_t v_id) const {
  if (!vertex_fn_) {
    LOG(FATAL) << "JIT vertex function not compiled";
    return Value(DataType::SQLNULL);
  }
  alignas(16) char buf[16];
  uint8_t is_null = reinterpret_cast<JitVertexFn>(vertex_fn_)(
      vertex_slots_.get(), static_cast<uint8_t>(v_label),
      static_cast<uint32_t>(v_id), buf);
  return makeValueFromBuffer(result_type_, buf, is_null);
}

Value JitCompiledExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                  vid_t dst, const void* edata) const {
  if (!edge_fn_) {
    LOG(FATAL) << "JIT edge function not compiled";
    return Value(DataType::SQLNULL);
  }
  alignas(16) char buf[16];
  uint8_t is_null = reinterpret_cast<JitEdgeFn>(edge_fn_)(
      edge_slots_.get(), &label, static_cast<uint32_t>(src),
      static_cast<uint32_t>(dst), edata, buf);
  return makeValueFromBuffer(result_type_, buf, is_null);
}

// ============================================================================
// JitCompiledExpr typed_eval implementations
// Directly write primitive to out_value, bypassing Value entirely.
// ============================================================================

bool JitCompiledExpr::typed_eval_record(const Context& ctx, size_t idx,
                                        void* out_value) const {
  if (!record_fn_) {
    LOG(FATAL) << "JIT record function not compiled";
    return true;
  }
  return reinterpret_cast<JitRecordFn>(record_fn_)(
      record_slots_.get(), &ctx, static_cast<uint64_t>(idx),
      out_value) != 0;
}

bool JitCompiledExpr::typed_eval_vertex(label_t v_label, vid_t v_id,
                                        void* out_value) const {
  if (!vertex_fn_) {
    LOG(FATAL) << "JIT vertex function not compiled";
    return true;
  }
  return reinterpret_cast<JitVertexFn>(vertex_fn_)(
      vertex_slots_.get(), static_cast<uint8_t>(v_label),
      static_cast<uint32_t>(v_id), out_value) != 0;
}

bool JitCompiledExpr::typed_eval_edge(const LabelTriplet& label, vid_t src,
                                      vid_t dst, const void* edata,
                                      void* out_value) const {
  if (!edge_fn_) {
    LOG(FATAL) << "JIT edge function not compiled";
    return true;
  }
  return reinterpret_cast<JitEdgeFn>(edge_fn_)(
      edge_slots_.get(), &label, static_cast<uint32_t>(src),
      static_cast<uint32_t>(dst), edata, out_value) != 0;
}
// ============================================================================
// JitCompiledTemplate implementation
// ============================================================================

std::unique_ptr<LeafExprSlots> JitCompiledTemplate::bindLeaves(
    const IStorageInterface* storage,
    const ParamsMap& params) const {
  auto slots = std::make_unique<LeafExprSlots>();
  if (!unbound_leaves_) return slots;

  slots->leaves.reserve(unbound_leaves_->leaves.size());
  for (const auto* leaf_expr : unbound_leaves_->leaves) {
    slots->leaves.push_back(leaf_expr->bind(storage, params));
  }
  return slots;
}

// ============================================================================
// Two-phase API implementation
// ============================================================================

static bool canAndShouldJit(const ExprBase* expr) {
  DataTypeId result_type = expr->type().id();
  return ExprCodegen::isTypeSupported(result_type) &&
         ExprCodegen::canCodegen(expr);
}

std::shared_ptr<JitCompiledTemplate> compileExprTemplate(
    const ExprBase* expr,
    EvalMode eval_mode) {
  if (!canAndShouldJit(expr)) {
    return nullptr;
  }

  ExprCodegen codegen;
  std::unique_ptr<UnboundLeafSlots> unbound_leaves;
  void* fn_ptr = nullptr;

  switch (eval_mode) {
  case EvalMode::kRecord:
    fn_ptr = codegen.compileRecordExpr(expr, unbound_leaves);
    break;
  case EvalMode::kVertex:
    fn_ptr = codegen.compileVertexExpr(expr, unbound_leaves);
    break;
  case EvalMode::kEdge:
    fn_ptr = codegen.compileEdgeExpr(expr, unbound_leaves);
    break;
  }

  if (!fn_ptr) {
    LOG(WARNING) << "JIT compilation failed for eval_mode="
                 << static_cast<int>(eval_mode);
    return nullptr;
  }

  return std::make_shared<JitCompiledTemplate>(
      fn_ptr, expr->type().id(), eval_mode, std::move(unbound_leaves));
}

std::unique_ptr<BindedExprBase> bindTemplate(
    const std::shared_ptr<JitCompiledTemplate>& tmpl,
    const IStorageInterface* storage,
    const ParamsMap& params) {
  if (!tmpl || !tmpl->valid()) {
    return nullptr;
  }

  auto slots = tmpl->bindLeaves(storage, params);
  void* fn = tmpl->function_ptr();
  DataTypeId result_type = tmpl->result_type();

  switch (tmpl->eval_mode()) {
  case EvalMode::kRecord:
    return std::make_unique<JitCompiledExpr>(
        fn, nullptr, nullptr, result_type,
        std::move(slots), nullptr, nullptr);
  case EvalMode::kVertex:
    return std::make_unique<JitCompiledExpr>(
        nullptr, fn, nullptr, result_type,
        nullptr, std::move(slots), nullptr);
  case EvalMode::kEdge:
    return std::make_unique<JitCompiledExpr>(
        nullptr, nullptr, fn, result_type,
        nullptr, nullptr, std::move(slots));
  }
  return nullptr;
}

// ============================================================================
// One-shot API: compile + bind in one call (convenience wrappers)
// ============================================================================

std::unique_ptr<BindedExprBase> tryJitCompileRecord(
    const ExprBase* expr,
    const IStorageInterface* storage,
    const ParamsMap& params) {
  auto tmpl = compileExprTemplate(expr, EvalMode::kRecord);
  if (!tmpl) {
    return expr->bind(storage, params);
  }
  auto result = bindTemplate(tmpl, storage, params);
  if (!result) {
    LOG(WARNING) << "JIT record bind failed, falling back to interpreter";
    return expr->bind(storage, params);
  }
  return result;
}

std::unique_ptr<BindedExprBase> tryJitCompileVertex(
    const ExprBase* expr,
    const IStorageInterface* storage,
    const ParamsMap& params) {
  auto tmpl = compileExprTemplate(expr, EvalMode::kVertex);
  if (!tmpl) {
    return expr->bind(storage, params);
  }
  auto result = bindTemplate(tmpl, storage, params);
  if (!result) {
    LOG(WARNING) << "JIT vertex bind failed, falling back to interpreter";
    return expr->bind(storage, params);
  }
  return result;
}

std::unique_ptr<BindedExprBase> tryJitCompileEdge(
    const ExprBase* expr,
    const IStorageInterface* storage,
    const ParamsMap& params) {
  auto tmpl = compileExprTemplate(expr, EvalMode::kEdge);
  if (!tmpl) {
    return expr->bind(storage, params);
  }
  auto result = bindTemplate(tmpl, storage, params);
  if (!result) {
    LOG(WARNING) << "JIT edge bind failed, falling back to interpreter";
    return expr->bind(storage, params);
  }
  return result;
}

}  // namespace codegen
}  // namespace execution
}  // namespace neug

#endif  // NEUG_ENABLE_JIT_EXPRESSION
