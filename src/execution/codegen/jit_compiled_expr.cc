
#ifdef NEUG_ENABLE_JIT_EXPRESSION

#include "neug/execution/expression/codegen/jit_compiled_expr.h"
#include "neug/execution/expression/codegen/expr_codegen.h"
#include "neug/execution/expression/codegen/codegen_types.h"
#include <glog/logging.h>

namespace neug {
namespace execution {
namespace codegen {

// ============================================================================
// JIT function pointer typedefs
// ============================================================================

// Record: fn(slots_ptr, ctx_ptr, idx) -> NullableValue<T>
using JitRecordFn_Bool = NullableBool (*)(const void*, const void*, uint64_t);
using JitRecordFn_Int32 = NullableInt32 (*)(const void*, const void*, uint64_t);
using JitRecordFn_Int64 = NullableInt64 (*)(const void*, const void*, uint64_t);
using JitRecordFn_UInt32 = NullableUInt32 (*)(const void*, const void*, uint64_t);
using JitRecordFn_UInt64 = NullableUInt64 (*)(const void*, const void*, uint64_t);
using JitRecordFn_Float = NullableFloat (*)(const void*, const void*, uint64_t);
using JitRecordFn_Double = NullableDouble (*)(const void*, const void*, uint64_t);

// Vertex: fn(slots_ptr, label_u8, vid_u32) -> NullableValue<T>
using JitVertexFn_Bool = NullableBool (*)(const void*, uint8_t, uint32_t);
using JitVertexFn_Int32 = NullableInt32 (*)(const void*, uint8_t, uint32_t);
using JitVertexFn_Int64 = NullableInt64 (*)(const void*, uint8_t, uint32_t);
using JitVertexFn_UInt32 = NullableUInt32 (*)(const void*, uint8_t, uint32_t);
using JitVertexFn_UInt64 = NullableUInt64 (*)(const void*, uint8_t, uint32_t);
using JitVertexFn_Float = NullableFloat (*)(const void*, uint8_t, uint32_t);
using JitVertexFn_Double = NullableDouble (*)(const void*, uint8_t, uint32_t);

// Edge: fn(slots_ptr, triplet_ptr, src_u32, dst_u32, edata_ptr) -> NullableValue<T>
using JitEdgeFn_Bool = NullableBool (*)(const void*, const void*, uint32_t, uint32_t, const void*);
using JitEdgeFn_Int32 = NullableInt32 (*)(const void*, const void*, uint32_t, uint32_t, const void*);
using JitEdgeFn_Int64 = NullableInt64 (*)(const void*, const void*, uint32_t, uint32_t, const void*);
using JitEdgeFn_UInt32 = NullableUInt32 (*)(const void*, const void*, uint32_t, uint32_t, const void*);
using JitEdgeFn_UInt64 = NullableUInt64 (*)(const void*, const void*, uint32_t, uint32_t, const void*);
using JitEdgeFn_Float = NullableFloat (*)(const void*, const void*, uint32_t, uint32_t, const void*);
using JitEdgeFn_Double = NullableDouble (*)(const void*, const void*, uint32_t, uint32_t, const void*);

// ============================================================================
// JitCompiledExpr eval implementations
// ============================================================================

#define JIT_DISPATCH_RECORD(RESULT_TYPE, FN, SLOTS, CTX, IDX)                 \
  do {                                                                        \
    const void* _s = (SLOTS);                                                 \
    const void* _c = (CTX);                                                   \
    uint64_t _i = (IDX);                                                      \
    switch (RESULT_TYPE) {                                                    \
    case DataTypeId::kBoolean: {                                              \
      auto r = reinterpret_cast<JitRecordFn_Bool>(FN)(_s, _c, _i);          \
      return r.is_null ? Value(DataType::BOOLEAN) : Value::BOOLEAN(r.value); \
    }                                                                         \
    case DataTypeId::kInt32: {                                                \
      auto r = reinterpret_cast<JitRecordFn_Int32>(FN)(_s, _c, _i);         \
      return r.is_null ? Value(DataType::INT32) : Value::INT32(r.value);     \
    }                                                                         \
    case DataTypeId::kInt64: {                                                \
      auto r = reinterpret_cast<JitRecordFn_Int64>(FN)(_s, _c, _i);         \
      return r.is_null ? Value(DataType::INT64) : Value::INT64(r.value);     \
    }                                                                         \
    case DataTypeId::kUInt32: {                                               \
      auto r = reinterpret_cast<JitRecordFn_UInt32>(FN)(_s, _c, _i);        \
      return r.is_null ? Value(DataType::UINT32) : Value::UINT32(r.value);   \
    }                                                                         \
    case DataTypeId::kUInt64: {                                               \
      auto r = reinterpret_cast<JitRecordFn_UInt64>(FN)(_s, _c, _i);        \
      return r.is_null ? Value(DataType::UINT64) : Value::UINT64(r.value);   \
    }                                                                         \
    case DataTypeId::kFloat: {                                                \
      auto r = reinterpret_cast<JitRecordFn_Float>(FN)(_s, _c, _i);         \
      return r.is_null ? Value(DataType::FLOAT) : Value::FLOAT(r.value);     \
    }                                                                         \
    case DataTypeId::kDouble: {                                               \
      auto r = reinterpret_cast<JitRecordFn_Double>(FN)(_s, _c, _i);        \
      return r.is_null ? Value(DataType::DOUBLE) : Value::DOUBLE(r.value);   \
    }                                                                         \
    default:                                                                  \
      LOG(FATAL) << "Unsupported JIT result type";                           \
      return Value(DataType::SQLNULL);                                        \
    }                                                                         \
  } while (0)

#define JIT_DISPATCH_VERTEX(RESULT_TYPE, FN, SLOTS, LABEL, VID)               \
  do {                                                                        \
    const void* _s = (SLOTS);                                                 \
    uint8_t _l = (LABEL);                                                     \
    uint32_t _v = (VID);                                                      \
    switch (RESULT_TYPE) {                                                    \
    case DataTypeId::kBoolean: {                                              \
      auto r = reinterpret_cast<JitVertexFn_Bool>(FN)(_s, _l, _v);          \
      return r.is_null ? Value(DataType::BOOLEAN) : Value::BOOLEAN(r.value); \
    }                                                                         \
    case DataTypeId::kInt32: {                                                \
      auto r = reinterpret_cast<JitVertexFn_Int32>(FN)(_s, _l, _v);         \
      return r.is_null ? Value(DataType::INT32) : Value::INT32(r.value);     \
    }                                                                         \
    case DataTypeId::kInt64: {                                                \
      auto r = reinterpret_cast<JitVertexFn_Int64>(FN)(_s, _l, _v);         \
      return r.is_null ? Value(DataType::INT64) : Value::INT64(r.value);     \
    }                                                                         \
    case DataTypeId::kUInt32: {                                               \
      auto r = reinterpret_cast<JitVertexFn_UInt32>(FN)(_s, _l, _v);        \
      return r.is_null ? Value(DataType::UINT32) : Value::UINT32(r.value);   \
    }                                                                         \
    case DataTypeId::kUInt64: {                                               \
      auto r = reinterpret_cast<JitVertexFn_UInt64>(FN)(_s, _l, _v);        \
      return r.is_null ? Value(DataType::UINT64) : Value::UINT64(r.value);   \
    }                                                                         \
    case DataTypeId::kFloat: {                                                \
      auto r = reinterpret_cast<JitVertexFn_Float>(FN)(_s, _l, _v);         \
      return r.is_null ? Value(DataType::FLOAT) : Value::FLOAT(r.value);     \
    }                                                                         \
    case DataTypeId::kDouble: {                                               \
      auto r = reinterpret_cast<JitVertexFn_Double>(FN)(_s, _l, _v);        \
      return r.is_null ? Value(DataType::DOUBLE) : Value::DOUBLE(r.value);   \
    }                                                                         \
    default:                                                                  \
      LOG(FATAL) << "Unsupported JIT result type";                           \
      return Value(DataType::SQLNULL);                                        \
    }                                                                         \
  } while (0)

#define JIT_DISPATCH_EDGE(RESULT_TYPE, FN, SLOTS, TRIP, SRC, DST, EDATA)      \
  do {                                                                        \
    const void* _s = (SLOTS);                                                 \
    const void* _t = (TRIP);                                                  \
    uint32_t _src = (SRC);                                                    \
    uint32_t _dst = (DST);                                                    \
    const void* _e = (EDATA);                                                 \
    switch (RESULT_TYPE) {                                                    \
    case DataTypeId::kBoolean: {                                              \
      auto r = reinterpret_cast<JitEdgeFn_Bool>(FN)(_s, _t, _src, _dst, _e);\
      return r.is_null ? Value(DataType::BOOLEAN) : Value::BOOLEAN(r.value); \
    }                                                                         \
    case DataTypeId::kInt32: {                                                \
      auto r = reinterpret_cast<JitEdgeFn_Int32>(FN)(_s, _t, _src, _dst, _e);\
      return r.is_null ? Value(DataType::INT32) : Value::INT32(r.value);     \
    }                                                                         \
    case DataTypeId::kInt64: {                                                \
      auto r = reinterpret_cast<JitEdgeFn_Int64>(FN)(_s, _t, _src, _dst, _e);\
      return r.is_null ? Value(DataType::INT64) : Value::INT64(r.value);     \
    }                                                                         \
    case DataTypeId::kUInt32: {                                               \
      auto r = reinterpret_cast<JitEdgeFn_UInt32>(FN)(_s, _t, _src, _dst, _e);\
      return r.is_null ? Value(DataType::UINT32) : Value::UINT32(r.value);   \
    }                                                                         \
    case DataTypeId::kUInt64: {                                               \
      auto r = reinterpret_cast<JitEdgeFn_UInt64>(FN)(_s, _t, _src, _dst, _e);\
      return r.is_null ? Value(DataType::UINT64) : Value::UINT64(r.value);   \
    }                                                                         \
    case DataTypeId::kFloat: {                                                \
      auto r = reinterpret_cast<JitEdgeFn_Float>(FN)(_s, _t, _src, _dst, _e);\
      return r.is_null ? Value(DataType::FLOAT) : Value::FLOAT(r.value);     \
    }                                                                         \
    case DataTypeId::kDouble: {                                               \
      auto r = reinterpret_cast<JitEdgeFn_Double>(FN)(_s, _t, _src, _dst, _e);\
      return r.is_null ? Value(DataType::DOUBLE) : Value::DOUBLE(r.value);   \
    }                                                                         \
    default:                                                                  \
      LOG(FATAL) << "Unsupported JIT result type";                           \
      return Value(DataType::SQLNULL);                                        \
    }                                                                         \
  } while (0)

Value JitCompiledExpr::eval_record(const Context& ctx, size_t idx) const {
  if (!record_fn_) {
    LOG(FATAL) << "JIT record function not compiled";
    return Value(DataType::SQLNULL);
  }
  JIT_DISPATCH_RECORD(result_type_, record_fn_, record_slots_.get(),
                      &ctx, static_cast<uint64_t>(idx));
}

Value JitCompiledExpr::eval_vertex(label_t v_label, vid_t v_id) const {
  if (!vertex_fn_) {
    LOG(FATAL) << "JIT vertex function not compiled";
    return Value(DataType::SQLNULL);
  }
  JIT_DISPATCH_VERTEX(result_type_, vertex_fn_, vertex_slots_.get(),
                      static_cast<uint8_t>(v_label),
                      static_cast<uint32_t>(v_id));
}

Value JitCompiledExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                  vid_t dst, const void* edata) const {
  if (!edge_fn_) {
    LOG(FATAL) << "JIT edge function not compiled";
    return Value(DataType::SQLNULL);
  }
  JIT_DISPATCH_EDGE(result_type_, edge_fn_, edge_slots_.get(),
                    &label, static_cast<uint32_t>(src),
                    static_cast<uint32_t>(dst), edata);
}

// ============================================================================
// JitCompiledExpr typed_eval implementations
// Directly extract primitive from NullableValue<T>, bypassing Value entirely.
// ============================================================================

#define JIT_TYPED_DISPATCH_RECORD(RESULT_TYPE, FN, SLOTS, CTX, IDX, OUT)      \
  do {                                                                        \
    const void* _s = (SLOTS);                                                 \
    const void* _c = (CTX);                                                   \
    uint64_t _i = (IDX);                                                      \
    switch (RESULT_TYPE) {                                                    \
    case DataTypeId::kBoolean: {                                              \
      auto r = reinterpret_cast<JitRecordFn_Bool>(FN)(_s, _c, _i);          \
      *static_cast<bool*>(OUT) = r.value;                                    \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kInt32: {                                                \
      auto r = reinterpret_cast<JitRecordFn_Int32>(FN)(_s, _c, _i);         \
      *static_cast<int32_t*>(OUT) = r.value;                                 \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kInt64: {                                                \
      auto r = reinterpret_cast<JitRecordFn_Int64>(FN)(_s, _c, _i);         \
      *static_cast<int64_t*>(OUT) = r.value;                                 \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kUInt32: {                                               \
      auto r = reinterpret_cast<JitRecordFn_UInt32>(FN)(_s, _c, _i);        \
      *static_cast<uint32_t*>(OUT) = r.value;                                \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kUInt64: {                                               \
      auto r = reinterpret_cast<JitRecordFn_UInt64>(FN)(_s, _c, _i);        \
      *static_cast<uint64_t*>(OUT) = r.value;                                \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kFloat: {                                                \
      auto r = reinterpret_cast<JitRecordFn_Float>(FN)(_s, _c, _i);         \
      *static_cast<float*>(OUT) = r.value;                                   \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kDouble: {                                               \
      auto r = reinterpret_cast<JitRecordFn_Double>(FN)(_s, _c, _i);        \
      *static_cast<double*>(OUT) = r.value;                                  \
      return r.is_null != 0;                                                  \
    }                                                                         \
    default:                                                                  \
      LOG(FATAL) << "Unsupported JIT result type";                           \
      return true;                                                            \
    }                                                                         \
  } while (0)

#define JIT_TYPED_DISPATCH_VERTEX(RESULT_TYPE, FN, SLOTS, LABEL, VID, OUT)    \
  do {                                                                        \
    const void* _s = (SLOTS);                                                 \
    uint8_t _l = (LABEL);                                                     \
    uint32_t _v = (VID);                                                      \
    switch (RESULT_TYPE) {                                                    \
    case DataTypeId::kBoolean: {                                              \
      auto r = reinterpret_cast<JitVertexFn_Bool>(FN)(_s, _l, _v);          \
      *static_cast<bool*>(OUT) = r.value;                                    \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kInt32: {                                                \
      auto r = reinterpret_cast<JitVertexFn_Int32>(FN)(_s, _l, _v);         \
      *static_cast<int32_t*>(OUT) = r.value;                                 \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kInt64: {                                                \
      auto r = reinterpret_cast<JitVertexFn_Int64>(FN)(_s, _l, _v);         \
      *static_cast<int64_t*>(OUT) = r.value;                                 \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kUInt32: {                                               \
      auto r = reinterpret_cast<JitVertexFn_UInt32>(FN)(_s, _l, _v);        \
      *static_cast<uint32_t*>(OUT) = r.value;                                \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kUInt64: {                                               \
      auto r = reinterpret_cast<JitVertexFn_UInt64>(FN)(_s, _l, _v);        \
      *static_cast<uint64_t*>(OUT) = r.value;                                \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kFloat: {                                                \
      auto r = reinterpret_cast<JitVertexFn_Float>(FN)(_s, _l, _v);         \
      *static_cast<float*>(OUT) = r.value;                                   \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kDouble: {                                               \
      auto r = reinterpret_cast<JitVertexFn_Double>(FN)(_s, _l, _v);        \
      *static_cast<double*>(OUT) = r.value;                                  \
      return r.is_null != 0;                                                  \
    }                                                                         \
    default:                                                                  \
      LOG(FATAL) << "Unsupported JIT result type";                           \
      return true;                                                            \
    }                                                                         \
  } while (0)

#define JIT_TYPED_DISPATCH_EDGE(RESULT_TYPE, FN, SLOTS, TRIP, SRC, DST,       \
                                EDATA, OUT)                                   \
  do {                                                                        \
    const void* _s = (SLOTS);                                                 \
    const void* _t = (TRIP);                                                  \
    uint32_t _src = (SRC);                                                    \
    uint32_t _dst = (DST);                                                    \
    const void* _e = (EDATA);                                                 \
    switch (RESULT_TYPE) {                                                    \
    case DataTypeId::kBoolean: {                                              \
      auto r = reinterpret_cast<JitEdgeFn_Bool>(FN)(_s, _t, _src, _dst, _e);\
      *static_cast<bool*>(OUT) = r.value;                                    \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kInt32: {                                                \
      auto r = reinterpret_cast<JitEdgeFn_Int32>(FN)(_s, _t, _src, _dst, _e);\
      *static_cast<int32_t*>(OUT) = r.value;                                 \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kInt64: {                                                \
      auto r = reinterpret_cast<JitEdgeFn_Int64>(FN)(_s, _t, _src, _dst, _e);\
      *static_cast<int64_t*>(OUT) = r.value;                                 \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kUInt32: {                                               \
      auto r = reinterpret_cast<JitEdgeFn_UInt32>(FN)(_s, _t, _src, _dst, _e);\
      *static_cast<uint32_t*>(OUT) = r.value;                                \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kUInt64: {                                               \
      auto r = reinterpret_cast<JitEdgeFn_UInt64>(FN)(_s, _t, _src, _dst, _e);\
      *static_cast<uint64_t*>(OUT) = r.value;                                \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kFloat: {                                                \
      auto r = reinterpret_cast<JitEdgeFn_Float>(FN)(_s, _t, _src, _dst, _e);\
      *static_cast<float*>(OUT) = r.value;                                   \
      return r.is_null != 0;                                                  \
    }                                                                         \
    case DataTypeId::kDouble: {                                               \
      auto r = reinterpret_cast<JitEdgeFn_Double>(FN)(_s, _t, _src, _dst, _e);\
      *static_cast<double*>(OUT) = r.value;                                  \
      return r.is_null != 0;                                                  \
    }                                                                         \
    default:                                                                  \
      LOG(FATAL) << "Unsupported JIT result type";                           \
      return true;                                                            \
    }                                                                         \
  } while (0)

bool JitCompiledExpr::typed_eval_record(const Context& ctx, size_t idx,
                                        void* out_value) const {
  if (!record_fn_) {
    LOG(FATAL) << "JIT record function not compiled";
    return true;
  }
  JIT_TYPED_DISPATCH_RECORD(result_type_, record_fn_, record_slots_.get(),
                            &ctx, static_cast<uint64_t>(idx), out_value);
}

bool JitCompiledExpr::typed_eval_vertex(label_t v_label, vid_t v_id,
                                        void* out_value) const {
  if (!vertex_fn_) {
    LOG(FATAL) << "JIT vertex function not compiled";
    return true;
  }
  JIT_TYPED_DISPATCH_VERTEX(result_type_, vertex_fn_, vertex_slots_.get(),
                            static_cast<uint8_t>(v_label),
                            static_cast<uint32_t>(v_id), out_value);
}

bool JitCompiledExpr::typed_eval_edge(const LabelTriplet& label, vid_t src,
                                      vid_t dst, const void* edata,
                                      void* out_value) const {
  if (!edge_fn_) {
    LOG(FATAL) << "JIT edge function not compiled";
    return true;
  }
  JIT_TYPED_DISPATCH_EDGE(result_type_, edge_fn_, edge_slots_.get(),
                          &label, static_cast<uint32_t>(src),
                          static_cast<uint32_t>(dst), edata, out_value);
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
