#pragma once
#ifdef NEUG_ENABLE_JIT_EXPRESSION

#include <cstdint>
#include <memory>
#include <vector>

namespace neug {
namespace execution {

class ExprBase;
class BindedExprBase;

namespace codegen {

// JIT compiled functions return NullableValue<T> for primitive types.
// The struct layout must match the LLVM IR struct we generate.
// Layout: { T value; int8_t is_null; } (with padding)
template <typename T>
struct NullableValue {
  T value;
  int8_t is_null;  // 0 = not null, 1 = null

  bool IsNull() const { return is_null != 0; }
};

// Concrete types used as JIT function return types
using NullableBool = NullableValue<bool>;
using NullableInt32 = NullableValue<int32_t>;
using NullableInt64 = NullableValue<int64_t>;
using NullableUInt32 = NullableValue<uint32_t>;
using NullableUInt64 = NullableValue<uint64_t>;
using NullableFloat = NullableValue<float>;
using NullableDouble = NullableValue<double>;

// Function pointer types for JIT-compiled expressions.
// All JIT functions take (const void* leaf_slots, ...) as first arg.

// Record eval: fn(slots, ctx_ptr, idx) -> NullableValue<T>
// Vertex eval: fn(slots, label_u8, vid_u32) -> NullableValue<T>
// Edge eval: fn(slots, label_triplet_ptr, src_u32, dst_u32, edata_ptr) -> NullableValue<T>

// Holds binded leaf expressions that the JIT function calls back into.
// The JIT function receives a pointer to this struct to access leaf evaluators.
struct LeafExprSlots {
  std::vector<std::unique_ptr<BindedExprBase>> leaves;
};

// Holds unbound leaf expression pointers collected during IR generation.
// These are later bound with storage/params to produce LeafExprSlots.
// This enables separating JIT compilation from binding:
//   Phase 1 (compile): generate IR + collect ExprBase* into UnboundLeafSlots
//   Phase 2 (bind):    iterate UnboundLeafSlots, call bind(), fill LeafExprSlots
struct UnboundLeafSlots {
  std::vector<const ExprBase*> leaves;
};

}  // namespace codegen
}  // namespace execution
}  // namespace neug

#endif  // NEUG_ENABLE_JIT_EXPRESSION
