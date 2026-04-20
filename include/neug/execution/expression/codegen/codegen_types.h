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
