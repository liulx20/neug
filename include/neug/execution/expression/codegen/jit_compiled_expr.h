#pragma once
#ifdef NEUG_ENABLE_JIT_EXPRESSION

#include "neug/execution/expression/codegen/codegen_types.h"
#include "neug/execution/expression/codegen/expr_codegen.h"
#include "neug/execution/expression/expr.h"
#include <memory>

namespace neug {
namespace execution {
namespace codegen {

// ============================================================================
// JitCompiledTemplate: pre-compiled JIT function + unbound leaf list.
//
// This separates JIT compilation from binding:
//   Phase 1 (compile): ExprCodegen generates IR and JIT-compiles it.
//                       Leaf nodes are recorded as ExprBase* pointers.
//   Phase 2 (bind):    bindLeaves() iterates the unbound leaves, calls
//                       bind(storage, params), and produces LeafExprSlots.
//
// A single JitCompiledTemplate can be bound multiple times with different
// storage/params, avoiding redundant LLVM IR generation and JIT compilation.
// ============================================================================
class JitCompiledTemplate {
 public:
  JitCompiledTemplate(void* function_ptr,
                      DataTypeId result_type,
                      EvalMode eval_mode,
                      std::unique_ptr<UnboundLeafSlots> unbound_leaves)
      : function_ptr_(function_ptr),
        result_type_(result_type),
        eval_mode_(eval_mode),
        unbound_leaves_(std::move(unbound_leaves)) {}

  // Bind all leaf expressions with the given storage and params.
  // Returns a LeafExprSlots ready for use by the JIT function.
  std::unique_ptr<LeafExprSlots> bindLeaves(
      const IStorageInterface* storage,
      const ParamsMap& params) const;

  void* function_ptr() const { return function_ptr_; }
  DataTypeId result_type() const { return result_type_; }
  EvalMode eval_mode() const { return eval_mode_; }
  bool valid() const { return function_ptr_ != nullptr; }

 private:
  void* function_ptr_;
  DataTypeId result_type_;
  EvalMode eval_mode_;
  std::unique_ptr<UnboundLeafSlots> unbound_leaves_;
};

// ============================================================================
// JitCompiledExpr: bound JIT expression ready for evaluation.
//
// Holds the JIT function pointer + bound LeafExprSlots.
// Inherits from VertexExprBase, EdgeExprBase, and RecordExprBase so that
// GeneralPred can call whichever eval method matches the usage context.
// ============================================================================
class JitCompiledExpr : public VertexExprBase,
                        public EdgeExprBase,
                        public RecordExprBase {
 public:
  JitCompiledExpr(void* record_fn, void* vertex_fn, void* edge_fn,
                  DataTypeId result_type,
                  std::unique_ptr<LeafExprSlots> record_slots,
                  std::unique_ptr<LeafExprSlots> vertex_slots,
                  std::unique_ptr<LeafExprSlots> edge_slots)
      : record_fn_(record_fn),
        vertex_fn_(vertex_fn),
        edge_fn_(edge_fn),
        result_type_(result_type),
        type_(result_type),
        record_slots_(std::move(record_slots)),
        vertex_slots_(std::move(vertex_slots)),
        edge_slots_(std::move(edge_slots)) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override;
  Value eval_vertex(label_t v_label, vid_t v_id) const override;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* edata) const override;

  bool typed_eval_record(const Context& ctx, size_t idx,
                         void* out_value) const override;
  bool typed_eval_vertex(label_t v_label, vid_t v_id,
                         void* out_value) const override;
  bool typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                       const void* edata, void* out_value) const override;

 private:
  void* record_fn_;
  void* vertex_fn_;
  void* edge_fn_;
  DataTypeId result_type_;
  DataType type_;
  std::unique_ptr<LeafExprSlots> record_slots_;
  std::unique_ptr<LeafExprSlots> vertex_slots_;
  std::unique_ptr<LeafExprSlots> edge_slots_;
};

// ============================================================================
// Two-phase API: compile first, bind later
// ============================================================================

// Phase 1: Compile an expression tree into a JitCompiledTemplate.
// This is expensive (LLVM IR generation + JIT compilation) but only needs
// to be done once per expression tree.
// Returns nullptr if codegen is not possible.
std::shared_ptr<JitCompiledTemplate> compileExprTemplate(
    const ExprBase* expr,
    EvalMode eval_mode);

// Phase 2: Bind a pre-compiled template with storage/params to produce
// a ready-to-evaluate JitCompiledExpr.
std::unique_ptr<BindedExprBase> bindTemplate(
    const std::shared_ptr<JitCompiledTemplate>& tmpl,
    const IStorageInterface* storage,
    const ParamsMap& params);

// ============================================================================
// One-shot API: compile + bind in one call (convenience wrappers)
// ============================================================================

std::unique_ptr<BindedExprBase> tryJitCompileRecord(
    const ExprBase* expr,
    const IStorageInterface* storage,
    const ParamsMap& params);

std::unique_ptr<BindedExprBase> tryJitCompileVertex(
    const ExprBase* expr,
    const IStorageInterface* storage,
    const ParamsMap& params);

std::unique_ptr<BindedExprBase> tryJitCompileEdge(
    const ExprBase* expr,
    const IStorageInterface* storage,
    const ParamsMap& params);

}  // namespace codegen
}  // namespace execution
}  // namespace neug

#endif  // NEUG_ENABLE_JIT_EXPRESSION
