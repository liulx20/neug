#pragma once
#ifdef NEUG_ENABLE_JIT_EXPRESSION

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Value.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/expression/codegen/codegen_types.h"

namespace neug {

class IStorageInterface;

namespace execution {

namespace codegen {

enum class EvalMode { kRecord, kVertex, kEdge };

// Represents a codegen'd value: an LLVM value + null flag
struct CodegenValue {
  llvm::Value* value;    // The actual primitive value
  llvm::Value* is_null;  // i8, 0 = not null, 1 = null
};

class ExprCodegen {
 public:
  ExprCodegen();

  // Check if a type is supported for JIT codegen (primitive types only)
  static bool isTypeSupported(DataTypeId type_id);

  // Check if an expression tree can be fully codegen'd
  static bool canCodegen(const ExprBase* expr);

  // Compile an expression tree into a JIT function (without binding leaves).
  // Only generates IR and JIT-compiles it. Leaf nodes are recorded as
  // ExprBase* pointers in unbound_leaves for later binding.
  // Returns the JIT function pointer, or nullptr on failure.
  void* compileRecordExpr(const ExprBase* expr,
                          std::unique_ptr<UnboundLeafSlots>& unbound_leaves);

  void* compileVertexExpr(const ExprBase* expr,
                          std::unique_ptr<UnboundLeafSlots>& unbound_leaves);

  void* compileEdgeExpr(const ExprBase* expr,
                        std::unique_ptr<UnboundLeafSlots>& unbound_leaves);

 private:
  // Recursively generate LLVM IR for an ExprBase node.
  // Leaf nodes are recorded in unbound_leaves (no bind happens here).
  CodegenValue generateExprIR(const ExprBase* expr,
                              llvm::IRBuilder<>& builder,
                              llvm::Value* slots_ptr,
                              llvm::Value* context_ptr,
                              llvm::Value* idx_val,
                              UnboundLeafSlots& unbound_leaves);

  // Get the LLVM type corresponding to a DataTypeId
  llvm::Type* getLLVMType(llvm::LLVMContext& ctx, DataTypeId type_id);

  // Get the LLVM struct type for NullableValue<T>
  llvm::StructType* getNullableType(llvm::LLVMContext& ctx, DataTypeId type_id);

  // Create a null CodegenValue
  CodegenValue createNull(llvm::IRBuilder<>& builder, DataTypeId type_id);

  // Create a non-null CodegenValue from an LLVM value
  CodegenValue createNonNull(llvm::IRBuilder<>& builder, llvm::Value* value);

  // Generate a trampoline call IR for a leaf expression.
  // Records the ExprBase* in unbound_leaves (does NOT call bind).
  CodegenValue generateLeafCallIR(const ExprBase* expr,
                                  llvm::IRBuilder<>& builder,
                                  llvm::Value* slots_ptr,
                                  llvm::Value* context_ptr,
                                  llvm::Value* idx_val,
                                  UnboundLeafSlots& unbound_leaves);

  // Generate null-propagating arithmetic IR
  CodegenValue generateArithIR(const CodegenValue& lhs,
                               const CodegenValue& rhs,
                               DataTypeId result_type,
                               int arith_op,
                               llvm::IRBuilder<>& builder);

  // Generate comparison/logical IR
  CodegenValue generateCompareIR(const CodegenValue& lhs,
                                 const CodegenValue& rhs,
                                 int logical_op,
                                 DataTypeId operand_type,
                                 llvm::IRBuilder<>& builder);

  // Generate AND with short-circuit
  CodegenValue generateAndIR(const ExprBase* lhs_expr,
                             const ExprBase* rhs_expr,
                             llvm::IRBuilder<>& builder,
                             llvm::Value* slots_ptr,
                             llvm::Value* context_ptr,
                             llvm::Value* idx_val,
                             UnboundLeafSlots& unbound_leaves);

  // Generate OR with short-circuit
  CodegenValue generateOrIR(const ExprBase* lhs_expr,
                            const ExprBase* rhs_expr,
                            llvm::IRBuilder<>& builder,
                            llvm::Value* slots_ptr,
                            llvm::Value* context_ptr,
                            llvm::Value* idx_val,
                            UnboundLeafSlots& unbound_leaves);

  // Generate NOT IR
  CodegenValue generateNotIR(const CodegenValue& operand,
                             llvm::IRBuilder<>& builder);

  // Generate ISNULL IR
  CodegenValue generateIsNullIR(const CodegenValue& operand,
                                llvm::IRBuilder<>& builder);

  static std::atomic<uint64_t> func_counter_;
  EvalMode eval_mode_{EvalMode::kRecord};
};

}  // namespace codegen
}  // namespace execution
}  // namespace neug

#endif  // NEUG_ENABLE_JIT_EXPRESSION
