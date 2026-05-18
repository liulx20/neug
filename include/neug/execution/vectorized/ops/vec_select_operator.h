#pragma once
#include "neug/execution/common/types/value.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class VecSelectOperator : public IVecOperator {
 public:
	VecSelectOperator(int column_tag, CompareOp op,
	                  neug::execution::Value constant);

	std::string GetName() const override { return "VecSelectOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	int column_tag_;
	CompareOp op_;
	neug::execution::Value constant_;
};

}  // namespace neug::execution::vec
