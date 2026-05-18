#pragma once
#include <memory>

#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class VecExprFilterOperator : public IVecOperator {
 public:
	explicit VecExprFilterOperator(std::unique_ptr<VecExpression> predicate)
	    : predicate_(std::move(predicate)) {}

	std::string GetName() const override { return "VecExprFilterOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override {
		return std::make_unique<OperatorState>();
	}

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState&,
	                           const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> predicate_;
};

}  // namespace neug::execution::vec
