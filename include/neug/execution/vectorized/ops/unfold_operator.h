#pragma once
#include <memory>
#include <string>

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class UnfoldOperator : public IVecOperator {
 public:
	UnfoldOperator(int input_tag, int output_alias, DataType elem_type);

	std::string GetName() const override { return "UnfoldOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	int input_tag_;
	int output_alias_;
	DataType elem_type_;
};

}  // namespace neug::execution::vec
