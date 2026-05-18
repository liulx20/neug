#pragma once
#include <string>

#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class PropertyReadOperator : public IVecOperator {
 public:
	PropertyReadOperator(int vertex_tag, std::string prop_name, int output_tag);

	std::string GetName() const override { return "PropertyReadOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	int vertex_tag_;
	std::string prop_name_;
	int output_tag_;
};

}  // namespace neug::execution::vec
