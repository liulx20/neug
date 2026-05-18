#pragma once
#include <string>
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class PathExpandOperator : public IVecOperator {
 public:
	PathExpandOperator(int start_tag, int output_alias,
	                   std::vector<execution::LabelTriplet> labels,
	                   execution::Direction dir, int hop_lower, int hop_upper);

	std::string GetName() const override { return "PathExpandOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	int start_tag_;
	int output_alias_;
	std::vector<execution::LabelTriplet> labels_;
	execution::Direction dir_;
	int hop_lower_;
	int hop_upper_;
};

}  // namespace neug::execution::vec
