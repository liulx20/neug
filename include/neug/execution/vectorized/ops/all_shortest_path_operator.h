#pragma once
#include <string>
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class AllShortestPathOperator : public IVecOperator {
 public:
	AllShortestPathOperator(int start_tag, int v_alias, int path_alias,
	                        execution::LabelTriplet label, int hop_lower,
	                        int hop_upper, std::string dest_param_name,
	                        int64_t dest_const, bool use_param);

	std::string GetName() const override { return "AllShortestPathOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	int start_tag_;
	int v_alias_;
	int path_alias_;
	execution::LabelTriplet label_;
	int hop_lower_;
	int hop_upper_;
	std::string dest_param_name_;
	int64_t dest_const_;
	bool use_param_;
};

}  // namespace neug::execution::vec
