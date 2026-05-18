#pragma once
#include <string>
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

struct IntersectSubPlan {
	int v_tag;
	std::vector<execution::LabelTriplet> labels;
	execution::Direction dir;
};

class IntersectOperator : public IVecOperator {
 public:
	IntersectOperator(std::vector<IntersectSubPlan> sub_plans, int key_alias);

	std::string GetName() const override { return "IntersectOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	std::vector<IntersectSubPlan> sub_plans_;
	int key_alias_;
};

}  // namespace neug::execution::vec
