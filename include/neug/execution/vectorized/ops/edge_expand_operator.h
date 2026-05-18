#pragma once
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

struct EdgePropInfo {
	std::string prop_name;
	int tag;
	DataType type;
};

class EdgeExpandOperator : public IVecOperator {
 public:
	EdgeExpandOperator(int v_tag, int output_alias,
	                   std::vector<execution::LabelTriplet> labels,
	                   execution::Direction dir,
	                   bool is_optional = false,
	                   std::unique_ptr<VecExpression> predicate = nullptr,
	                   std::vector<EdgePropInfo> edge_props = {});

	std::string GetName() const override { return "EdgeExpandOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	int v_tag_;
	int output_alias_;
	std::vector<execution::LabelTriplet> labels_;
	execution::Direction dir_;
	bool is_optional_;
	std::unique_ptr<VecExpression> predicate_;
	std::vector<EdgePropInfo> edge_props_;
};

}  // namespace neug::execution::vec
