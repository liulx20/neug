#pragma once
#include <vector>

#include "neug/execution/vectorized/pipeline/operator_base.h"
#include "neug/utils/property/types.h"

namespace neug::execution::vec {

class GetVFilterOperator : public IVecOperator {
 public:
	GetVFilterOperator(int vertex_tag, std::vector<label_t> allowed_labels);

	std::string GetName() const override { return "GetVFilterOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override {
		return std::make_unique<OperatorState>();
	}

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState&,
	                           const VecExecContext& ctx) override;

 private:
	int vertex_tag_;
	std::vector<label_t> allowed_labels_;
};

}  // namespace neug::execution::vec
