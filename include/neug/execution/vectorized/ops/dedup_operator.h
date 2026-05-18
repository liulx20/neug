#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/join/row_layout.h"
#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class DedupOperator : public IVecOperator {
 public:
	DedupOperator(std::vector<int> key_tags, std::vector<DataType> key_types);

	std::string GetName() const override { return "DedupOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& ostate,
	                           const VecExecContext& ctx) override;

 private:
	std::vector<int> key_tags_;
	RowLayout key_layout_;
};

}  // namespace neug::execution::vec
