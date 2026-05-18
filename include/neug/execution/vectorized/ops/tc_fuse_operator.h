#pragma once
#include <array>
#include <memory>
#include <string>
#include <tuple>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class TCFuseOperator : public IVecOperator {
 public:
	TCFuseOperator(
	    int input_tag, int alias1, int alias2,
	    std::array<std::tuple<label_t, label_t, label_t, execution::Direction>, 3>
	        labels,
	    bool is_lt, std::string param_name, DataTypeId edge_prop_type);

	std::string GetName() const override { return "TCFuseOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	int input_tag_;
	int alias1_;
	int alias2_;
	std::array<std::tuple<label_t, label_t, label_t, execution::Direction>, 3>
	    labels_;
	bool is_lt_;
	std::string param_name_;
	DataTypeId edge_prop_type_;
};

}  // namespace neug::execution::vec
