#include "neug/execution/vectorized/ops/vec_expr_filter_operator.h"

#include "neug/execution/vectorized/core/selection_vector.h"

namespace neug::execution::vec {

OperatorResultType VecExprFilterOperator::Execute(GraphDataChunk& input,
                                                   GraphDataChunk& output,
                                                   OperatorState&,
                                                   const VecExecContext& ctx) {
	size_t count = input.size();
	if (count == 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	input.Flatten();

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector bool_vec{bool_type};
	predicate_->Evaluate(input, count, bool_vec, ctx);

	const auto* flags = bool_vec.GetData<bool>();
	SelectionVector sel(count);
	size_t selected = 0;
	for (size_t i = 0; i < count; i++) {
		if (flags[i]) {
			sel.SetIndex(selected++, static_cast<sel_t>(i));
		}
	}

	if (selected == 0) {
		output = std::move(input);
		output.SetCardinality(0);
	} else if (selected == count) {
		output = std::move(input);
	} else {
		input.Compact(sel, selected);
		output = std::move(input);
	}
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
