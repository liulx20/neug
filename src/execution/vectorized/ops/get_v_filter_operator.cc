#include "neug/execution/vectorized/ops/get_v_filter_operator.h"

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/core/selection_vector.h"

namespace neug::execution::vec {

GetVFilterOperator::GetVFilterOperator(int vertex_tag,
                                       std::vector<label_t> allowed_labels)
    : vertex_tag_(vertex_tag),
      allowed_labels_(std::move(allowed_labels)) {}

OperatorResultType GetVFilterOperator::Execute(GraphDataChunk& input,
                                                GraphDataChunk& output,
                                                OperatorState&,
                                                const VecExecContext&) {
	size_t count = input.size();
	if (count == 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	int col_idx = input.FindColumnByTag(vertex_tag_);
	if (col_idx < 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	const auto& vec = input.GetVector(static_cast<size_t>(col_idx));

	if (VertexVector::IsConstantLabel(vec)) {
		label_t label = VertexVector::GetConstantLabel(vec);
		bool found = false;
		for (auto l : allowed_labels_) {
			if (l == label) {
				found = true;
				break;
			}
		}
		if (found) {
			output = std::move(input);
		} else {
			output = std::move(input);
			output.SetCardinality(0);
		}
		return OperatorResultType::kNeedMoreInput;
	}

	input.Flatten();
	const label_t* labels = VertexVector::GetLabels(vec);

	SelectionVector sel(count);
	size_t selected = 0;
	for (size_t i = 0; i < count; i++) {
		for (auto l : allowed_labels_) {
			if (labels[i] == l) {
				sel.SetIndex(selected++, static_cast<sel_t>(i));
				break;
			}
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
