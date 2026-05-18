#include "neug/execution/vectorized/ops/unfold_operator.h"

#include <algorithm>
#include <cstring>

#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/list_vector_buffer.h"
#include "neug/execution/vectorized/expression/vec_expression.h"

namespace neug::execution::vec {

struct UnfoldState : public OperatorState {
	GraphDataChunk pending_input;
	size_t current_row = 0;
	size_t current_elem_offset = 0;
	bool has_pending = false;
};

UnfoldOperator::UnfoldOperator(int input_tag, int output_alias,
                               DataType elem_type)
    : input_tag_(input_tag),
      output_alias_(output_alias),
      elem_type_(std::move(elem_type)) {}

std::unique_ptr<OperatorState> UnfoldOperator::GetOperatorState() const {
	return std::make_unique<UnfoldState>();
}

OperatorResultType UnfoldOperator::Execute(GraphDataChunk& input,
                                           GraphDataChunk& output,
                                           OperatorState& ostate,
                                           const VecExecContext&) {
	auto& state = static_cast<UnfoldState&>(ostate);

	GraphDataChunk* src;
	if (state.has_pending) {
		src = &state.pending_input;
	} else {
		if (input.size() == 0) {
			output = std::move(input);
			return OperatorResultType::kNeedMoreInput;
		}
		input.Flatten();
		state.pending_input = std::move(input);
		state.current_row = 0;
		state.current_elem_offset = 0;
		state.has_pending = true;
		src = &state.pending_input;
	}

	size_t src_count = src->size();
	int list_col_idx = src->FindColumnByTag(input_tag_);
	if (list_col_idx < 0) {
		output = GraphDataChunk();
		output.SetCardinality(0);
		state.has_pending = false;
		return OperatorResultType::kNeedMoreInput;
	}

	const auto& list_vec = src->GetVector(static_cast<size_t>(list_col_idx));
	const list_entry_t* entries = ListVector::GetEntries(list_vec);
	const auto& child_vec = ListVector::GetChild(list_vec);

	// Collect output rows up to STANDARD_VECTOR_SIZE
	std::vector<size_t> src_rows;
	std::vector<size_t> elem_offsets;
	src_rows.reserve(STANDARD_VECTOR_SIZE);
	elem_offsets.reserve(STANDARD_VECTOR_SIZE);

	while (state.current_row < src_count &&
	       src_rows.size() < STANDARD_VECTOR_SIZE) {
		const auto& entry = entries[state.current_row];
		size_t remaining_in_row = entry.length - state.current_elem_offset;
		size_t can_take =
		    std::min(remaining_in_row,
		             STANDARD_VECTOR_SIZE - src_rows.size());

		for (size_t j = 0; j < can_take; j++) {
			src_rows.push_back(state.current_row);
			elem_offsets.push_back(entry.offset + state.current_elem_offset + j);
		}

		state.current_elem_offset += can_take;
		if (state.current_elem_offset >= entry.length) {
			state.current_row++;
			state.current_elem_offset = 0;
		}
	}

	if (src_rows.empty()) {
		output = GraphDataChunk();
		output.SetCardinality(0);
		state.has_pending = false;
		return OperatorResultType::kNeedMoreInput;
	}

	size_t out_count = src_rows.size();
	output = GraphDataChunk();

	// Copy non-list columns, duplicating rows
	for (size_t col = 0; col < src->ColumnCount(); col++) {
		int tag = src->GetTag(col);
		if (tag == input_tag_) continue;
		const auto& vec = src->GetVector(col);

		std::vector<sel_t> sel(out_count);
		for (size_t i = 0; i < out_count; i++) {
			sel[i] = static_cast<sel_t>(src_rows[i]);
		}
		auto sliced = GraphVector::Slice(vec, sel.data(), out_count);
		output.AddColumn(tag, std::move(sliced));
	}

	// Extract list elements into a new column
	DataTypeId elem_tid = elem_type_.id();
	if (elem_tid == DataTypeId::kVarchar) {
		GraphVector out_vec{elem_type_};
		auto* child_str = StringVector::GetStringData(
		    const_cast<GraphVector&>(child_vec));
		auto* out_str = StringVector::GetStringData(out_vec);
		for (size_t i = 0; i < out_count; i++) {
			out_str[i] = StringVector::AddString(
			    out_vec, child_str[elem_offsets[i]].GetString());
		}
		output.AddColumn(output_alias_, std::move(out_vec));
	} else if (elem_tid == DataTypeId::kVertex) {
		auto out_vec = VertexVector::Create();
		vid_t* out_vids = VertexVector::GetVids(out_vec);
		label_t* out_labels = VertexVector::GetLabels(out_vec);
		const vid_t* child_vids = VertexVector::GetVids(child_vec);
		if (VertexVector::IsConstantLabel(child_vec)) {
			label_t lbl = VertexVector::GetConstantLabel(child_vec);
			for (size_t i = 0; i < out_count; i++) {
				out_vids[i] = child_vids[elem_offsets[i]];
				out_labels[i] = lbl;
			}
		} else {
			const label_t* child_labels = VertexVector::GetLabels(child_vec);
			for (size_t i = 0; i < out_count; i++) {
				out_vids[i] = child_vids[elem_offsets[i]];
				out_labels[i] = child_labels[elem_offsets[i]];
			}
		}
		output.AddColumn(output_alias_, std::move(out_vec));
	} else {
		GraphVector out_vec{elem_type_};
		size_t type_size = GetTypeSize(elem_tid);
		auto* out_data = out_vec.GetData<char>();
		const auto* child_data = child_vec.GetData<char>();
		for (size_t i = 0; i < out_count; i++) {
			std::memcpy(out_data + i * type_size,
			            child_data + elem_offsets[i] * type_size, type_size);
		}
		output.AddColumn(output_alias_, std::move(out_vec));
	}

	output.SetCardinality(out_count);

	bool done = (state.current_row >= src_count);
	if (done) {
		state.has_pending = false;
		return OperatorResultType::kNeedMoreInput;
	}
	return OperatorResultType::kHaveMoreOutput;
}

}  // namespace neug::execution::vec
