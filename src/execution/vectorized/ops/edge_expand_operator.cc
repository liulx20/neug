#include "neug/execution/vectorized/ops/edge_expand_operator.h"

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/csr/generic_view.h"

namespace neug::execution::vec {

struct EdgeExpandState : public OperatorState {
	bool has_pending = false;
	GraphDataChunk pending_input;
	size_t row_idx = 0;

	struct ViewInfo {
		GenericView view;
		label_t nbr_label;
		label_t src_label;
		label_t dst_label;
		label_t edge_label;
	};
	std::vector<ViewInfo> views;
	size_t view_idx = 0;
	NbrIterator cur_iter;
	NbrIterator end_iter;
	bool iter_active = false;

	std::vector<sel_t> offsets;
	std::vector<vid_t> nbr_vids;
	std::vector<label_t> nbr_labels;

	bool found_neighbor_for_current_row = false;
	std::vector<size_t> null_positions;

	std::vector<const void*> data_ptrs;
	std::vector<uint16_t> view_indices;
};

EdgeExpandOperator::EdgeExpandOperator(
    int v_tag, int output_alias,
    std::vector<execution::LabelTriplet> labels, execution::Direction dir,
    bool is_optional, std::unique_ptr<VecExpression> predicate,
    std::vector<EdgePropInfo> edge_props)
    : v_tag_(v_tag),
      output_alias_(output_alias),
      labels_(std::move(labels)),
      dir_(dir),
      is_optional_(is_optional),
      predicate_(std::move(predicate)),
      edge_props_(std::move(edge_props)) {}

std::unique_ptr<OperatorState> EdgeExpandOperator::GetOperatorState() const {
	return std::make_unique<EdgeExpandState>();
}

static void BuildOutputFromOffsets(const GraphDataChunk& input,
                                   GraphDataChunk& output,
                                   int output_alias,
                                   const std::vector<sel_t>& offsets,
                                   const std::vector<vid_t>& nbr_vids,
                                   const std::vector<label_t>& nbr_labels,
                                   size_t count,
                                   const std::vector<size_t>& null_positions) {
	output = GraphDataChunk();

	for (size_t col = 0; col < input.ColumnCount(); col++) {
		int tag = input.GetTag(col);
		if (tag == output_alias) continue;
		const auto& vec = input.GetVector(col);
		auto sliced = GraphVector::Slice(vec, offsets.data(), count);
		output.AddColumn(tag, std::move(sliced));
	}

	auto nbr_vec = VertexVector::Create();
	vid_t* out_vids = VertexVector::GetVids(nbr_vec);
	label_t* out_labels = VertexVector::GetLabels(nbr_vec);
	for (size_t i = 0; i < count; i++) {
		out_vids[i] = nbr_vids[i];
		out_labels[i] = nbr_labels[i];
	}
	if (!null_positions.empty()) {
		for (size_t pos : null_positions) {
			nbr_vec.validity().SetInvalid(pos);
		}
	}
	output.AddColumn(output_alias, std::move(nbr_vec));
	output.SetCardinality(count);
}

static void WritePropertyToVector(GraphVector& vec, size_t idx,
                                  const Property& prop) {
	switch (prop.type()) {
	case DataTypeId::kInt32:
		vec.GetData<int32_t>()[idx] = prop.as_int32();
		break;
	case DataTypeId::kInt64:
		vec.GetData<int64_t>()[idx] = prop.as_int64();
		break;
	case DataTypeId::kUInt32:
		vec.GetData<uint32_t>()[idx] = prop.as_uint32();
		break;
	case DataTypeId::kUInt64:
		vec.GetData<uint64_t>()[idx] = prop.as_uint64();
		break;
	case DataTypeId::kFloat:
		vec.GetData<float>()[idx] = prop.as_float();
		break;
	case DataTypeId::kDouble:
		vec.GetData<double>()[idx] = prop.as_double();
		break;
	case DataTypeId::kBoolean:
		vec.GetData<bool>()[idx] = prop.as_bool();
		break;
	case DataTypeId::kVarchar: {
		auto sv = prop.as_string_view();
		StringVector::GetStringData(vec)[idx] =
		    StringVector::AddString(vec, std::string(sv));
		break;
	}
	default:
		break;
	}
}

OperatorResultType EdgeExpandOperator::Execute(GraphDataChunk& input,
                                               GraphDataChunk& output,
                                               OperatorState& ostate,
                                          const VecExecContext& ctx) {
	auto& state = static_cast<EdgeExpandState&>(ostate);

	GraphDataChunk* src;
	if (state.has_pending) {
		src = &state.pending_input;
	} else {
		if (input.size() == 0) {
			output = std::move(input);
			return OperatorResultType::kNeedMoreInput;
		}
		state.pending_input = std::move(input);
		state.has_pending = true;
		state.row_idx = 0;
		state.views.clear();
		state.view_idx = 0;
		state.iter_active = false;
		state.found_neighbor_for_current_row = false;
		src = &state.pending_input;
	}

	state.offsets.clear();
	state.nbr_vids.clear();
	state.nbr_labels.clear();
	state.null_positions.clear();
	state.data_ptrs.clear();
	state.view_indices.clear();

	int v_col_idx = src->FindColumnByTag(v_tag_);
	assert(v_col_idx >= 0);
	const auto& vertex_vec = src->GetVector(static_cast<size_t>(v_col_idx));
	const vid_t* vids = VertexVector::GetVids(vertex_vec);
	const bool has_sel = vertex_vec.IsDictionary();
	const SelectionVector* v_sel = has_sel ? &vertex_vec.sel() : nullptr;

	size_t input_size = src->size();
	const bool has_pred = predicate_ != nullptr;

	auto flush_with_filter = [&]() {
		if (state.offsets.empty()) return;

		size_t count = state.offsets.size();
		GraphDataChunk temp_chunk;

		for (auto& prop_info : edge_props_) {
			GraphVector prop_vec(prop_info.type);
			for (size_t i = 0; i < count; i++) {
				auto& vi = state.views[state.view_indices[i]];
				auto accessor = ctx.graph->GetEdgeDataAccessor(
				    vi.src_label, vi.dst_label, vi.edge_label,
				    prop_info.prop_name);
				Property val = accessor.get_data_from_ptr(state.data_ptrs[i]);
				WritePropertyToVector(prop_vec, i, val);
			}
			temp_chunk.AddColumn(prop_info.tag, std::move(prop_vec));
		}
		temp_chunk.SetCardinality(count);

		DataType bool_type{DataTypeId::kBoolean};
		GraphVector bool_vec{bool_type};
		predicate_->Evaluate(temp_chunk, count, bool_vec, ctx);
		const auto* flags = bool_vec.GetData<bool>();

		std::vector<sel_t> filtered_offsets;
		std::vector<vid_t> filtered_vids;
		std::vector<label_t> filtered_labels;
		for (size_t i = 0; i < count; i++) {
			if (flags[i]) {
				filtered_offsets.push_back(state.offsets[i]);
				filtered_vids.push_back(state.nbr_vids[i]);
				filtered_labels.push_back(state.nbr_labels[i]);
			}
		}

		state.offsets = std::move(filtered_offsets);
		state.nbr_vids = std::move(filtered_vids);
		state.nbr_labels = std::move(filtered_labels);
		state.data_ptrs.clear();
		state.view_indices.clear();
	};

	for (; state.row_idx < input_size; state.row_idx++) {
		size_t phys_idx = has_sel
		    ? v_sel->GetIndex(state.row_idx)
		    : state.row_idx;
		vid_t vid = vids[phys_idx];

		label_t v_label;
		if (VertexVector::IsConstantLabel(vertex_vec)) {
			v_label = VertexVector::GetConstantLabel(vertex_vec);
		} else {
			v_label = VertexVector::GetLabels(vertex_vec)[phys_idx];
		}

		if (!state.iter_active && state.view_idx == 0) {
			state.views.clear();
			state.found_neighbor_for_current_row = false;
			for (auto& triplet : labels_) {
				if (dir_ == execution::Direction::kOut ||
				    dir_ == execution::Direction::kBoth) {
					if (triplet.src_label == v_label) {
						EdgeExpandState::ViewInfo vi;
						vi.view = ctx.graph->GetGenericOutgoingGraphView(
						    v_label, triplet.dst_label,
						    triplet.edge_label);
						vi.nbr_label = triplet.dst_label;
						vi.src_label = triplet.src_label;
						vi.dst_label = triplet.dst_label;
						vi.edge_label = triplet.edge_label;
						state.views.push_back(std::move(vi));
					}
				}
				if (dir_ == execution::Direction::kIn ||
				    dir_ == execution::Direction::kBoth) {
					if (triplet.dst_label == v_label) {
						EdgeExpandState::ViewInfo vi;
						vi.view = ctx.graph->GetGenericIncomingGraphView(
						    v_label, triplet.src_label,
						    triplet.edge_label);
						vi.nbr_label = triplet.src_label;
						vi.src_label = triplet.src_label;
						vi.dst_label = triplet.dst_label;
						vi.edge_label = triplet.edge_label;
						state.views.push_back(std::move(vi));
					}
				}
			}
		}

		while (state.view_idx < state.views.size()) {
			if (!state.iter_active) {
				auto edges = state.views[state.view_idx].view.get_edges(vid);
				state.cur_iter = edges.begin();
				state.end_iter = edges.end();
				state.iter_active = true;
			}

			while (state.cur_iter != state.end_iter) {
				state.offsets.push_back(
				    static_cast<sel_t>(state.row_idx));
				state.nbr_vids.push_back(state.cur_iter.get_vertex());
				state.nbr_labels.push_back(
				    state.views[state.view_idx].nbr_label);
				state.found_neighbor_for_current_row = true;

				if (has_pred) {
					state.data_ptrs.push_back(
					    state.cur_iter.get_data_ptr());
					state.view_indices.push_back(
					    static_cast<uint16_t>(state.view_idx));
				}

				++state.cur_iter;

				if (state.offsets.size() >= STANDARD_VECTOR_SIZE) {
					if (has_pred) {
						flush_with_filter();
					}
					if (!state.offsets.empty()) {
						BuildOutputFromOffsets(
						    *src, output, output_alias_,
						    state.offsets, state.nbr_vids,
						    state.nbr_labels, state.offsets.size(),
						    state.null_positions);
						state.offsets.clear();
						state.nbr_vids.clear();
						state.nbr_labels.clear();
						state.null_positions.clear();
						state.data_ptrs.clear();
						state.view_indices.clear();
						return OperatorResultType::kHaveMoreOutput;
					}
				}
			}

			state.iter_active = false;
			state.view_idx++;
		}

		if (is_optional_ && !state.found_neighbor_for_current_row) {
			state.offsets.push_back(static_cast<sel_t>(state.row_idx));
			state.nbr_vids.push_back(std::numeric_limits<vid_t>::max());
			state.nbr_labels.push_back(0);
			state.null_positions.push_back(state.offsets.size() - 1);
			if (has_pred) {
				state.data_ptrs.push_back(nullptr);
				state.view_indices.push_back(0);
			}

			if (state.offsets.size() >= STANDARD_VECTOR_SIZE) {
				if (has_pred) {
					flush_with_filter();
				}
				if (!state.offsets.empty()) {
					BuildOutputFromOffsets(
					    *src, output, output_alias_,
					    state.offsets, state.nbr_vids,
					    state.nbr_labels, state.offsets.size(),
					    state.null_positions);
					state.offsets.clear();
					state.nbr_vids.clear();
					state.nbr_labels.clear();
					state.null_positions.clear();
					state.data_ptrs.clear();
					state.view_indices.clear();
					state.row_idx++;
					state.view_idx = 0;
					state.iter_active = false;
					return OperatorResultType::kHaveMoreOutput;
				}
			}
		}

		state.view_idx = 0;
		state.iter_active = false;
	}

	if (has_pred && !state.offsets.empty()) {
		flush_with_filter();
	}

	if (!state.offsets.empty()) {
		BuildOutputFromOffsets(*src, output, output_alias_, state.offsets,
		                      state.nbr_vids, state.nbr_labels,
		                      state.offsets.size(), state.null_positions);
	} else {
		output = GraphDataChunk();
		output.SetCardinality(0);
	}

	state.has_pending = false;
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
