#include "neug/execution/vectorized/ops/tc_fuse_operator.h"

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/csr/generic_view.h"

namespace neug::execution::vec {

struct TCFuseState : public OperatorState {
	bool has_pending = false;
	GraphDataChunk pending_input;
	size_t row_idx = 0;

	StorageReadInterface::vertex_array_t<bool> d0_set;
	bool d0_set_initialized = false;
	std::vector<vid_t> d0_vec;

	bool bitmap_built = false;
	bool in_edge1_loop = false;
	bool in_edge2_loop = false;

	GenericView csr1_view;
	NbrIterator it1;
	NbrIterator end1;
	vid_t current_nbr1 = 0;

	GenericView csr2_view;
	NbrIterator it2;
	NbrIterator end2;

	std::vector<sel_t> offsets;
	std::vector<vid_t> nbr1_vids;
	std::vector<vid_t> nbr2_vids;
	std::vector<label_t> nbr1_labels;
	std::vector<label_t> nbr2_labels;
};

TCFuseOperator::TCFuseOperator(
    int input_tag, int alias1, int alias2,
    std::array<std::tuple<label_t, label_t, label_t, execution::Direction>, 3>
        labels,
    bool is_lt, std::string param_name, DataTypeId edge_prop_type)
    : input_tag_(input_tag),
      alias1_(alias1),
      alias2_(alias2),
      labels_(labels),
      is_lt_(is_lt),
      param_name_(std::move(param_name)),
      edge_prop_type_(edge_prop_type) {}

std::unique_ptr<OperatorState> TCFuseOperator::GetOperatorState() const {
	return std::make_unique<TCFuseState>();
}

template <typename T>
static void BuildBitmapWithPredicate(const StorageReadInterface& graph,
                                     const GenericView& csr0,
                                     const EdgeDataAccessor& ed_accessor,
                                     vid_t vid, bool is_lt, const T& param,
                                     StorageReadInterface::vertex_array_t<bool>& d0_set,
                                     std::vector<vid_t>& d0_vec) {
	if (csr0.type() == CsrViewType::kMultipleMutable &&
	    ed_accessor.is_bundled()) {
		auto typed_csr =
		    csr0.get_typed_view<T, CsrViewType::kMultipleMutable>();
		if (is_lt) {
			typed_csr.foreach_nbr_lt(vid, param, [&](vid_t u, const T&) {
				d0_set[u] = true;
				d0_vec.push_back(u);
			});
		} else {
			typed_csr.foreach_nbr_gt(vid, param, [&](vid_t u, const T&) {
				d0_set[u] = true;
				d0_vec.push_back(u);
			});
		}
	} else {
		auto es = csr0.get_edges(vid);
		for (auto it = es.begin(); it != es.end(); ++it) {
			auto data = ed_accessor.get_typed_data<T>(it);
			bool pass = is_lt ? (data < param) : (param < data);
			if (pass) {
				auto nbr = it.get_vertex();
				d0_set[nbr] = true;
				d0_vec.push_back(nbr);
			}
		}
	}
}

OperatorResultType TCFuseOperator::Execute(GraphDataChunk& input,
                                           GraphDataChunk& output,
                                           OperatorState& ostate,
                                           const VecExecContext& ctx) {
	auto& state = static_cast<TCFuseState&>(ostate);

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
		state.bitmap_built = false;
		state.in_edge1_loop = false;
		state.in_edge2_loop = false;
		src = &state.pending_input;
	}

	state.offsets.clear();
	state.nbr1_vids.clear();
	state.nbr2_vids.clear();
	state.nbr1_labels.clear();
	state.nbr2_labels.clear();

	int v_col_idx = src->FindColumnByTag(input_tag_);
	assert(v_col_idx >= 0);
	const auto& vertex_vec = src->GetVector(static_cast<size_t>(v_col_idx));
	const vid_t* vids = VertexVector::GetVids(vertex_vec);
	const bool has_sel = vertex_vec.IsDictionary();
	const SelectionVector* v_sel = has_sel ? &vertex_vec.sel() : nullptr;

	label_t input_label;
	if (VertexVector::IsConstantLabel(vertex_vec)) {
		input_label = VertexVector::GetConstantLabel(vertex_vec);
	} else {
		size_t first_phys = has_sel ? v_sel->GetIndex(0) : 0;
		input_label = VertexVector::GetLabels(vertex_vec)[first_phys];
	}

	auto [src0, dst0, e0, dir0] = labels_[0];
	auto [src1, dst1, e1, dir1] = labels_[1];
	auto [src2, dst2, e2, dir2] = labels_[2];

	label_t d0_nbr_label = dst0;
	label_t d1_nbr_label = dst1;
	label_t d2_nbr_label = dst2;

	size_t input_size = src->size();

	for (; state.row_idx < input_size; state.row_idx++) {
		size_t phys_idx =
		    has_sel ? v_sel->GetIndex(state.row_idx) : state.row_idx;
		vid_t vid = vids[phys_idx];

		if (!state.bitmap_built) {
			if (!state.d0_set_initialized) {
				state.d0_set.Init(ctx.graph->GetVertexSet(d0_nbr_label), false);
				state.d0_set_initialized = true;
			}

			auto csr0 =
			    (dir0 == execution::Direction::kOut)
			        ? ctx.graph->GetGenericOutgoingGraphView(
			              input_label, d0_nbr_label, e0)
			        : ctx.graph->GetGenericIncomingGraphView(
			              input_label, d0_nbr_label, e0);

			label_t ed_src = (dir0 == execution::Direction::kOut) ? input_label : d0_nbr_label;
			label_t ed_dst = (dir0 == execution::Direction::kOut) ? d0_nbr_label : input_label;
			auto ed_accessor = ctx.graph->GetEdgeDataAccessor(ed_src, ed_dst, e0, 0);

			assert(ctx.params != nullptr);
			const auto& param_val = ctx.params->at(param_name_);

			state.d0_vec.clear();
			switch (edge_prop_type_) {
			case DataTypeId::kInt32:
				BuildBitmapWithPredicate<int32_t>(
				    *ctx.graph, csr0, ed_accessor, vid, is_lt_,
				    param_val.GetValue<int32_t>(), state.d0_set,
				    state.d0_vec);
				break;
			case DataTypeId::kInt64:
				BuildBitmapWithPredicate<int64_t>(
				    *ctx.graph, csr0, ed_accessor, vid, is_lt_,
				    param_val.GetValue<int64_t>(), state.d0_set,
				    state.d0_vec);
				break;
			case DataTypeId::kTimestampMs:
				BuildBitmapWithPredicate<DateTime>(
				    *ctx.graph, csr0, ed_accessor, vid, is_lt_,
				    param_val.GetValue<DateTime>(), state.d0_set,
				    state.d0_vec);
				break;
			default:
				break;
			}

			state.csr1_view =
			    (dir1 == execution::Direction::kOut)
			        ? ctx.graph->GetGenericOutgoingGraphView(
			              input_label, d1_nbr_label, e1)
			        : ctx.graph->GetGenericIncomingGraphView(
			              input_label, d1_nbr_label, e1);
			auto edges1 = state.csr1_view.get_edges(vid);
			state.it1 = edges1.begin();
			state.end1 = edges1.end();
			state.in_edge1_loop = true;
			state.in_edge2_loop = false;
			state.bitmap_built = true;
		}

		while (state.it1 != state.end1) {
			if (!state.in_edge2_loop) {
				state.current_nbr1 = state.it1.get_vertex();
				state.csr2_view =
				    (dir2 == execution::Direction::kOut)
				        ? ctx.graph->GetGenericOutgoingGraphView(
				              d1_nbr_label, d2_nbr_label, e2)
				        : ctx.graph->GetGenericIncomingGraphView(
				              d1_nbr_label, d2_nbr_label, e2);
				auto edges2 = state.csr2_view.get_edges(state.current_nbr1);
				state.it2 = edges2.begin();
				state.end2 = edges2.end();
				state.in_edge2_loop = true;
			}

			while (state.it2 != state.end2) {
				vid_t nbr2 = state.it2.get_vertex();
				if (state.d0_set[nbr2]) {
					state.offsets.push_back(
					    static_cast<sel_t>(state.row_idx));
					state.nbr1_vids.push_back(state.current_nbr1);
					state.nbr1_labels.push_back(d1_nbr_label);
					state.nbr2_vids.push_back(nbr2);
					state.nbr2_labels.push_back(d2_nbr_label);
				}
				++state.it2;

				if (state.offsets.size() >= STANDARD_VECTOR_SIZE) {
					goto flush_output;
				}
			}

			state.in_edge2_loop = false;
			++state.it1;
		}

		for (auto u : state.d0_vec) {
			state.d0_set[u] = false;
		}
		state.d0_vec.clear();
		state.bitmap_built = false;
		state.in_edge1_loop = false;
		state.in_edge2_loop = false;
	}

	// All rows processed
	if (!state.offsets.empty()) {
		goto flush_output;
	}

	output = GraphDataChunk();
	output.SetCardinality(0);
	state.has_pending = false;
	return OperatorResultType::kNeedMoreInput;

flush_output:
	size_t count = state.offsets.size();
	output = GraphDataChunk();

	// Slice input columns (the input vertex column, reshuffled)
	for (size_t col = 0; col < src->ColumnCount(); col++) {
		int tag = src->GetTag(col);
		if (tag == alias1_ || tag == alias2_) continue;
		const auto& vec = src->GetVector(col);
		auto sliced = GraphVector::Slice(vec, state.offsets.data(), count);
		output.AddColumn(tag, std::move(sliced));
	}

	// Add alias1 (nbr1) vertex column
	auto nbr1_vec = VertexVector::Create();
	vid_t* out_vids1 = VertexVector::GetVids(nbr1_vec);
	label_t* out_labels1 = VertexVector::GetLabels(nbr1_vec);
	for (size_t i = 0; i < count; i++) {
		out_vids1[i] = state.nbr1_vids[i];
		out_labels1[i] = state.nbr1_labels[i];
	}
	output.AddColumn(alias1_, std::move(nbr1_vec));

	// Add alias2 (nbr2) vertex column
	auto nbr2_vec = VertexVector::Create();
	vid_t* out_vids2 = VertexVector::GetVids(nbr2_vec);
	label_t* out_labels2 = VertexVector::GetLabels(nbr2_vec);
	for (size_t i = 0; i < count; i++) {
		out_vids2[i] = state.nbr2_vids[i];
		out_labels2[i] = state.nbr2_labels[i];
	}
	output.AddColumn(alias2_, std::move(nbr2_vec));
	output.SetCardinality(count);

	state.offsets.clear();
	state.nbr1_vids.clear();
	state.nbr2_vids.clear();
	state.nbr1_labels.clear();
	state.nbr2_labels.clear();

	if (state.row_idx < input_size || state.bitmap_built) {
		return OperatorResultType::kHaveMoreOutput;
	}

	state.has_pending = false;
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
