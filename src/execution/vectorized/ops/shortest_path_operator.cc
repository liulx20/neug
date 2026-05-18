#include "neug/execution/vectorized/ops/shortest_path_operator.h"

#include <queue>

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/utils/property/property.h"

namespace neug::execution::vec {

struct ShortestPathState : public OperatorState {
	GraphDataChunk pending_input;
	bool has_pending = false;
	size_t row_idx = 0;

	bool dest_resolved = false;
	vid_t dest_vid = 0;
	bool dest_valid = false;

	std::vector<sel_t> offsets;
	std::vector<vid_t> dest_vids;
	std::vector<int64_t> path_lengths;
};

ShortestPathOperator::ShortestPathOperator(
    int start_tag, int v_alias, int path_alias, execution::LabelTriplet label,
    int hop_lower, int hop_upper, std::string dest_param_name,
    int64_t dest_const, bool use_param)
    : start_tag_(start_tag),
      v_alias_(v_alias),
      path_alias_(path_alias),
      label_(label),
      hop_lower_(hop_lower),
      hop_upper_(hop_upper),
      dest_param_name_(std::move(dest_param_name)),
      dest_const_(dest_const),
      use_param_(use_param) {}

std::unique_ptr<OperatorState> ShortestPathOperator::GetOperatorState() const {
	return std::make_unique<ShortestPathState>();
}

static int bidirectional_bfs(const StorageReadInterface& graph,
                             const execution::LabelTriplet& label,
                             int hop_upper, vid_t src, vid_t dst) {
	if (src == dst) return 0;

	label_t v_label = label.src_label;
	label_t e_label = label.edge_label;
	auto vertices = graph.GetVertexSet(v_label);
	StorageReadInterface::vertex_array_t<int> dis(vertices, 0);
	StorageReadInterface::vertex_array_t<int> pre(vertices, -1);

	std::queue<vid_t> q1, q2, tmp;
	q1.push(src);
	dis[src] = 1;
	q2.push(dst);
	dis[dst] = -1;

	auto oview = graph.GetGenericOutgoingGraphView(v_label, v_label, e_label);
	auto iview = graph.GetGenericIncomingGraphView(v_label, v_label, e_label);

	auto expand_and_check = [&](vid_t x, int d_x, bool forward) -> int {
		auto expand_view = [&](const GenericView& view) -> int {
			auto es = view.get_edges(x);
			for (auto it = es.begin(); it != es.end(); ++it) {
				vid_t y = it.get_vertex();
				if (dis[y] == 0) {
					dis[y] = forward ? d_x + 1 : d_x - 1;
					tmp.push(y);
					pre[y] = x;
				} else if ((forward && dis[y] < 0) ||
				           (!forward && dis[y] > 0)) {
					int len;
					if (forward) {
						len = d_x + (-dis[y]) - 1;
					} else {
						len = dis[y] + (-d_x) - 1;
					}
					return len;
				}
			}
			return -1;
		};
		int r = expand_view(oview);
		if (r >= 0) return r;
		return expand_view(iview);
	};

	while (true) {
		if (q1.size() <= q2.size()) {
			if (q1.empty()) break;
			while (!q1.empty()) {
				vid_t x = q1.front();
				if (dis[x] >= hop_upper + 1) return -1;
				q1.pop();
				int r = expand_and_check(x, dis[x], true);
				if (r >= 0) return r;
			}
			std::swap(q1, tmp);
		} else {
			if (q2.empty()) break;
			while (!q2.empty()) {
				vid_t x = q2.front();
				if (dis[x] <= -hop_upper - 1) return -1;
				q2.pop();
				int r = expand_and_check(x, dis[x], false);
				if (r >= 0) return r;
			}
			std::swap(q2, tmp);
		}
	}
	return -1;
}

OperatorResultType ShortestPathOperator::Execute(GraphDataChunk& input,
                                                 GraphDataChunk& output,
                                                 OperatorState& ostate,
                                                 const VecExecContext& ctx) {
	auto& state = static_cast<ShortestPathState&>(ostate);

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
		src = &state.pending_input;
	}

	if (!state.dest_resolved) {
		state.dest_resolved = true;
		int64_t pk_val;
		if (use_param_) {
			assert(ctx.params != nullptr);
			pk_val = ctx.params->at(dest_param_name_).GetValue<int64_t>();
		} else {
			pk_val = dest_const_;
		}
		Property oid = Property::from_int64(pk_val);
		state.dest_valid =
		    ctx.graph->GetVertexIndex(label_.dst_label, oid, state.dest_vid);
	}

	if (!state.dest_valid) {
		output = GraphDataChunk();
		output.SetCardinality(0);
		state.has_pending = false;
		return OperatorResultType::kNeedMoreInput;
	}

	state.offsets.clear();
	state.dest_vids.clear();
	state.path_lengths.clear();

	int v_col_idx = src->FindColumnByTag(start_tag_);
	assert(v_col_idx >= 0);
	const auto& vertex_vec = src->GetVector(static_cast<size_t>(v_col_idx));
	const vid_t* vids = VertexVector::GetVids(vertex_vec);
	const bool has_sel = vertex_vec.IsDictionary();
	const SelectionVector* v_sel = has_sel ? &vertex_vec.sel() : nullptr;
	size_t input_size = src->size();

	for (; state.row_idx < input_size; state.row_idx++) {
		size_t phys_idx =
		    has_sel ? v_sel->GetIndex(state.row_idx) : state.row_idx;
		vid_t vid = vids[phys_idx];

		int len =
		    bidirectional_bfs(*ctx.graph, label_, hop_upper_, vid, state.dest_vid);
		if (len >= 0 && len >= hop_lower_ && len < hop_upper_) {
			state.offsets.push_back(static_cast<sel_t>(state.row_idx));
			state.dest_vids.push_back(state.dest_vid);
			state.path_lengths.push_back(static_cast<int64_t>(len));
		}

		if (state.offsets.size() >= STANDARD_VECTOR_SIZE) {
			state.row_idx++;
			goto flush_output;
		}
	}

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

	for (size_t col = 0; col < src->ColumnCount(); col++) {
		int tag = src->GetTag(col);
		if (tag == v_alias_ || tag == path_alias_) continue;
		const auto& vec = src->GetVector(col);
		auto sliced = GraphVector::Slice(vec, state.offsets.data(), count);
		output.AddColumn(tag, std::move(sliced));
	}

	auto dest_vec = VertexVector::Create();
	vid_t* out_vids = VertexVector::GetVids(dest_vec);
	label_t* out_labels = VertexVector::GetLabels(dest_vec);
	for (size_t i = 0; i < count; i++) {
		out_vids[i] = state.dest_vids[i];
		out_labels[i] = label_.dst_label;
	}
	output.AddColumn(v_alias_, std::move(dest_vec));

	auto len_vec = GraphVector(DataType{DataTypeId::kInt64});
	int64_t* len_data = len_vec.GetData<int64_t>();
	for (size_t i = 0; i < count; i++) {
		len_data[i] = state.path_lengths[i];
	}
	output.AddColumn(path_alias_, std::move(len_vec));
	output.SetCardinality(count);

	state.offsets.clear();
	state.dest_vids.clear();
	state.path_lengths.clear();

	if (state.row_idx < input_size) {
		return OperatorResultType::kHaveMoreOutput;
	}

	state.has_pending = false;
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
