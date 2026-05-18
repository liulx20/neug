#include "neug/execution/vectorized/ops/all_shortest_path_operator.h"

#include <queue>

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/utils/property/property.h"

namespace neug::execution::vec {

struct AllShortestPathState : public OperatorState {
	GraphDataChunk pending_input;
	bool has_pending = false;
	size_t row_idx = 0;

	bool dest_resolved = false;
	vid_t dest_vid = 0;
	bool dest_valid = false;

	std::vector<sel_t> offsets;
	std::vector<vid_t> dest_vids;
	std::vector<execution::Path> paths;
};

AllShortestPathOperator::AllShortestPathOperator(
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

std::unique_ptr<OperatorState> AllShortestPathOperator::GetOperatorState()
    const {
	return std::make_unique<AllShortestPathState>();
}

static void dfs_enumerate(
    const GenericView& oview, const GenericView& iview,
    vid_t cur, vid_t dst,
    const StorageReadInterface::vertex_array_t<bool>& on_path,
    const StorageReadInterface::vertex_array_t<int8_t>& dist,
    std::vector<vid_t>& cur_path,
    std::vector<std::vector<vid_t>>& all_paths,
    std::vector<std::vector<std::pair<execution::Direction, const void*>>>& all_edge_datas,
    std::vector<std::pair<execution::Direction, const void*>>& cur_edge_data) {
	cur_path.push_back(cur);
	if (cur == dst) {
		all_paths.push_back(cur_path);
		all_edge_datas.push_back(cur_edge_data);
		cur_path.pop_back();
		return;
	}
	{
		auto es = oview.get_edges(cur);
		for (auto it = es.begin(); it != es.end(); ++it) {
			vid_t nbr = it.get_vertex();
			if (on_path[nbr] && dist[nbr] == dist[cur] + 1) {
				cur_edge_data.emplace_back(execution::Direction::kOut,
				                           it.get_data_ptr());
				dfs_enumerate(oview, iview, nbr, dst, on_path, dist,
				              cur_path, all_paths, all_edge_datas,
				              cur_edge_data);
				cur_edge_data.pop_back();
			}
		}
	}
	{
		auto es = iview.get_edges(cur);
		for (auto it = es.begin(); it != es.end(); ++it) {
			vid_t nbr = it.get_vertex();
			if (on_path[nbr] && dist[nbr] == dist[cur] + 1) {
				cur_edge_data.emplace_back(execution::Direction::kIn,
				                           it.get_data_ptr());
				dfs_enumerate(oview, iview, nbr, dst, on_path, dist,
				              cur_path, all_paths, all_edge_datas,
				              cur_edge_data);
				cur_edge_data.pop_back();
			}
		}
	}
	cur_path.pop_back();
}

static void find_all_shortest_paths(
    const StorageReadInterface& graph,
    const execution::LabelTriplet& label,
    int hop_upper, vid_t src, vid_t dst,
    std::vector<std::vector<vid_t>>& paths,
    std::vector<std::vector<std::pair<execution::Direction, const void*>>>& edge_datas) {
	if (src == dst) {
		paths.push_back({src});
		edge_datas.push_back({});
		return;
	}

	label_t v_label = label.src_label;
	label_t e_label = label.edge_label;
	auto vertices = graph.GetVertexSet(v_label);
	StorageReadInterface::vertex_array_t<int8_t> dist_from_src(vertices, -1);
	StorageReadInterface::vertex_array_t<int8_t> dist_from_dst(vertices, -1);

	dist_from_src[src] = 0;
	dist_from_dst[dst] = 0;

	std::queue<vid_t> q1, q2, tmp;
	q1.push(src);
	q2.push(dst);

	auto oview = graph.GetGenericOutgoingGraphView(v_label, v_label, e_label);
	auto iview = graph.GetGenericIncomingGraphView(v_label, v_label, e_label);

	std::vector<vid_t> meeting_vertices;
	int8_t src_dep = 0, dst_dep = 0;

	while (true) {
		if (src_dep >= hop_upper || dst_dep >= hop_upper ||
		    !meeting_vertices.empty()) {
			break;
		}
		if (q1.size() <= q2.size()) {
			if (q1.empty()) break;
			while (!q1.empty()) {
				vid_t v = q1.front();
				q1.pop();
				auto expand = [&](const GenericView& view) {
					auto es = view.get_edges(v);
					for (auto it = es.begin(); it != es.end(); ++it) {
						vid_t nbr = it.get_vertex();
						if (dist_from_src[nbr] == -1) {
							dist_from_src[nbr] = src_dep + 1;
							tmp.push(nbr);
							if (dist_from_dst[nbr] != -1) {
								meeting_vertices.push_back(nbr);
							}
						}
					}
				};
				expand(oview);
				expand(iview);
			}
			std::swap(q1, tmp);
			++src_dep;
		} else {
			if (q2.empty()) break;
			while (!q2.empty()) {
				vid_t v = q2.front();
				q2.pop();
				auto expand = [&](const GenericView& view) {
					auto es = view.get_edges(v);
					for (auto it = es.begin(); it != es.end(); ++it) {
						vid_t nbr = it.get_vertex();
						if (dist_from_dst[nbr] == -1) {
							dist_from_dst[nbr] = dst_dep + 1;
							tmp.push(nbr);
							if (dist_from_src[nbr] != -1) {
								meeting_vertices.push_back(nbr);
							}
						}
					}
				};
				expand(oview);
				expand(iview);
			}
			std::swap(q2, tmp);
			++dst_dep;
		}
	}

	if (meeting_vertices.empty()) return;
	if (src_dep + dst_dep >= hop_upper) return;

	// Back-propagate: mark all vertices on shortest paths
	StorageReadInterface::vertex_array_t<bool> on_path(vertices, false);
	std::queue<vid_t> bq;
	for (auto v : meeting_vertices) {
		bq.push(v);
		on_path[v] = true;
	}
	while (!bq.empty()) {
		vid_t v = bq.front();
		bq.pop();
		auto check_neighbor = [&](const GenericView& view) {
			auto es = view.get_edges(v);
			for (auto it = es.begin(); it != es.end(); ++it) {
				vid_t nbr = it.get_vertex();
				if (on_path[nbr]) continue;
				if (dist_from_src[nbr] != -1 &&
				    dist_from_src[nbr] + 1 == dist_from_src[v]) {
					bq.push(nbr);
					on_path[nbr] = true;
				}
				if (dist_from_dst[nbr] != -1 &&
				    dist_from_dst[nbr] + 1 == dist_from_dst[v]) {
					bq.push(nbr);
					on_path[nbr] = true;
					dist_from_src[nbr] = dist_from_src[v] + 1;
				}
			}
		};
		check_neighbor(oview);
		check_neighbor(iview);
	}

	on_path[src] = true;
	on_path[dst] = true;

	std::vector<vid_t> cur_path;
	std::vector<std::pair<execution::Direction, const void*>> cur_edge_data;
	dfs_enumerate(oview, iview, src, dst, on_path, dist_from_src,
	              cur_path, paths, edge_datas, cur_edge_data);
}

OperatorResultType AllShortestPathOperator::Execute(
    GraphDataChunk& input, GraphDataChunk& output, OperatorState& ostate,
    const VecExecContext& ctx) {
	auto& state = static_cast<AllShortestPathState&>(ostate);

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
	state.paths.clear();

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

		std::vector<std::vector<vid_t>> found_paths;
		std::vector<std::vector<std::pair<execution::Direction, const void*>>>
		    found_edge_datas;
		find_all_shortest_paths(*ctx.graph, label_, hop_upper_, vid,
		                        state.dest_vid, found_paths, found_edge_datas);

		for (size_t pi = 0; pi < found_paths.size(); pi++) {
			auto& path_vids = found_paths[pi];
			int32_t path_len = static_cast<int32_t>(path_vids.size()) - 1;
			if (path_len < hop_lower_) continue;

			state.offsets.push_back(static_cast<sel_t>(state.row_idx));
			state.dest_vids.push_back(state.dest_vid);
			state.paths.push_back(execution::Path(
			    label_.src_label, label_.edge_label, path_vids,
			    found_edge_datas[pi]));
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

	auto path_vec = PathVector::Create();
	for (size_t i = 0; i < count; i++) {
		PathVector::SetPath(path_vec, i, std::move(state.paths[i]));
	}
	output.AddColumn(path_alias_, std::move(path_vec));
	output.SetCardinality(count);

	state.offsets.clear();
	state.dest_vids.clear();
	state.paths.clear();

	if (state.row_idx < input_size) {
		return OperatorResultType::kHaveMoreOutput;
	}

	state.has_pending = false;
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
