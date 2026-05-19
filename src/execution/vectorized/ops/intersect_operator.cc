#include "neug/execution/vectorized/ops/intersect_operator.h"

#include <unordered_set>

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/csr/generic_view.h"

namespace neug::execution::vec {

struct IntersectState : public OperatorState {
	bool has_pending = false;
	GraphDataChunk pending_input;
	size_t row_idx = 0;

	std::vector<std::pair<label_t, vid_t>> result_vertices;
	size_t emit_idx = 0;

	std::unordered_set<uint64_t> vertex_set;
	std::unordered_set<uint64_t> scratch_set;
	std::vector<sel_t> offsets;
	std::vector<vid_t> nbr_vids;
	std::vector<label_t> nbr_labels;
	size_t last_set_size = 64;
};

IntersectOperator::IntersectOperator(std::vector<IntersectSubPlan> sub_plans,
                                     int key_alias)
    : sub_plans_(std::move(sub_plans)),
      key_alias_(key_alias) {}

std::unique_ptr<OperatorState> IntersectOperator::GetOperatorState() const {
	return std::make_unique<IntersectState>();
}

static uint64_t EncodeVertex(label_t label, vid_t vid) {
	return (static_cast<uint64_t>(label) << 32) | static_cast<uint64_t>(vid);
}

static void ExpandNeighbors(const StorageReadInterface& graph,
                            const IntersectSubPlan& plan,
                            label_t v_label, vid_t vid,
                            std::unordered_set<uint64_t>& out_set) {
	for (const auto& triplet : plan.labels) {
		if (plan.dir == execution::Direction::kOut ||
		    plan.dir == execution::Direction::kBoth) {
			if (triplet.src_label == v_label) {
				auto view = graph.GetGenericOutgoingGraphView(
				    v_label, triplet.dst_label, triplet.edge_label);
				auto edges = view.get_edges(vid);
				for (auto it = edges.begin(); it != edges.end(); ++it) {
					out_set.insert(EncodeVertex(triplet.dst_label, it.get_vertex()));
				}
			}
		}
		if (plan.dir == execution::Direction::kIn ||
		    plan.dir == execution::Direction::kBoth) {
			if (triplet.dst_label == v_label) {
				auto view = graph.GetGenericIncomingGraphView(
				    v_label, triplet.src_label, triplet.edge_label);
				auto edges = view.get_edges(vid);
				for (auto it = edges.begin(); it != edges.end(); ++it) {
					out_set.insert(EncodeVertex(triplet.src_label, it.get_vertex()));
				}
			}
		}
	}
}

static void IntersectNeighbors(const StorageReadInterface& graph,
                               const IntersectSubPlan& plan,
                               label_t v_label, vid_t vid,
                               std::unordered_set<uint64_t>& current_set,
                               std::unordered_set<uint64_t>& scratch) {
	scratch.clear();
	for (const auto& triplet : plan.labels) {
		if (plan.dir == execution::Direction::kOut ||
		    plan.dir == execution::Direction::kBoth) {
			if (triplet.src_label == v_label) {
				auto view = graph.GetGenericOutgoingGraphView(
				    v_label, triplet.dst_label, triplet.edge_label);
				auto edges = view.get_edges(vid);
				for (auto it = edges.begin(); it != edges.end(); ++it) {
					uint64_t enc = EncodeVertex(triplet.dst_label, it.get_vertex());
					if (current_set.count(enc)) {
						scratch.insert(enc);
					}
				}
			}
		}
		if (plan.dir == execution::Direction::kIn ||
		    plan.dir == execution::Direction::kBoth) {
			if (triplet.dst_label == v_label) {
				auto view = graph.GetGenericIncomingGraphView(
				    v_label, triplet.src_label, triplet.edge_label);
				auto edges = view.get_edges(vid);
				for (auto it = edges.begin(); it != edges.end(); ++it) {
					uint64_t enc = EncodeVertex(triplet.src_label, it.get_vertex());
					if (current_set.count(enc)) {
						scratch.insert(enc);
					}
				}
			}
		}
	}
	current_set.swap(scratch);
}

static void BuildIntersectOutput(const GraphDataChunk& src,
                                 GraphDataChunk& output, int key_alias,
                                 const std::vector<sel_t>& offsets,
                                 const std::vector<vid_t>& nbr_vids,
                                 const std::vector<label_t>& nbr_labels) {
	size_t count = offsets.size();
	output = GraphDataChunk();
	for (size_t col = 0; col < src.ColumnCount(); col++) {
		int tag = src.GetTag(col);
		if (tag == key_alias) continue;
		const auto& vec = src.GetVector(col);
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
	output.AddColumn(key_alias, std::move(nbr_vec));
	output.SetCardinality(count);
}

OperatorResultType IntersectOperator::Execute(GraphDataChunk& input,
                                              GraphDataChunk& output,
                                              OperatorState& ostate,
                                          const VecExecContext& ctx) {
	auto& state = static_cast<IntersectState&>(ostate);

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
		state.result_vertices.clear();
		state.emit_idx = 0;
		src = &state.pending_input;
	}

	state.offsets.clear();
	state.nbr_vids.clear();
	state.nbr_labels.clear();

	size_t input_size = src->size();

	for (; state.row_idx < input_size; state.row_idx++) {
		// If we have leftover results from previous row, emit them
		while (state.emit_idx < state.result_vertices.size()) {
			state.offsets.push_back(static_cast<sel_t>(state.row_idx));
			state.nbr_labels.push_back(state.result_vertices[state.emit_idx].first);
			state.nbr_vids.push_back(state.result_vertices[state.emit_idx].second);
			state.emit_idx++;

			if (state.offsets.size() >= STANDARD_VECTOR_SIZE) {
				BuildIntersectOutput(*src, output, key_alias_,
				                     state.offsets, state.nbr_vids, state.nbr_labels);
				state.offsets.clear();
				state.nbr_vids.clear();
				state.nbr_labels.clear();
				return OperatorResultType::kHaveMoreOutput;
			}
		}

		// Already computed and fully emitted for this row — advance
		if (!state.result_vertices.empty()) {
			state.result_vertices.clear();
			state.emit_idx = 0;
			continue;
		}

		// Compute intersection for current row
		state.result_vertices.clear();
		state.emit_idx = 0;

		// Build initial set from sub_plan[0]
		state.vertex_set.clear();
		state.vertex_set.reserve(state.last_set_size);
		{
			const auto& sp = sub_plans_[0];
			int v_col_idx = src->FindColumnByTag(sp.v_tag);
			if (v_col_idx < 0) continue;
			const auto& vertex_vec = src->GetVector(
			    static_cast<size_t>(v_col_idx));
			const vid_t* vids = VertexVector::GetVids(vertex_vec);
			const bool has_sel = vertex_vec.IsDictionary();
			const SelectionVector* v_sel =
			    has_sel ? &vertex_vec.sel() : nullptr;
			size_t phys_idx = has_sel
			    ? v_sel->GetIndex(state.row_idx) : state.row_idx;
			vid_t vid = vids[phys_idx];
			label_t v_label;
			if (VertexVector::IsConstantLabel(vertex_vec)) {
				v_label = VertexVector::GetConstantLabel(vertex_vec);
			} else {
				v_label = VertexVector::GetLabels(vertex_vec)[phys_idx];
			}
			ExpandNeighbors(*ctx.graph, sp, v_label, vid, state.vertex_set);
		}

		if (state.vertex_set.empty()) continue;

		// Intersect with subsequent sub_plans
		for (size_t sp_idx = 1; sp_idx < sub_plans_.size(); sp_idx++) {
			const auto& sp = sub_plans_[sp_idx];
			int v_col_idx = src->FindColumnByTag(sp.v_tag);
			if (v_col_idx < 0) {
				state.vertex_set.clear();
				break;
			}
			const auto& vertex_vec = src->GetVector(
			    static_cast<size_t>(v_col_idx));
			const vid_t* vids = VertexVector::GetVids(vertex_vec);
			const bool has_sel = vertex_vec.IsDictionary();
			const SelectionVector* v_sel =
			    has_sel ? &vertex_vec.sel() : nullptr;
			size_t phys_idx = has_sel
			    ? v_sel->GetIndex(state.row_idx) : state.row_idx;
			vid_t vid = vids[phys_idx];
			label_t v_label;
			if (VertexVector::IsConstantLabel(vertex_vec)) {
				v_label = VertexVector::GetConstantLabel(vertex_vec);
			} else {
				v_label = VertexVector::GetLabels(vertex_vec)[phys_idx];
			}
			IntersectNeighbors(*ctx.graph, sp, v_label, vid,
			                   state.vertex_set, state.scratch_set);
			if (state.vertex_set.empty()) break;
		}

		if (state.vertex_set.empty()) continue;

		state.last_set_size = std::max(state.last_set_size,
		                               state.vertex_set.size());

		// Decode results
		for (uint64_t enc : state.vertex_set) {
			label_t lbl = static_cast<label_t>(enc >> 32);
			vid_t vid = static_cast<vid_t>(enc & 0xFFFFFFFF);
			state.result_vertices.emplace_back(lbl, vid);
		}
		state.emit_idx = 0;

		// Emit as many as we can
		while (state.emit_idx < state.result_vertices.size()) {
			state.offsets.push_back(static_cast<sel_t>(state.row_idx));
			state.nbr_labels.push_back(state.result_vertices[state.emit_idx].first);
			state.nbr_vids.push_back(state.result_vertices[state.emit_idx].second);
			state.emit_idx++;

			if (state.offsets.size() >= STANDARD_VECTOR_SIZE) {
				BuildIntersectOutput(*src, output, key_alias_,
				                     state.offsets, state.nbr_vids, state.nbr_labels);
				state.offsets.clear();
				state.nbr_vids.clear();
				state.nbr_labels.clear();
				return OperatorResultType::kHaveMoreOutput;
			}
		}

		// All results for this row emitted normally — clear so next
		// iteration's check knows this row is done
		state.result_vertices.clear();
		state.emit_idx = 0;
	}

	// Emit remaining
	if (!state.offsets.empty()) {
		BuildIntersectOutput(*src, output, key_alias_,
		                     state.offsets, state.nbr_vids, state.nbr_labels);
	} else {
		output = GraphDataChunk();
		output.SetCardinality(0);
	}

	state.has_pending = false;
	state.result_vertices.clear();
	state.emit_idx = 0;
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
