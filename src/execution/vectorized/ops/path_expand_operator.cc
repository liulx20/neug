#include "neug/execution/vectorized/ops/path_expand_operator.h"

#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/csr/generic_view.h"

namespace neug::execution::vec {

struct FrontierEntry {
	vid_t vid;
	label_t label;
	size_t src_row;
};

struct OutputEntry {
	size_t src_row;
	vid_t vid;
	label_t label;
};

struct PathExpandState : public OperatorState {
	GraphDataChunk pending_input;
	bool has_pending = false;

	std::vector<FrontierEntry> frontier;
	int current_depth = 0;
	bool bfs_done = false;

	std::vector<OutputEntry> output_buffer;
	size_t output_pos = 0;
};

PathExpandOperator::PathExpandOperator(
    int start_tag, int output_alias,
    std::vector<execution::LabelTriplet> labels, execution::Direction dir,
    int hop_lower, int hop_upper)
    : start_tag_(start_tag),
      output_alias_(output_alias),
      labels_(std::move(labels)),
      dir_(dir),
      hop_lower_(hop_lower),
      hop_upper_(hop_upper) {}

std::unique_ptr<OperatorState> PathExpandOperator::GetOperatorState() const {
	return std::make_unique<PathExpandState>();
}

static void EmitOutput(const GraphDataChunk& src, GraphDataChunk& output,
                       int output_alias,
                       const std::vector<OutputEntry>& buffer,
                       size_t begin, size_t end) {
	size_t count = end - begin;
	output = GraphDataChunk();

	std::vector<sel_t> sel(count);
	for (size_t i = 0; i < count; i++) {
		sel[i] = static_cast<sel_t>(buffer[begin + i].src_row);
	}

	for (size_t col = 0; col < src.ColumnCount(); col++) {
		int tag = src.GetTag(col);
		if (tag == output_alias) continue;
		const auto& vec = src.GetVector(col);
		auto sliced = GraphVector::Slice(vec, sel.data(), count);
		output.AddColumn(tag, std::move(sliced));
	}

	auto nbr_vec = VertexVector::Create();
	vid_t* out_vids = VertexVector::GetVids(nbr_vec);
	label_t* out_labels = VertexVector::GetLabels(nbr_vec);
	for (size_t i = 0; i < count; i++) {
		out_vids[i] = buffer[begin + i].vid;
		out_labels[i] = buffer[begin + i].label;
	}
	output.AddColumn(output_alias, std::move(nbr_vec));
	output.SetCardinality(count);
}

OperatorResultType PathExpandOperator::Execute(GraphDataChunk& input,
                                               GraphDataChunk& output,
                                               OperatorState& ostate,
                                               const VecExecContext& ctx) {
	auto& state = static_cast<PathExpandState&>(ostate);

	if (!state.has_pending) {
		if (input.size() == 0) {
			output = std::move(input);
			return OperatorResultType::kNeedMoreInput;
		}
		input.Flatten();
		state.pending_input = std::move(input);
		state.has_pending = true;
		state.current_depth = 0;
		state.bfs_done = false;
		state.output_buffer.clear();
		state.output_pos = 0;
		state.frontier.clear();

		int v_col = state.pending_input.FindColumnByTag(start_tag_);
		if (v_col < 0) {
			output = GraphDataChunk();
			output.SetCardinality(0);
			state.has_pending = false;
			return OperatorResultType::kNeedMoreInput;
		}

		const auto& vertex_vec =
		    state.pending_input.GetVector(static_cast<size_t>(v_col));
		const vid_t* vids = VertexVector::GetVids(vertex_vec);
		bool const_label = VertexVector::IsConstantLabel(vertex_vec);
		label_t clabel = const_label
		    ? VertexVector::GetConstantLabel(vertex_vec) : 0;
		const label_t* labels_ptr = const_label
		    ? nullptr : VertexVector::GetLabels(vertex_vec);

		size_t n = state.pending_input.size();
		state.frontier.reserve(n);
		for (size_t i = 0; i < n; i++) {
			label_t lbl = const_label ? clabel : labels_ptr[i];
			state.frontier.push_back({vids[i], lbl, i});
		}
	}

	// Emit pending output first
	if (state.output_pos < state.output_buffer.size()) {
		size_t remain = state.output_buffer.size() - state.output_pos;
		size_t emit = std::min(remain, STANDARD_VECTOR_SIZE);
		EmitOutput(state.pending_input, output, output_alias_,
		           state.output_buffer, state.output_pos,
		           state.output_pos + emit);
		state.output_pos += emit;
		if (state.output_pos < state.output_buffer.size() || !state.bfs_done) {
			return OperatorResultType::kHaveMoreOutput;
		}
		state.has_pending = false;
		return OperatorResultType::kNeedMoreInput;
	}

	// BFS loop
	while (!state.bfs_done) {
		// Collect frontier vertices at depths within [hop_lower, hop_upper)
		if (state.current_depth >= hop_lower_) {
			for (auto& e : state.frontier) {
				state.output_buffer.push_back({e.src_row, e.vid, e.label});
			}
		}

		state.current_depth++;
		if (state.current_depth >= hop_upper_) {
			state.bfs_done = true;
			break;
		}

		// Expand frontier
		std::vector<FrontierEntry> next;
		for (auto& e : state.frontier) {
			for (auto& triplet : labels_) {
				if (dir_ == execution::Direction::kOut ||
				    dir_ == execution::Direction::kBoth) {
					if (triplet.src_label == e.label) {
						auto view = ctx.graph->GetGenericOutgoingGraphView(
						    e.label, triplet.dst_label,
						    triplet.edge_label);
						auto edges = view.get_edges(e.vid);
						for (auto it = edges.begin(); it != edges.end();
						     ++it) {
							next.push_back(
							    {it.get_vertex(), triplet.dst_label,
							     e.src_row});
						}
					}
				}
				if (dir_ == execution::Direction::kIn ||
				    dir_ == execution::Direction::kBoth) {
					if (triplet.dst_label == e.label) {
						auto view = ctx.graph->GetGenericIncomingGraphView(
						    e.label, triplet.src_label,
						    triplet.edge_label);
						auto edges = view.get_edges(e.vid);
						for (auto it = edges.begin(); it != edges.end();
						     ++it) {
							next.push_back(
							    {it.get_vertex(), triplet.src_label,
							     e.src_row});
						}
					}
				}
			}
		}

		state.frontier = std::move(next);
		if (state.frontier.empty()) {
			state.bfs_done = true;
			break;
		}

		// If accumulated output is large enough, emit a batch
		if (state.output_buffer.size() - state.output_pos >=
		    STANDARD_VECTOR_SIZE) {
			size_t emit = STANDARD_VECTOR_SIZE;
			EmitOutput(state.pending_input, output, output_alias_,
			           state.output_buffer, state.output_pos,
			           state.output_pos + emit);
			state.output_pos += emit;
			return OperatorResultType::kHaveMoreOutput;
		}
	}

	// Emit remaining output
	size_t remain = state.output_buffer.size() - state.output_pos;
	if (remain > 0) {
		size_t emit = std::min(remain, STANDARD_VECTOR_SIZE);
		EmitOutput(state.pending_input, output, output_alias_,
		           state.output_buffer, state.output_pos,
		           state.output_pos + emit);
		state.output_pos += emit;
		if (state.output_pos < state.output_buffer.size()) {
			return OperatorResultType::kHaveMoreOutput;
		}
	} else {
		output = GraphDataChunk();
		output.SetCardinality(0);
	}

	state.has_pending = false;
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
