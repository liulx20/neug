#include "neug/execution/vectorized/ops/hash_join_probe_operator.h"

#include "neug/execution/vectorized/core/graph_vector.h"

namespace neug::execution::vec {

static bool NeedsFlatten(const GraphDataChunk& chunk) {
	for (size_t i = 0; i < chunk.ColumnCount(); i++) {
		if (chunk.GetVector(i).IsDictionary()) return true;
	}
	return false;
}

struct HashJoinProbeState : public OperatorState {
	bool has_pending = false;
	GraphDataChunk pending_input;
	std::vector<uint32_t> build_matches;
	std::vector<uint32_t> probe_matches;
	size_t match_offset = 0;
};

HashJoinProbeOperator::HashJoinProbeOperator(
    JoinHashTable* hash_table, TupleDataCollection* build_tuples,
    execution::JoinKind join_kind, std::vector<int> probe_key_tags,
    std::vector<int> build_output_col_indices)
    : hash_table_(hash_table),
      build_tuples_(build_tuples),
      join_kind_(join_kind),
      probe_key_tags_(std::move(probe_key_tags)),
      build_output_col_indices_(std::move(build_output_col_indices)) {}

std::unique_ptr<OperatorState>
HashJoinProbeOperator::GetOperatorState() const {
	return std::make_unique<HashJoinProbeState>();
}

OperatorResultType HashJoinProbeOperator::Execute(GraphDataChunk& input,
                                                  GraphDataChunk& output,
                                                  OperatorState& state,
                                          const VecExecContext&) {
	switch (join_kind_) {
	case execution::JoinKind::kInnerJoin:
		return ExecuteInner(input, output, state);
	case execution::JoinKind::kSemiJoin:
		return ExecuteSemi(input, output, state);
	case execution::JoinKind::kAntiJoin:
		return ExecuteAnti(input, output, state);
	case execution::JoinKind::kLeftOuterJoin:
		return ExecuteLeftOuter(input, output, state);
	default:
		return ExecuteInner(input, output, state);
	}
}

OperatorResultType HashJoinProbeOperator::ExecuteInner(
    GraphDataChunk& input, GraphDataChunk& output, OperatorState& ostate) {
	auto& state = static_cast<HashJoinProbeState&>(ostate);

	if (!state.has_pending) {
		if (input.size() == 0) {
			output = std::move(input);
			return OperatorResultType::kNeedMoreInput;
		}

		if (NeedsFlatten(input)) input.Flatten();

		state.build_matches.clear();
		state.probe_matches.clear();
		hash_table_->Probe(input, probe_key_tags_, input.size(),
		                   state.build_matches, state.probe_matches);

		if (state.build_matches.empty()) {
			output = GraphDataChunk();
			output.SetCardinality(0);
			return OperatorResultType::kNeedMoreInput;
		}

		state.pending_input = std::move(input);
		state.has_pending = true;
		state.match_offset = 0;
	}

	// Produce output in batches of STANDARD_VECTOR_SIZE
	size_t total = state.build_matches.size();
	size_t remaining = total - state.match_offset;
	size_t batch = std::min(remaining, static_cast<size_t>(STANDARD_VECTOR_SIZE));

	output = GraphDataChunk();

	// Probe side: Slice with probe_matches
	for (size_t col = 0; col < state.pending_input.ColumnCount(); col++) {
		int tag = state.pending_input.GetTag(col);
		const auto& vec = state.pending_input.GetVector(col);
		auto sliced = GraphVector::Slice(
		    vec, state.probe_matches.data() + state.match_offset, batch);
		output.AddColumn(tag, std::move(sliced));
	}

	// Build side: Gather from TupleDataCollection (行转列)
	if (!build_output_col_indices_.empty()) {
		GraphDataChunk build_chunk;
		build_tuples_->GatherColumns(
		    state.build_matches.data() + state.match_offset, batch,
		    build_output_col_indices_, build_chunk);

		for (size_t col = 0; col < build_chunk.ColumnCount(); col++) {
			int tag = build_chunk.GetTag(col);
			// Skip columns already in probe output
			if (output.FindColumnByTag(tag) < 0) {
				output.AddColumn(tag, std::move(build_chunk.GetVector(col)));
			}
		}
	}

	output.SetCardinality(batch);
	state.match_offset += batch;

	if (state.match_offset >= total) {
		state.has_pending = false;
		return OperatorResultType::kNeedMoreInput;
	}
	return OperatorResultType::kHaveMoreOutput;
}

OperatorResultType HashJoinProbeOperator::ExecuteSemi(
    GraphDataChunk& input, GraphDataChunk& output, OperatorState&) {
	size_t count = input.size();
	if (count == 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	if (NeedsFlatten(input)) input.Flatten();

	std::vector<bool> has_match;
	hash_table_->ProbeExists(input, probe_key_tags_, count, has_match);

	SelectionVector sel(count);
	size_t selected = 0;
	for (size_t i = 0; i < count; i++) {
		if (has_match[i]) {
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

OperatorResultType HashJoinProbeOperator::ExecuteAnti(
    GraphDataChunk& input, GraphDataChunk& output, OperatorState&) {
	size_t count = input.size();
	if (count == 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	if (NeedsFlatten(input)) input.Flatten();

	std::vector<bool> has_match;
	hash_table_->ProbeExists(input, probe_key_tags_, count, has_match);

	SelectionVector sel(count);
	size_t selected = 0;
	for (size_t i = 0; i < count; i++) {
		if (!has_match[i]) {
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

OperatorResultType HashJoinProbeOperator::ExecuteLeftOuter(
    GraphDataChunk& input, GraphDataChunk& output, OperatorState& ostate) {
	auto& state = static_cast<HashJoinProbeState&>(ostate);

	if (!state.has_pending) {
		if (input.size() == 0) {
			output = std::move(input);
			return OperatorResultType::kNeedMoreInput;
		}

		if (NeedsFlatten(input)) input.Flatten();
		size_t count = input.size();

		state.build_matches.clear();
		state.probe_matches.clear();
		hash_table_->Probe(input, probe_key_tags_, count,
		                   state.build_matches, state.probe_matches);

		// For LEFT OUTER: include unmatched probe rows with NULL build cols
		std::vector<bool> matched(count, false);
		for (uint32_t pm : state.probe_matches) {
			matched[pm] = true;
		}

		// Append unmatched rows with a sentinel build index
		for (size_t i = 0; i < count; i++) {
			if (!matched[i]) {
				state.probe_matches.push_back(static_cast<uint32_t>(i));
				state.build_matches.push_back(UINT32_MAX);  // NULL marker
			}
		}

		if (state.probe_matches.empty()) {
			output = GraphDataChunk();
			output.SetCardinality(0);
			return OperatorResultType::kNeedMoreInput;
		}

		state.pending_input = std::move(input);
		state.has_pending = true;
		state.match_offset = 0;
	}

	size_t total = state.probe_matches.size();
	size_t remaining = total - state.match_offset;
	size_t batch = std::min(remaining, static_cast<size_t>(STANDARD_VECTOR_SIZE));

	output = GraphDataChunk();

	// Probe side: Slice
	for (size_t col = 0; col < state.pending_input.ColumnCount(); col++) {
		int tag = state.pending_input.GetTag(col);
		const auto& vec = state.pending_input.GetVector(col);
		auto sliced = GraphVector::Slice(
		    vec, state.probe_matches.data() + state.match_offset, batch);
		output.AddColumn(tag, std::move(sliced));
	}

	// Build side: Gather matched rows, set NULL for unmatched
	if (!build_output_col_indices_.empty()) {
		// Separate matched and unmatched
		std::vector<uint32_t> valid_build_indices;
		std::vector<size_t> valid_positions;
		std::vector<size_t> null_positions;

		for (size_t i = 0; i < batch; i++) {
			uint32_t build_idx =
			    state.build_matches[state.match_offset + i];
			if (build_idx != UINT32_MAX) {
				valid_build_indices.push_back(build_idx);
				valid_positions.push_back(i);
			} else {
				null_positions.push_back(i);
			}
		}

		// Gather valid rows
		GraphDataChunk build_chunk;
		if (!valid_build_indices.empty()) {
			build_tuples_->GatherColumns(
			    valid_build_indices.data(), valid_build_indices.size(),
			    build_output_col_indices_, build_chunk);
		}

		// Create output vectors with nulls
		const auto& layout = build_tuples_->layout();
		for (int col_idx : build_output_col_indices_) {
			const auto& col_info =
			    layout.columns[static_cast<size_t>(col_idx)];

			GraphVector vec{col_info.type};

			if (!valid_build_indices.empty()) {
				int gather_col =
				    build_chunk.FindColumnByTag(col_info.tag);
				if (gather_col >= 0) {
					auto& src = build_chunk.GetVector(
					    static_cast<size_t>(gather_col));
					// Copy valid data to correct positions
					size_t type_size = GetTypeSize(col_info.type.id());
					if (type_size > 0 &&
					    col_info.type.id() != DataTypeId::kVertex &&
					    col_info.type.id() != DataTypeId::kVarchar) {
						auto* dst = vec.buffer().GetData();
						auto* src_data = src.buffer().GetData();
						for (size_t i = 0; i < valid_positions.size();
						     i++) {
							std::memcpy(dst + valid_positions[i] * type_size,
							            src_data + i * type_size,
							            type_size);
						}
					} else if (col_info.type.id() == DataTypeId::kVertex) {
						vid_t* d_vids = VertexVector::GetVids(vec);
						label_t* d_labels = VertexVector::GetLabels(vec);
						const vid_t* s_vids = VertexVector::GetVids(src);
						const label_t* s_labels =
						    VertexVector::GetLabels(src);
						for (size_t i = 0; i < valid_positions.size();
						     i++) {
							d_vids[valid_positions[i]] = s_vids[i];
							d_labels[valid_positions[i]] = s_labels[i];
						}
					}
				}
			}

			// Set nulls for unmatched positions
			for (size_t pos : null_positions) {
				vec.validity().SetInvalid(pos);
			}

			if (output.FindColumnByTag(col_info.tag) < 0) {
				output.AddColumn(col_info.tag, std::move(vec));
			}
		}
	}

	output.SetCardinality(batch);
	state.match_offset += batch;

	if (state.match_offset >= total) {
		state.has_pending = false;
		return OperatorResultType::kNeedMoreInput;
	}
	return OperatorResultType::kHaveMoreOutput;
}

}  // namespace neug::execution::vec
