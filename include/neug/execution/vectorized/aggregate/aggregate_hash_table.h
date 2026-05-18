#pragma once
#include <cstdint>
#include <memory>
#include <vector>

#include "neug/execution/vectorized/aggregate/aggregate_state.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/join/row_layout.h"
#include "neug/execution/vectorized/join/tuple_data.h"

namespace neug::execution::vec {

class AggregateHashTable {
 public:
	void Initialize(const RowLayout& key_layout,
	                const std::vector<int>& key_tags,
	                const std::vector<AggFuncDef>& agg_funcs);

	void AddChunk(GraphDataChunk& chunk, size_t count);

	size_t NumGroups() const { return num_groups_; }

	void Scan(size_t offset, size_t count, GraphDataChunk& output);

 private:
	uint32_t FindOrCreateGroup(const GraphDataChunk& chunk, size_t row);
	void UpdateAggStates(uint32_t group_idx, const GraphDataChunk& chunk,
	                     size_t row);

	static constexpr uint32_t EMPTY = UINT32_MAX;

	std::vector<uint32_t> directory_;
	size_t directory_mask_ = 0;

	struct GroupEntry {
		uint32_t key_row_idx;
		uint32_t next;
	};
	std::vector<GroupEntry> entries_;

	TupleDataCollection key_data_;
	RowLayout key_layout_;
	std::vector<int> key_tags_;

	// agg_states_[func_idx][group_idx]
	std::vector<std::vector<std::unique_ptr<AggStateBase>>> agg_states_;
	std::vector<AggFuncDef> agg_funcs_;

	size_t num_groups_ = 0;
	bool has_keys_ = true;
};

}  // namespace neug::execution::vec
