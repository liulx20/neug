#pragma once
#include <cstdint>
#include <vector>

#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/join/row_layout.h"
#include "neug/execution/vectorized/join/tuple_data.h"

namespace neug::execution::vec {

class JoinHashTable {
 public:
	void Initialize(const RowLayout& layout,
	                const std::vector<int>& key_col_indices);

	void Build(const TupleDataCollection& data);

	void Probe(const GraphDataChunk& probe_chunk,
	           const std::vector<int>& probe_key_tags,
	           size_t count,
	           std::vector<uint32_t>& build_matches,
	           std::vector<uint32_t>& probe_matches) const;

	// For SEMI/ANTI: returns which probe rows have at least one match
	void ProbeExists(const GraphDataChunk& probe_chunk,
	                 const std::vector<int>& probe_key_tags,
	                 size_t count,
	                 std::vector<bool>& has_match) const;

 private:
	static uint64_t HashRow(const uint8_t* row, const RowLayout& layout,
	                        const std::vector<int>& key_cols);

	static uint64_t HashProbeKeys(const GraphDataChunk& chunk, size_t row,
	                              const std::vector<int>& key_tags,
	                              const RowLayout& layout,
	                              const std::vector<int>& key_cols);

	static bool CompareKeys(const uint8_t* build_row,
	                        const GraphDataChunk& probe_chunk, size_t probe_row,
	                        const RowLayout& layout,
	                        const std::vector<int>& key_cols,
	                        const std::vector<int>& probe_key_tags);

	static constexpr uint32_t EMPTY = UINT32_MAX;

	std::vector<uint32_t> directory_;
	struct Entry {
		uint32_t row_idx;
		uint32_t next;
	};
	std::vector<Entry> entries_;

	RowLayout layout_;
	std::vector<int> key_col_indices_;
	const TupleDataCollection* data_ = nullptr;
	size_t directory_mask_ = 0;
};

}  // namespace neug::execution::vec
