#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/join/row_layout.h"

namespace neug::execution::vec {

class TupleDataCollection {
 public:
	void Initialize(const RowLayout& layout);

	// Scatter: 列转行
	void Append(const GraphDataChunk& chunk, size_t count);

	// Gather: 行转列
	void Gather(const uint32_t* row_indices, size_t count,
	            GraphDataChunk& output) const;

	// Gather specific columns only
	void GatherColumns(const uint32_t* row_indices, size_t count,
	                   const std::vector<int>& col_indices,
	                   GraphDataChunk& output) const;

	size_t Count() const { return count_; }
	const uint8_t* GetRow(size_t idx) const;
	uint8_t* GetRowMut(size_t idx);

	const RowLayout& layout() const { return layout_; }

 private:
	uint8_t* AllocateRow();

	RowLayout layout_;
	static constexpr size_t BLOCK_SIZE = 256 * 1024;  // 256KB blocks
	std::vector<std::unique_ptr<uint8_t[]>> blocks_;
	size_t rows_per_block_ = 0;
	size_t count_ = 0;

	// String heap for variable-length data
	std::vector<std::unique_ptr<std::string>> string_heap_;

	// Path heap for Path objects
	std::vector<std::shared_ptr<execution::Path>> path_heap_;
};

}  // namespace neug::execution::vec
