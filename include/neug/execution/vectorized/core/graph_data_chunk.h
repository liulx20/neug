#pragma once
#include <string>
#include <utility>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/core/selection_vector.h"

namespace neug::execution::vec {

class GraphDataChunk {
 public:
	GraphDataChunk() : count_(0) {}
	~GraphDataChunk() = default;

	GraphDataChunk(GraphDataChunk&& other) noexcept = default;
	GraphDataChunk& operator=(GraphDataChunk&& other) noexcept = default;

	void Initialize(const std::vector<std::pair<int, DataType>>& columns,
	                size_t capacity = STANDARD_VECTOR_SIZE);

	void Reset();

	size_t size() const { return count_; }
	size_t ColumnCount() const { return vectors_.size(); }
	void SetCardinality(size_t count) { count_ = count; }

	GraphVector& GetVector(size_t col_idx) { return vectors_[col_idx]; }
	const GraphVector& GetVector(size_t col_idx) const {
		return vectors_[col_idx];
	}

	int GetTag(size_t col_idx) const { return tags_[col_idx]; }

	int FindColumnByTag(int tag) const;
	GraphVector& GetVectorByTag(int tag);
	const GraphVector& GetVectorByTag(int tag) const;

	const std::vector<int>& tags() const { return tags_; }
	std::vector<DataType> GetTypes() const;

	void Flatten();
	void Compact(const SelectionVector& sel, size_t sel_count);
	void Append(const GraphDataChunk& other, size_t count);
	void AddColumn(int tag, DataType type,
	               size_t capacity = STANDARD_VECTOR_SIZE);
	void AddColumn(int tag, GraphVector&& vec);

	std::string ToString() const;

 private:
	std::vector<GraphVector> vectors_;
	std::vector<int> tags_;
	size_t count_;
};

}  // namespace neug::execution::vec
