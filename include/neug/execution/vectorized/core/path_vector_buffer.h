#pragma once
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

class PathVectorBuffer : public VectorBuffer {
 public:
	explicit PathVectorBuffer(size_t capacity)
	    : VectorBuffer(VectorBufferType::kPath, capacity),
	      paths_(capacity),
	      validity_(capacity) {}

	uint8_t* GetData() override { return nullptr; }
	const uint8_t* GetData() const override { return nullptr; }
	ValidityMask& validity() override { return validity_; }
	const ValidityMask& validity() const override { return validity_; }

	execution::Path& GetPath(size_t idx) { return paths_[idx]; }
	const execution::Path& GetPath(size_t idx) const { return paths_[idx]; }
	void SetPath(size_t idx, execution::Path path) { paths_[idx] = std::move(path); }

	std::vector<execution::Path>& paths() { return paths_; }
	const std::vector<execution::Path>& paths() const { return paths_; }

 private:
	std::vector<execution::Path> paths_;
	ValidityMask validity_;
};

}  // namespace neug::execution::vec
