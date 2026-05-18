#pragma once
#include <memory>
#include <vector>

#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

class GraphVector;

class StructVectorBuffer : public VectorBuffer {
 public:
	StructVectorBuffer(std::vector<std::unique_ptr<GraphVector>> children,
	                   size_t capacity)
	    : VectorBuffer(VectorBufferType::kStruct, capacity),
	      children_(std::move(children)),
	      validity_(capacity) {}

	ValidityMask& validity() override { return validity_; }
	const ValidityMask& validity() const override { return validity_; }

	size_t ChildCount() const { return children_.size(); }
	GraphVector& GetChild(size_t idx) { return *children_[idx]; }
	const GraphVector& GetChild(size_t idx) const { return *children_[idx]; }

	std::vector<std::unique_ptr<GraphVector>>& GetChildren() {
		return children_;
	}
	const std::vector<std::unique_ptr<GraphVector>>& GetChildren() const {
		return children_;
	}

 private:
	std::vector<std::unique_ptr<GraphVector>> children_;
	ValidityMask validity_;
};

}  // namespace neug::execution::vec
