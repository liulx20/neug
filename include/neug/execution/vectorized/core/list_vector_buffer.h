#pragma once
#include <memory>

#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

class GraphVector;

struct list_entry_t {
	uint64_t offset;
	uint64_t length;
};

class ListVectorBuffer : public VectorBuffer {
 public:
	ListVectorBuffer(std::unique_ptr<GraphVector> child, size_t capacity)
	    : VectorBuffer(VectorBufferType::kList, capacity),
	      data_(std::make_unique<uint8_t[]>(sizeof(list_entry_t) * capacity)),
	      child_(std::move(child)),
	      validity_(capacity) {}

	uint8_t* GetData() override { return data_.get(); }
	const uint8_t* GetData() const override { return data_.get(); }
	ValidityMask& validity() override { return validity_; }
	const ValidityMask& validity() const override { return validity_; }

	list_entry_t* GetListEntries() {
		return reinterpret_cast<list_entry_t*>(data_.get());
	}
	const list_entry_t* GetListEntries() const {
		return reinterpret_cast<const list_entry_t*>(data_.get());
	}

	GraphVector& GetChild() { return *child_; }
	const GraphVector& GetChild() const { return *child_; }

 private:
	std::unique_ptr<uint8_t[]> data_;
	std::unique_ptr<GraphVector> child_;
	ValidityMask validity_;
};

}  // namespace neug::execution::vec
