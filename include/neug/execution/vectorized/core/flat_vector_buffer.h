#pragma once
#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

class FlatVectorBuffer : public VectorBuffer {
 public:
	FlatVectorBuffer(size_t type_size, size_t capacity)
	    : VectorBuffer(VectorBufferType::kFlat, capacity),
	      data_(std::make_unique<uint8_t[]>(type_size * capacity)),
	      validity_(capacity) {}

	uint8_t* GetData() override { return data_.get(); }
	const uint8_t* GetData() const override { return data_.get(); }
	ValidityMask& validity() override { return validity_; }
	const ValidityMask& validity() const override { return validity_; }

 private:
	std::unique_ptr<uint8_t[]> data_;
	ValidityMask validity_;
};

}  // namespace neug::execution::vec
