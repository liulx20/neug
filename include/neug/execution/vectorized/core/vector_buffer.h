#pragma once
#include <cstdint>
#include <memory>

#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/validity_mask.h"

namespace neug::execution::vec {

class GraphVector;

enum class VectorBufferType : uint8_t {
	kFlat,
	kString,
	kStruct,
	kList,
	kPath,
};

class VectorBuffer {
 public:
	explicit VectorBuffer(VectorBufferType type, size_t capacity)
	    : buffer_type_(type), capacity_(capacity) {}
	virtual ~VectorBuffer() = default;

	VectorBufferType buffer_type() const { return buffer_type_; }
	size_t capacity() const { return capacity_; }

	virtual ValidityMask& validity() = 0;
	virtual const ValidityMask& validity() const = 0;

	virtual uint8_t* GetData() { return nullptr; }
	virtual const uint8_t* GetData() const { return nullptr; }

 protected:
	VectorBufferType buffer_type_;
	size_t capacity_;
};

}  // namespace neug::execution::vec
