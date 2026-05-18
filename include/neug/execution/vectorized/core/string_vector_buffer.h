#pragma once
#include <cstring>
#include <string>
#include <vector>

#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

struct string_t {
	uint32_t length;
	char value[12];

	static constexpr size_t INLINE_LENGTH = 12;
	static constexpr size_t PREFIX_LENGTH = 4;

	bool IsInlined() const { return length <= INLINE_LENGTH; }

	const char* GetData() const {
		if (IsInlined()) {
			return value;
		}
		const char* ptr;
		std::memcpy(&ptr, value + PREFIX_LENGTH, sizeof(ptr));
		return ptr;
	}

	void SetPointer(const char* ptr) {
		std::memcpy(value + PREFIX_LENGTH, &ptr, sizeof(ptr));
	}

	const char* GetPrefix() const { return value; }

	std::string GetString() const { return std::string(GetData(), length); }
};

static_assert(sizeof(string_t) == 16);

class StringHeap {
 public:
	char* Allocate(size_t size);
	void Reset();

 private:
	static constexpr size_t BLOCK_SIZE = 4096;
	struct Block {
		std::unique_ptr<char[]> data;
		size_t used = 0;
		size_t capacity = 0;
	};
	std::vector<Block> blocks_;
};

class StringVectorBuffer : public VectorBuffer {
 public:
	explicit StringVectorBuffer(size_t capacity)
	    : VectorBuffer(VectorBufferType::kString, capacity),
	      data_(std::make_unique<uint8_t[]>(sizeof(string_t) * capacity)),
	      validity_(capacity) {}

	uint8_t* GetData() override { return data_.get(); }
	const uint8_t* GetData() const override { return data_.get(); }
	ValidityMask& validity() override { return validity_; }
	const ValidityMask& validity() const override { return validity_; }

	StringHeap& heap() { return heap_; }

	string_t* GetStringData() {
		return reinterpret_cast<string_t*>(data_.get());
	}

	const string_t* GetStringData() const {
		return reinterpret_cast<const string_t*>(data_.get());
	}

	string_t AddString(const char* data, size_t len);
	string_t AddString(const std::string& str) {
		return AddString(str.data(), str.size());
	}

 private:
	std::unique_ptr<uint8_t[]> data_;
	StringHeap heap_;
	ValidityMask validity_;
};

}  // namespace neug::execution::vec
