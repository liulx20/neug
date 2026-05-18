#include "neug/execution/vectorized/core/string_vector_buffer.h"

#include <algorithm>
#include <cstring>

namespace neug::execution::vec {

char* StringHeap::Allocate(size_t size) {
	if (!blocks_.empty()) {
		auto& last = blocks_.back();
		if (last.used + size <= last.capacity) {
			char* ptr = last.data.get() + last.used;
			last.used += size;
			return ptr;
		}
	}
	size_t block_size = std::max(BLOCK_SIZE, size);
	Block block;
	block.data = std::make_unique<char[]>(block_size);
	block.capacity = block_size;
	block.used = size;
	char* ptr = block.data.get();
	blocks_.push_back(std::move(block));
	return ptr;
}

void StringHeap::Reset() {
	blocks_.clear();
}

string_t StringVectorBuffer::AddString(const char* data, size_t len) {
	string_t result;
	result.length = static_cast<uint32_t>(len);
	std::memset(result.value, 0, sizeof(result.value));
	if (len <= string_t::INLINE_LENGTH) {
		std::memcpy(result.value, data, len);
	} else {
		char* heap_ptr = heap_.Allocate(len);
		std::memcpy(heap_ptr, data, len);
		std::memcpy(result.value, data,
		            std::min(len, size_t(string_t::PREFIX_LENGTH)));
		result.SetPointer(heap_ptr);
	}
	return result;
}

}  // namespace neug::execution::vec
