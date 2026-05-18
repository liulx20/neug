#pragma once
#include <cstdint>
#include <cstring>
#include <memory>

#include "neug/execution/vectorized/core/constants.h"

namespace neug::execution::vec {

class ValidityMask {
 public:
	static constexpr size_t BITS_PER_ENTRY = 64;

	static constexpr size_t EntryCount(size_t count) {
		return (count + BITS_PER_ENTRY - 1) / BITS_PER_ENTRY;
	}

	ValidityMask() : data_(nullptr), count_(0) {}

	explicit ValidityMask(size_t count) : count_(count) {
		// data_ = nullptr means all valid (common case optimization)
	}

	bool AllValid() const { return data_ == nullptr; }

	void Initialize(size_t count) {
		count_ = count;
		data_.reset();
	}

	void SetInvalid(size_t idx) {
		EnsureAllocated();
		size_t entry_idx = idx / BITS_PER_ENTRY;
		size_t bit_idx = idx % BITS_PER_ENTRY;
		data_[entry_idx] &= ~(uint64_t(1) << bit_idx);
	}

	void SetValid(size_t idx) {
		if (!data_) {
			return;
		}
		size_t entry_idx = idx / BITS_PER_ENTRY;
		size_t bit_idx = idx % BITS_PER_ENTRY;
		data_[entry_idx] |= (uint64_t(1) << bit_idx);
	}

	bool IsValid(size_t idx) const {
		if (!data_) {
			return true;
		}
		size_t entry_idx = idx / BITS_PER_ENTRY;
		size_t bit_idx = idx % BITS_PER_ENTRY;
		return (data_[entry_idx] >> bit_idx) & 1;
	}

	void Reset() { data_.reset(); }

	void SetAllValid() {
		if (data_) {
			std::memset(data_.get(), 0xFF, EntryCount(count_) * sizeof(uint64_t));
		}
	}

	void SetAllInvalid() {
		EnsureAllocated();
		std::memset(data_.get(), 0, EntryCount(count_) * sizeof(uint64_t));
	}

	size_t count() const { return count_; }

 private:
	void EnsureAllocated() {
		if (!data_) {
			size_t entries = EntryCount(count_);
			data_ = std::make_unique<uint64_t[]>(entries);
			std::memset(data_.get(), 0xFF, entries * sizeof(uint64_t));
		}
	}

	std::unique_ptr<uint64_t[]> data_;
	size_t count_;
};

}  // namespace neug::execution::vec
