#pragma once
#include <cstdint>
#include <memory>

#include "neug/execution/vectorized/core/constants.h"

namespace neug::execution::vec {

class SelectionVector {
 public:
	SelectionVector() : sel_(nullptr), count_(0), owned_(false) {}

	explicit SelectionVector(size_t count)
	    : owned_data_(std::make_unique<sel_t[]>(count)),
	      sel_(owned_data_.get()),
	      count_(count),
	      owned_(true) {}

	SelectionVector(sel_t* data, size_t count)
	    : sel_(data), count_(count), owned_(false) {}

	sel_t GetIndex(size_t i) const { return sel_[i]; }
	void SetIndex(size_t i, sel_t val) { sel_[i] = val; }

	sel_t* data() { return sel_; }
	const sel_t* data() const { return sel_; }
	size_t count() const { return count_; }
	void SetCount(size_t count) { count_ = count; }

	bool IsSet() const { return sel_ != nullptr; }

	void InitializeIdentity(size_t count) {
		if (!owned_ || count_ < count) {
			owned_data_ = std::make_unique<sel_t[]>(count);
			sel_ = owned_data_.get();
			owned_ = true;
		}
		count_ = count;
		for (size_t i = 0; i < count; i++) {
			sel_[i] = static_cast<sel_t>(i);
		}
	}

 private:
	std::unique_ptr<sel_t[]> owned_data_;
	sel_t* sel_;
	size_t count_;
	bool owned_;
};

}  // namespace neug::execution::vec
