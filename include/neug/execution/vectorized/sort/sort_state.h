#pragma once
#include <cstdint>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/vectorized/join/tuple_data.h"

namespace neug::execution::vec {

struct SortKeyDef {
	int col_idx;
	DataTypeId type;
	bool ascending;
};

class SortKeyComparator {
 public:
	SortKeyComparator(const TupleDataCollection& data,
	                  const std::vector<SortKeyDef>& keys)
	    : data_(data), keys_(keys) {}

	bool operator()(uint32_t lhs_idx, uint32_t rhs_idx) const;

 private:
	const TupleDataCollection& data_;
	const std::vector<SortKeyDef>& keys_;
};

}  // namespace neug::execution::vec
