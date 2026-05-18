#pragma once
#include <cstddef>
#include <vector>

#include "neug/common/types.h"

namespace neug::execution::vec {

struct RowLayout {
	struct Column {
		int tag;
		DataType type;
		size_t offset;
		size_t size;
	};

	std::vector<Column> columns;
	size_t row_width = 0;
	size_t hash_offset = 0;
	size_t validity_offset = 0;

	void Initialize(const std::vector<int>& tags,
	                const std::vector<DataType>& types);

	size_t GetColumnOffset(size_t col_idx) const {
		return columns[col_idx].offset;
	}

	size_t GetColumnSize(size_t col_idx) const {
		return columns[col_idx].size;
	}

	int FindColumnByTag(int tag) const;

	static size_t GetSerializedSize(DataTypeId type_id);
};

}  // namespace neug::execution::vec
