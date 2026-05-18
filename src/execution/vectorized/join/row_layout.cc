#include "neug/execution/vectorized/join/row_layout.h"

namespace neug::execution::vec {

size_t RowLayout::GetSerializedSize(DataTypeId type_id) {
	switch (type_id) {
	case DataTypeId::kBoolean:
		return 1;
	case DataTypeId::kInt8:
	case DataTypeId::kUInt8:
		return 1;
	case DataTypeId::kInt16:
	case DataTypeId::kUInt16:
		return 2;
	case DataTypeId::kInt32:
	case DataTypeId::kUInt32:
	case DataTypeId::kFloat:
		return 4;
	case DataTypeId::kInt64:
	case DataTypeId::kUInt64:
	case DataTypeId::kDouble:
	case DataTypeId::kDate:
	case DataTypeId::kTimestampMs:
		return 8;
	case DataTypeId::kVertex:
		return 5;  // label(1) + vid(4)
	case DataTypeId::kVarchar:
		return 16;  // inline string_t {ptr(8) + len(4) + prefix(4)}
	case DataTypeId::kPath:
		return sizeof(void*);  // pointer to Path in side heap
	default:
		return 8;
	}
}

void RowLayout::Initialize(const std::vector<int>& tags,
                           const std::vector<DataType>& types) {
	columns.clear();
	columns.reserve(tags.size());

	// Layout: [hash(8)] [col0] [col1] ... [colN] [validity_bitmask]
	size_t offset = 8;  // hash at position 0

	for (size_t i = 0; i < tags.size(); i++) {
		size_t col_size = GetSerializedSize(types[i].id());
		columns.push_back({tags[i], types[i], offset, col_size});
		offset += col_size;
	}

	validity_offset = offset;
	size_t validity_bytes = (tags.size() + 7) / 8;
	offset += validity_bytes;

	hash_offset = 0;
	row_width = offset;
}

int RowLayout::FindColumnByTag(int tag) const {
	for (size_t i = 0; i < columns.size(); i++) {
		if (columns[i].tag == tag) {
			return static_cast<int>(i);
		}
	}
	return -1;
}

}  // namespace neug::execution::vec
