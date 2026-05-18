#include "neug/execution/vectorized/sort/sort_state.h"

#include <cstring>

namespace neug::execution::vec {

bool SortKeyComparator::operator()(uint32_t lhs_idx, uint32_t rhs_idx) const {
	const auto& layout = data_.layout();

	for (const auto& key : keys_) {
		const auto& col = layout.columns[key.col_idx];
		const uint8_t* lhs_row = data_.GetRow(lhs_idx) + col.offset;
		const uint8_t* rhs_row = data_.GetRow(rhs_idx) + col.offset;

		int cmp = 0;
		switch (key.type) {
		case DataTypeId::kInt32: {
			int32_t l, r;
			std::memcpy(&l, lhs_row, sizeof(int32_t));
			std::memcpy(&r, rhs_row, sizeof(int32_t));
			cmp = (l < r) ? -1 : (l > r) ? 1 : 0;
			break;
		}
		case DataTypeId::kUInt32: {
			uint32_t l, r;
			std::memcpy(&l, lhs_row, sizeof(uint32_t));
			std::memcpy(&r, rhs_row, sizeof(uint32_t));
			cmp = (l < r) ? -1 : (l > r) ? 1 : 0;
			break;
		}
		case DataTypeId::kInt64: {
			int64_t l, r;
			std::memcpy(&l, lhs_row, sizeof(int64_t));
			std::memcpy(&r, rhs_row, sizeof(int64_t));
			cmp = (l < r) ? -1 : (l > r) ? 1 : 0;
			break;
		}
		case DataTypeId::kUInt64: {
			uint64_t l, r;
			std::memcpy(&l, lhs_row, sizeof(uint64_t));
			std::memcpy(&r, rhs_row, sizeof(uint64_t));
			cmp = (l < r) ? -1 : (l > r) ? 1 : 0;
			break;
		}
		case DataTypeId::kFloat: {
			float l, r;
			std::memcpy(&l, lhs_row, sizeof(float));
			std::memcpy(&r, rhs_row, sizeof(float));
			cmp = (l < r) ? -1 : (l > r) ? 1 : 0;
			break;
		}
		case DataTypeId::kDouble: {
			double l, r;
			std::memcpy(&l, lhs_row, sizeof(double));
			std::memcpy(&r, rhs_row, sizeof(double));
			cmp = (l < r) ? -1 : (l > r) ? 1 : 0;
			break;
		}
		case DataTypeId::kVarchar: {
			const char *l_ptr, *r_ptr;
			uint32_t l_len, r_len;
			std::memcpy(&l_ptr, lhs_row, sizeof(const char*));
			std::memcpy(&l_len, lhs_row + 8, sizeof(uint32_t));
			std::memcpy(&r_ptr, rhs_row, sizeof(const char*));
			std::memcpy(&r_len, rhs_row + 8, sizeof(uint32_t));
			size_t min_len = l_len < r_len ? l_len : r_len;
			cmp = std::memcmp(l_ptr, r_ptr, min_len);
			if (cmp == 0) {
				cmp = (l_len < r_len) ? -1 : (l_len > r_len) ? 1 : 0;
			}
			break;
		}
		case DataTypeId::kVertex: {
			// Compare label first, then vid
			uint8_t l_label = lhs_row[0];
			uint8_t r_label = rhs_row[0];
			if (l_label != r_label) {
				cmp = (l_label < r_label) ? -1 : 1;
			} else {
				uint32_t l_vid, r_vid;
				std::memcpy(&l_vid, lhs_row + 1, sizeof(uint32_t));
				std::memcpy(&r_vid, rhs_row + 1, sizeof(uint32_t));
				cmp = (l_vid < r_vid) ? -1 : (l_vid > r_vid) ? 1 : 0;
			}
			break;
		}
		default:
			cmp = std::memcmp(lhs_row, rhs_row, col.size);
			break;
		}

		if (cmp != 0) {
			return key.ascending ? (cmp < 0) : (cmp > 0);
		}
	}
	return false;
}

}  // namespace neug::execution::vec
