#include "neug/execution/vectorized/join/tuple_data.h"

#include <cassert>
#include <cstring>

#include "neug/execution/vectorized/core/graph_vector.h"

namespace neug::execution::vec {

void TupleDataCollection::Initialize(const RowLayout& layout) {
	layout_ = layout;
	rows_per_block_ = BLOCK_SIZE / layout_.row_width;
	if (rows_per_block_ == 0) rows_per_block_ = 1;
}

uint8_t* TupleDataCollection::AllocateRow() {
	size_t block_idx = count_ / rows_per_block_;
	size_t row_in_block = count_ % rows_per_block_;

	if (block_idx >= blocks_.size()) {
		blocks_.push_back(
		    std::make_unique<uint8_t[]>(rows_per_block_ * layout_.row_width));
	}

	count_++;
	return blocks_[block_idx].get() + row_in_block * layout_.row_width;
}

const uint8_t* TupleDataCollection::GetRow(size_t idx) const {
	size_t block_idx = idx / rows_per_block_;
	size_t row_in_block = idx % rows_per_block_;
	return blocks_[block_idx].get() + row_in_block * layout_.row_width;
}

uint8_t* TupleDataCollection::GetRowMut(size_t idx) {
	size_t block_idx = idx / rows_per_block_;
	size_t row_in_block = idx % rows_per_block_;
	return blocks_[block_idx].get() + row_in_block * layout_.row_width;
}

// Scatter: 列转行
void TupleDataCollection::Append(const GraphDataChunk& chunk, size_t count) {
	for (size_t row = 0; row < count; row++) {
		uint8_t* row_ptr = AllocateRow();
		std::memset(row_ptr, 0, layout_.row_width);

		for (size_t col = 0; col < layout_.columns.size(); col++) {
			const auto& col_info = layout_.columns[col];
			uint8_t* dst = row_ptr + col_info.offset;

			int chunk_col = chunk.FindColumnByTag(col_info.tag);
			if (chunk_col < 0) continue;

			const auto& vec = chunk.GetVector(static_cast<size_t>(chunk_col));

			switch (col_info.type.id()) {
			case DataTypeId::kVertex: {
				const vid_t* vids = VertexVector::GetVids(vec);
				label_t label;
				if (VertexVector::IsConstantLabel(vec)) {
					label = VertexVector::GetConstantLabel(vec);
				} else {
					label = VertexVector::GetLabels(vec)[row];
				}
				dst[0] = label;
				std::memcpy(dst + 1, &vids[row], sizeof(vid_t));
				break;
			}
			case DataTypeId::kVarchar: {
				auto* str_data = StringVector::GetStringData(
				    const_cast<GraphVector&>(vec));
				std::string s = str_data[row].GetString();
				auto heap_str = std::make_unique<std::string>(std::move(s));
				const char* ptr = heap_str->c_str();
				uint32_t len = static_cast<uint32_t>(heap_str->size());
				std::memcpy(dst, &ptr, sizeof(const char*));
				std::memcpy(dst + 8, &len, sizeof(uint32_t));
				string_heap_.push_back(std::move(heap_str));
				break;
			}
			case DataTypeId::kPath: {
				const auto& path_buf =
				    static_cast<const PathVectorBuffer&>(vec.buffer());
				auto heap_path =
				    std::make_shared<execution::Path>(path_buf.GetPath(row));
				auto* raw = heap_path.get();
				std::memcpy(dst, &raw, sizeof(void*));
				path_heap_.push_back(std::move(heap_path));
				break;
			}
			default: {
				size_t type_size = col_info.size;
				auto* src_data = vec.buffer().GetData();
				std::memcpy(dst, src_data + row * type_size, type_size);
				break;
			}
			}
		}

		// Validity: set all valid for now (TODO: null handling)
		uint8_t* validity = row_ptr + layout_.validity_offset;
		size_t validity_bytes = (layout_.columns.size() + 7) / 8;
		std::memset(validity, 0xFF, validity_bytes);
	}
}

// Gather: 行转列
void TupleDataCollection::Gather(const uint32_t* row_indices, size_t count,
                                 GraphDataChunk& output) const {
	output = GraphDataChunk();

	for (size_t col = 0; col < layout_.columns.size(); col++) {
		const auto& col_info = layout_.columns[col];

		switch (col_info.type.id()) {
		case DataTypeId::kVertex: {
			auto vec = VertexVector::Create();
			vid_t* out_vids = VertexVector::GetVids(vec);
			label_t* out_labels = VertexVector::GetLabels(vec);
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				const uint8_t* src = row_ptr + col_info.offset;
				out_labels[i] = src[0];
				std::memcpy(&out_vids[i], src + 1, sizeof(vid_t));
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		case DataTypeId::kVarchar: {
			GraphVector vec{col_info.type};
			auto* str_data = StringVector::GetStringData(vec);
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				const uint8_t* src = row_ptr + col_info.offset;
				const char* ptr;
				uint32_t len;
				std::memcpy(&ptr, src, sizeof(const char*));
				std::memcpy(&len, src + 8, sizeof(uint32_t));
				str_data[i] = StringVector::AddString(
				    vec, std::string(ptr, len));
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		case DataTypeId::kPath: {
			auto vec = PathVector::Create();
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				const uint8_t* src = row_ptr + col_info.offset;
				execution::Path* ptr;
				std::memcpy(&ptr, src, sizeof(void*));
				PathVector::SetPath(vec, i, *ptr);
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		default: {
			GraphVector vec{col_info.type};
			auto* dst_data = vec.buffer().GetData();
			size_t type_size = col_info.size;
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				std::memcpy(dst_data + i * type_size,
				            row_ptr + col_info.offset, type_size);
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		}
	}
	output.SetCardinality(count);
}

void TupleDataCollection::GatherColumns(const uint32_t* row_indices,
                                        size_t count,
                                        const std::vector<int>& col_indices,
                                        GraphDataChunk& output) const {
	output = GraphDataChunk();

	for (int col : col_indices) {
		assert(col >= 0 && static_cast<size_t>(col) < layout_.columns.size());
		const auto& col_info = layout_.columns[static_cast<size_t>(col)];

		switch (col_info.type.id()) {
		case DataTypeId::kVertex: {
			auto vec = VertexVector::Create();
			vid_t* out_vids = VertexVector::GetVids(vec);
			label_t* out_labels = VertexVector::GetLabels(vec);
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				const uint8_t* src = row_ptr + col_info.offset;
				out_labels[i] = src[0];
				std::memcpy(&out_vids[i], src + 1, sizeof(vid_t));
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		case DataTypeId::kVarchar: {
			GraphVector vec{col_info.type};
			auto* str_data = StringVector::GetStringData(vec);
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				const uint8_t* src = row_ptr + col_info.offset;
				const char* ptr;
				uint32_t len;
				std::memcpy(&ptr, src, sizeof(const char*));
				std::memcpy(&len, src + 8, sizeof(uint32_t));
				str_data[i] = StringVector::AddString(
				    vec, std::string(ptr, len));
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		case DataTypeId::kPath: {
			auto vec = PathVector::Create();
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				const uint8_t* src = row_ptr + col_info.offset;
				execution::Path* ptr;
				std::memcpy(&ptr, src, sizeof(void*));
				PathVector::SetPath(vec, i, *ptr);
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		default: {
			GraphVector vec{col_info.type};
			auto* dst_data = vec.buffer().GetData();
			size_t type_size = col_info.size;
			for (size_t i = 0; i < count; i++) {
				const uint8_t* row_ptr = GetRow(row_indices[i]);
				std::memcpy(dst_data + i * type_size,
				            row_ptr + col_info.offset, type_size);
			}
			output.AddColumn(col_info.tag, std::move(vec));
			break;
		}
		}
	}
	output.SetCardinality(count);
}

}  // namespace neug::execution::vec
