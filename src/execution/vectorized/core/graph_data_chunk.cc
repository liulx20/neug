#include "neug/execution/vectorized/core/graph_data_chunk.h"

#include <cassert>
#include <cstring>
#include <sstream>
#include <stdexcept>

namespace neug::execution::vec {

void GraphDataChunk::Initialize(
    const std::vector<std::pair<int, DataType>>& columns, size_t capacity) {
	vectors_.clear();
	tags_.clear();
	vectors_.reserve(columns.size());
	tags_.reserve(columns.size());
	for (auto& [tag, type] : columns) {
		tags_.push_back(tag);
		vectors_.emplace_back(type, capacity);
	}
	count_ = 0;
}

void GraphDataChunk::Reset() {
	count_ = 0;
}

int GraphDataChunk::FindColumnByTag(int tag) const {
	for (size_t i = 0; i < tags_.size(); i++) {
		if (tags_[i] == tag) {
			return static_cast<int>(i);
		}
	}
	return -1;
}

GraphVector& GraphDataChunk::GetVectorByTag(int tag) {
	int idx = FindColumnByTag(tag);
	assert(idx >= 0);
	return vectors_[static_cast<size_t>(idx)];
}

const GraphVector& GraphDataChunk::GetVectorByTag(int tag) const {
	int idx = FindColumnByTag(tag);
	assert(idx >= 0);
	return vectors_[static_cast<size_t>(idx)];
}

std::vector<DataType> GraphDataChunk::GetTypes() const {
	std::vector<DataType> types;
	types.reserve(vectors_.size());
	for (auto& vec : vectors_) {
		types.push_back(vec.type());
	}
	return types;
}

void GraphDataChunk::Flatten() {
	for (auto& vec : vectors_) {
		if (vec.IsDictionary()) {
			neug::execution::vec::Flatten(vec, count_);
		}
	}
}

static void CompactFlatVector(GraphVector& vec, const SelectionVector& sel,
                              size_t sel_count) {
	size_t type_size = GetTypeSize(vec.type_id());
	if (type_size == 0) {
		return;
	}
	auto* data = vec.buffer().GetData();
	auto tmp = std::make_unique<uint8_t[]>(type_size * sel_count);
	for (size_t i = 0; i < sel_count; i++) {
		std::memcpy(tmp.get() + i * type_size,
		            data + sel.GetIndex(i) * type_size, type_size);
	}
	std::memcpy(data, tmp.get(), type_size * sel_count);

	if (!vec.validity().AllValid()) {
		ValidityMask new_mask(sel_count);
		for (size_t i = 0; i < sel_count; i++) {
			if (!vec.validity().IsValid(sel.GetIndex(i))) {
				new_mask.SetInvalid(i);
			}
		}
		// Copy new validity state back
		// For simplicity, invalidate and re-set
		vec.validity().Reset();
		for (size_t i = 0; i < sel_count; i++) {
			if (!new_mask.IsValid(i)) {
				vec.validity().SetInvalid(i);
			}
		}
	}
}

static void CompactStringVector(GraphVector& vec, const SelectionVector& sel,
                                size_t sel_count) {
	auto& buf = static_cast<StringVectorBuffer&>(vec.buffer());
	auto* data = buf.GetStringData();
	auto tmp = std::make_unique<string_t[]>(sel_count);
	for (size_t i = 0; i < sel_count; i++) {
		tmp[i] = data[sel.GetIndex(i)];
	}
	std::memcpy(data, tmp.get(), sizeof(string_t) * sel_count);
}

static void CompactStructVector(GraphVector& vec, const SelectionVector& sel,
                                size_t sel_count) {
	auto& buf = static_cast<StructVectorBuffer&>(vec.buffer());
	for (size_t c = 0; c < buf.ChildCount(); c++) {
		auto& child = buf.GetChild(c);
		if (child.IsConstant()) {
			continue;
		}
		if (child.buffer_type() == VectorBufferType::kFlat) {
			CompactFlatVector(child, sel, sel_count);
		} else if (child.buffer_type() == VectorBufferType::kString) {
			CompactStringVector(child, sel, sel_count);
		} else if (child.buffer_type() == VectorBufferType::kStruct) {
			CompactStructVector(child, sel, sel_count);
		}
	}
}

void GraphDataChunk::Compact(const SelectionVector& sel, size_t sel_count) {
	for (auto& vec : vectors_) {
		if (vec.IsConstant()) {
			continue;
		}
		switch (vec.buffer_type()) {
		case VectorBufferType::kFlat:
			CompactFlatVector(vec, sel, sel_count);
			break;
		case VectorBufferType::kString:
			CompactStringVector(vec, sel, sel_count);
			break;
		case VectorBufferType::kStruct:
			CompactStructVector(vec, sel, sel_count);
			break;
		default:
			break;
		}
	}
	count_ = sel_count;
}

void GraphDataChunk::Append(const GraphDataChunk& other, size_t count) {
	assert(vectors_.size() == other.vectors_.size());
	for (size_t i = 0; i < vectors_.size(); i++) {
		auto& dst = vectors_[i];
		auto& src = other.vectors_[i];
		size_t type_size = GetTypeSize(dst.type_id());
		if (type_size > 0 && dst.buffer_type() == VectorBufferType::kFlat) {
			auto* dst_data = dst.buffer().GetData();
			auto* src_data = src.buffer().GetData();
			std::memcpy(dst_data + count_ * type_size, src_data,
			            type_size * count);
		}
	}
	count_ += count;
}

void GraphDataChunk::AddColumn(int tag, DataType type, size_t capacity) {
	tags_.push_back(tag);
	vectors_.emplace_back(std::move(type), capacity);
}

void GraphDataChunk::AddColumn(int tag, GraphVector&& vec) {
	tags_.push_back(tag);
	vectors_.push_back(std::move(vec));
}

std::string GraphDataChunk::ToString() const {
	std::ostringstream ss;
	ss << "GraphDataChunk(rows=" << count_
	   << ", cols=" << vectors_.size() << ", tags=[";
	for (size_t i = 0; i < tags_.size(); i++) {
		if (i > 0) {
			ss << ",";
		}
		ss << tags_[i];
	}
	ss << "])";
	return ss.str();
}

}  // namespace neug::execution::vec
