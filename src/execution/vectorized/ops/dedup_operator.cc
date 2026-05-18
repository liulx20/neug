#include "neug/execution/vectorized/ops/dedup_operator.h"

#include <cstring>

#include "neug/execution/vectorized/core/graph_vector.h"

namespace neug::execution::vec {

static uint64_t MurmurHash64(const void* key, size_t len) {
	const uint64_t seed = 0xc70f6907UL;
	const uint64_t m = 0xc6a4a7935bd1e995ULL;
	const int r = 47;
	uint64_t h = seed ^ (len * m);
	const uint8_t* data = static_cast<const uint8_t*>(key);
	while (len >= 8) {
		uint64_t k;
		std::memcpy(&k, data, 8);
		k *= m;
		k ^= k >> r;
		k *= m;
		h ^= k;
		h *= m;
		data += 8;
		len -= 8;
	}
	switch (len) {
	case 7: h ^= uint64_t(data[6]) << 48; [[fallthrough]];
	case 6: h ^= uint64_t(data[5]) << 40; [[fallthrough]];
	case 5: h ^= uint64_t(data[4]) << 32; [[fallthrough]];
	case 4: h ^= uint64_t(data[3]) << 24; [[fallthrough]];
	case 3: h ^= uint64_t(data[2]) << 16; [[fallthrough]];
	case 2: h ^= uint64_t(data[1]) << 8; [[fallthrough]];
	case 1: h ^= uint64_t(data[0]); h *= m;
	}
	h ^= h >> r;
	h *= m;
	h ^= h >> r;
	return h;
}

static uint64_t CombineHash(uint64_t h1, uint64_t h2) {
	h1 ^= h2 + 0x9e3779b97f4a7c15ULL + (h1 << 12) + (h1 >> 4);
	return h1;
}

static uint64_t HashKeyFromChunk(const GraphDataChunk& chunk, size_t row,
                                 const std::vector<int>& key_tags,
                                 const RowLayout& layout) {
	uint64_t hash = 0;
	for (size_t k = 0; k < key_tags.size(); k++) {
		int tag = key_tags[k];
		int col_idx = chunk.FindColumnByTag(tag);
		if (col_idx < 0) continue;
		const auto& vec = chunk.GetVector(static_cast<size_t>(col_idx));
		const auto& col_info = layout.columns[k];

		uint8_t buf[16];
		switch (col_info.type.id()) {
		case DataTypeId::kVertex: {
			const vid_t* vids = VertexVector::GetVids(vec);
			label_t label;
			if (VertexVector::IsConstantLabel(vec)) {
				label = VertexVector::GetConstantLabel(vec);
			} else {
				label = VertexVector::GetLabels(vec)[row];
			}
			buf[0] = label;
			std::memcpy(buf + 1, &vids[row], sizeof(vid_t));
			hash = CombineHash(hash, MurmurHash64(buf, 5));
			break;
		}
		case DataTypeId::kVarchar: {
			auto* str_data = StringVector::GetStringData(
			    const_cast<GraphVector&>(vec));
			auto s = str_data[row].GetString();
			hash = CombineHash(hash, MurmurHash64(s.data(), s.size()));
			break;
		}
		default: {
			size_t type_size = col_info.size;
			const auto* data = vec.buffer().GetData();
			hash = CombineHash(hash,
			                   MurmurHash64(data + row * type_size, type_size));
			break;
		}
		}
	}
	return hash;
}

static bool CompareKeyWithRow(const uint8_t* row_data,
                              const GraphDataChunk& chunk, size_t chunk_row,
                              const RowLayout& layout,
                              const std::vector<int>& key_tags) {
	for (size_t k = 0; k < key_tags.size(); k++) {
		const auto& col = layout.columns[k];
		const uint8_t* stored = row_data + col.offset;

		int tag = key_tags[k];
		int col_idx = chunk.FindColumnByTag(tag);
		if (col_idx < 0) return false;
		const auto& vec = chunk.GetVector(static_cast<size_t>(col_idx));

		switch (col.type.id()) {
		case DataTypeId::kVertex: {
			const vid_t* vids = VertexVector::GetVids(vec);
			label_t label;
			if (VertexVector::IsConstantLabel(vec)) {
				label = VertexVector::GetConstantLabel(vec);
			} else {
				label = VertexVector::GetLabels(vec)[chunk_row];
			}
			if (stored[0] != label) return false;
			vid_t stored_vid;
			std::memcpy(&stored_vid, stored + 1, sizeof(vid_t));
			if (stored_vid != vids[chunk_row]) return false;
			break;
		}
		case DataTypeId::kVarchar: {
			auto* str_data = StringVector::GetStringData(
			    const_cast<GraphVector&>(vec));
			auto s = str_data[chunk_row].GetString();
			const char* ptr;
			uint32_t len;
			std::memcpy(&ptr, stored, sizeof(const char*));
			std::memcpy(&len, stored + 8, sizeof(uint32_t));
			if (len != s.size()) return false;
			if (std::memcmp(ptr, s.data(), len) != 0) return false;
			break;
		}
		default: {
			size_t type_size = col.size;
			const auto* data = vec.buffer().GetData();
			if (std::memcmp(stored, data + chunk_row * type_size, type_size) !=
			    0)
				return false;
			break;
		}
		}
	}
	return true;
}

struct DedupState : public OperatorState {
	static constexpr uint32_t EMPTY = UINT32_MAX;

	struct Entry {
		uint32_t row_idx;
		uint32_t next;
	};

	std::vector<uint32_t> directory;
	std::vector<Entry> entries;
	size_t directory_mask = 0;
	size_t count = 0;

	TupleDataCollection key_data;

	void EnsureInitialized(const RowLayout& layout) {
		if (directory.empty()) {
			key_data.Initialize(layout);
			directory.assign(16, EMPTY);
			directory_mask = 15;
		}
	}

	bool InsertIfNew(const GraphDataChunk& chunk, size_t row,
	                 const std::vector<int>& key_tags,
	                 const RowLayout& layout) {
		EnsureInitialized(layout);

		uint64_t hash = HashKeyFromChunk(chunk, row, key_tags, layout);
		uint32_t slot = static_cast<uint32_t>(hash & directory_mask);
		uint32_t entry_idx = directory[slot];

		while (entry_idx != EMPTY) {
			const auto& entry = entries[entry_idx];
			const uint8_t* key_row = key_data.GetRow(entry.row_idx);
			if (CompareKeyWithRow(key_row, chunk, row, layout, key_tags)) {
				return false;
			}
			entry_idx = entries[entry_idx].next;
		}

		// New key: store it
		GraphDataChunk single;
		for (size_t k = 0; k < key_tags.size(); k++) {
			int tag = key_tags[k];
			int col_idx = chunk.FindColumnByTag(tag);
			if (col_idx >= 0) {
				const auto& vec = chunk.GetVector(static_cast<size_t>(col_idx));
				sel_t sel_idx = static_cast<sel_t>(row);
				auto sliced = GraphVector::Slice(vec, &sel_idx, 1);
				Flatten(sliced, 1);
				single.AddColumn(tag, std::move(sliced));
			}
		}
		single.SetCardinality(1);
		key_data.Append(single, 1);

		uint32_t new_row_idx = static_cast<uint32_t>(count);
		Entry new_entry;
		new_entry.row_idx = new_row_idx;
		new_entry.next = directory[slot];
		entries.push_back(new_entry);
		directory[slot] = static_cast<uint32_t>(entries.size() - 1);

		count++;

		// Resize if load > 0.7
		if (count > (directory.size() * 7) / 10) {
			size_t new_size = directory.size() * 2;
			directory.assign(new_size, EMPTY);
			directory_mask = new_size - 1;
			for (size_t i = 0; i < entries.size(); i++) {
				const uint8_t* kr = key_data.GetRow(entries[i].row_idx);
				uint64_t h = 0;
				for (size_t c = 0; c < layout.columns.size(); c++) {
					const auto& col = layout.columns[c];
					h = CombineHash(h, MurmurHash64(kr + col.offset, col.size));
				}
				uint32_t s = static_cast<uint32_t>(h & directory_mask);
				entries[i].next = directory[s];
				directory[s] = static_cast<uint32_t>(i);
			}
		}

		return true;
	}
};

DedupOperator::DedupOperator(std::vector<int> key_tags,
                             std::vector<DataType> key_types)
    : key_tags_(std::move(key_tags)) {
	key_layout_.Initialize(key_tags_, key_types);
}

std::unique_ptr<OperatorState> DedupOperator::GetOperatorState() const {
	return std::make_unique<DedupState>();
}

OperatorResultType DedupOperator::Execute(GraphDataChunk& input,
                                          GraphDataChunk& output,
                                          OperatorState& ostate,
                                          const VecExecContext&) {
	auto& state = static_cast<DedupState&>(ostate);
	size_t count = input.size();
	if (count == 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	input.Flatten();

	std::vector<sel_t> sel;
	sel.reserve(count);

	for (size_t i = 0; i < count; i++) {
		if (state.InsertIfNew(input, i, key_tags_, key_layout_)) {
			sel.push_back(static_cast<sel_t>(i));
		}
	}

	if (sel.empty()) {
		output = GraphDataChunk();
		output.SetCardinality(0);
	} else if (sel.size() == count) {
		output = std::move(input);
	} else {
		output = GraphDataChunk();
		for (size_t col = 0; col < input.ColumnCount(); col++) {
			int tag = input.GetTag(col);
			const auto& vec = input.GetVector(col);
			auto sliced = GraphVector::Slice(vec, sel.data(), sel.size());
			output.AddColumn(tag, std::move(sliced));
		}
		output.SetCardinality(sel.size());
	}

	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
