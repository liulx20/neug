#include "neug/execution/vectorized/join/join_hash_table.h"

#include <cassert>
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

void JoinHashTable::Initialize(const RowLayout& layout,
                               const std::vector<int>& key_col_indices) {
	layout_ = layout;
	key_col_indices_ = key_col_indices;
}

uint64_t JoinHashTable::HashRow(const uint8_t* row, const RowLayout& layout,
                                const std::vector<int>& key_cols) {
	uint64_t hash = 0;
	for (int col_idx : key_cols) {
		const auto& col = layout.columns[static_cast<size_t>(col_idx)];
		const uint8_t* data = row + col.offset;
		uint64_t h = MurmurHash64(data, col.size);
		hash = CombineHash(hash, h);
	}
	return hash;
}

uint64_t JoinHashTable::HashProbeKeys(const GraphDataChunk& chunk, size_t row,
                                      const std::vector<int>& key_tags,
                                      const RowLayout& layout,
                                      const std::vector<int>& key_cols) {
	uint64_t hash = 0;
	for (size_t k = 0; k < key_tags.size(); k++) {
		int tag = key_tags[k];
		int col_idx = chunk.FindColumnByTag(tag);
		assert(col_idx >= 0);
		const auto& vec = chunk.GetVector(static_cast<size_t>(col_idx));
		const auto& layout_col = layout.columns[static_cast<size_t>(key_cols[k])];

		uint8_t buf[16];
		switch (layout_col.type.id()) {
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
		default: {
			size_t type_size = layout_col.size;
			const auto* data = vec.buffer().GetData();
			hash = CombineHash(hash,
			                   MurmurHash64(data + row * type_size, type_size));
			break;
		}
		}
	}
	return hash;
}

bool JoinHashTable::CompareKeys(const uint8_t* build_row,
                                const GraphDataChunk& probe_chunk,
                                size_t probe_row,
                                const RowLayout& layout,
                                const std::vector<int>& key_cols,
                                const std::vector<int>& probe_key_tags) {
	for (size_t k = 0; k < key_cols.size(); k++) {
		int build_col = key_cols[k];
		const auto& col = layout.columns[static_cast<size_t>(build_col)];
		const uint8_t* build_data = build_row + col.offset;

		int tag = probe_key_tags[k];
		int probe_col = probe_chunk.FindColumnByTag(tag);
		assert(probe_col >= 0);
		const auto& vec = probe_chunk.GetVector(static_cast<size_t>(probe_col));

		switch (col.type.id()) {
		case DataTypeId::kVertex: {
			const vid_t* vids = VertexVector::GetVids(vec);
			label_t label;
			if (VertexVector::IsConstantLabel(vec)) {
				label = VertexVector::GetConstantLabel(vec);
			} else {
				label = VertexVector::GetLabels(vec)[probe_row];
			}
			if (build_data[0] != label) return false;
			vid_t build_vid;
			std::memcpy(&build_vid, build_data + 1, sizeof(vid_t));
			if (build_vid != vids[probe_row]) return false;
			break;
		}
		default: {
			size_t type_size = col.size;
			const auto* probe_data = vec.buffer().GetData();
			if (std::memcmp(build_data,
			                probe_data + probe_row * type_size,
			                type_size) != 0) {
				return false;
			}
			break;
		}
		}
	}
	return true;
}

void JoinHashTable::Build(const TupleDataCollection& data) {
	data_ = &data;
	size_t n = data.Count();
	if (n == 0) {
		directory_.assign(1, EMPTY);
		directory_mask_ = 0;
		return;
	}

	// Power-of-2 directory size, at least 2x entries
	size_t dir_size = 1;
	while (dir_size < n * 2) dir_size <<= 1;
	directory_.assign(dir_size, EMPTY);
	directory_mask_ = dir_size - 1;

	entries_.resize(n);
	for (size_t i = 0; i < n; i++) {
		const uint8_t* row = data.GetRow(i);
		uint64_t hash = HashRow(row, layout_, key_col_indices_);

		// Store hash in the row (at offset 0)
		const_cast<uint8_t*>(row)[0] = 0;  // hash stored separately in entry

		uint32_t slot = static_cast<uint32_t>(hash & directory_mask_);
		entries_[i].row_idx = static_cast<uint32_t>(i);
		entries_[i].next = directory_[slot];
		directory_[slot] = static_cast<uint32_t>(i);
	}
}

void JoinHashTable::Probe(const GraphDataChunk& probe_chunk,
                          const std::vector<int>& probe_key_tags,
                          size_t count,
                          std::vector<uint32_t>& build_matches,
                          std::vector<uint32_t>& probe_matches) const {
	build_matches.clear();
	probe_matches.clear();

	if (data_ == nullptr || data_->Count() == 0) return;

	for (size_t i = 0; i < count; i++) {
		uint64_t hash = HashProbeKeys(probe_chunk, i, probe_key_tags,
		                              layout_, key_col_indices_);
		uint32_t slot = static_cast<uint32_t>(hash & directory_mask_);
		uint32_t entry_idx = directory_[slot];

		while (entry_idx != EMPTY) {
			const auto& entry = entries_[entry_idx];
			const uint8_t* build_row = data_->GetRow(entry.row_idx);

			if (CompareKeys(build_row, probe_chunk, i, layout_,
			                key_col_indices_, probe_key_tags)) {
				build_matches.push_back(entry.row_idx);
				probe_matches.push_back(static_cast<uint32_t>(i));
			}

			entry_idx = entry.next;
		}
	}
}

void JoinHashTable::ProbeExists(const GraphDataChunk& probe_chunk,
                                const std::vector<int>& probe_key_tags,
                                size_t count,
                                std::vector<bool>& has_match) const {
	has_match.assign(count, false);

	if (data_ == nullptr || data_->Count() == 0) return;

	for (size_t i = 0; i < count; i++) {
		uint64_t hash = HashProbeKeys(probe_chunk, i, probe_key_tags,
		                              layout_, key_col_indices_);
		uint32_t slot = static_cast<uint32_t>(hash & directory_mask_);
		uint32_t entry_idx = directory_[slot];

		while (entry_idx != EMPTY) {
			const auto& entry = entries_[entry_idx];
			const uint8_t* build_row = data_->GetRow(entry.row_idx);

			if (CompareKeys(build_row, probe_chunk, i, layout_,
			                key_col_indices_, probe_key_tags)) {
				has_match[i] = true;
				break;
			}
			entry_idx = entry.next;
		}
	}
}

}  // namespace neug::execution::vec
