#include "neug/execution/vectorized/aggregate/aggregate_hash_table.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "neug/common/extra_type_info.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/core/list_vector_buffer.h"

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

void AggregateHashTable::Initialize(const RowLayout& key_layout,
                                    const std::vector<int>& key_tags,
                                    const std::vector<AggFuncDef>& agg_funcs) {
	key_layout_ = key_layout;
	key_tags_ = key_tags;
	agg_funcs_ = agg_funcs;
	has_keys_ = !key_tags.empty();

	if (has_keys_) {
		key_data_.Initialize(key_layout_);
		directory_.assign(16, EMPTY);
		directory_mask_ = 15;
	}

	agg_states_.resize(agg_funcs_.size());
}

void AggregateHashTable::AddChunk(GraphDataChunk& chunk, size_t count) {
	if (!has_keys_) {
		if (num_groups_ == 0) {
			num_groups_ = 1;
			for (size_t f = 0; f < agg_funcs_.size(); f++) {
				agg_states_[f].push_back(CreateAggState(agg_funcs_[f].kind));
			}
		}
		for (size_t row = 0; row < count; row++) {
			UpdateAggStates(0, chunk, row);
		}
		return;
	}

	for (size_t row = 0; row < count; row++) {
		uint32_t group_idx = FindOrCreateGroup(chunk, row);
		UpdateAggStates(group_idx, chunk, row);
	}
}

uint32_t AggregateHashTable::FindOrCreateGroup(const GraphDataChunk& chunk,
                                               size_t row) {
	uint64_t hash = HashKeyFromChunk(chunk, row, key_tags_, key_layout_);
	uint32_t slot = static_cast<uint32_t>(hash & directory_mask_);
	uint32_t entry_idx = directory_[slot];

	while (entry_idx != EMPTY) {
		const auto& entry = entries_[entry_idx];
		const uint8_t* key_row = key_data_.GetRow(entry.key_row_idx);
		if (CompareKeyWithRow(key_row, chunk, row, key_layout_, key_tags_)) {
			return entry.key_row_idx;
		}
		entry_idx = entry.next;
	}

	GraphDataChunk single;
	for (size_t k = 0; k < key_tags_.size(); k++) {
		int tag = key_tags_[k];
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
	key_data_.Append(single, 1);

	uint32_t new_group_idx = static_cast<uint32_t>(num_groups_);

	GroupEntry new_entry;
	new_entry.key_row_idx = new_group_idx;
	new_entry.next = directory_[slot];
	entries_.push_back(new_entry);
	directory_[slot] = static_cast<uint32_t>(entries_.size() - 1);

	for (size_t f = 0; f < agg_funcs_.size(); f++) {
		agg_states_[f].push_back(CreateAggState(agg_funcs_[f].kind));
	}

	num_groups_++;

	if (num_groups_ > (directory_.size() * 7) / 10) {
		size_t new_size = directory_.size() * 2;
		directory_.assign(new_size, EMPTY);
		directory_mask_ = new_size - 1;
		for (size_t i = 0; i < entries_.size(); i++) {
			const uint8_t* kr = key_data_.GetRow(entries_[i].key_row_idx);
			uint64_t h = 0;
			for (size_t k = 0; k < key_layout_.columns.size(); k++) {
				const auto& col = key_layout_.columns[k];
				h = CombineHash(h, MurmurHash64(kr + col.offset, col.size));
			}
			uint32_t s = static_cast<uint32_t>(h & directory_mask_);
			entries_[i].next = directory_[s];
			directory_[s] = static_cast<uint32_t>(i);
		}
	}

	return new_group_idx;
}

static double ReadNumericValue(const GraphVector& vec, size_t row,
                               DataTypeId type_id) {
	switch (type_id) {
	case DataTypeId::kInt32:
		return static_cast<double>(vec.GetData<int32_t>()[row]);
	case DataTypeId::kUInt32:
		return static_cast<double>(vec.GetData<uint32_t>()[row]);
	case DataTypeId::kInt64:
		return static_cast<double>(vec.GetData<int64_t>()[row]);
	case DataTypeId::kUInt64:
		return static_cast<double>(vec.GetData<uint64_t>()[row]);
	case DataTypeId::kFloat:
		return static_cast<double>(vec.GetData<float>()[row]);
	case DataTypeId::kDouble:
		return vec.GetData<double>()[row];
	default:
		return 0.0;
	}
}

void AggregateHashTable::UpdateAggStates(uint32_t group_idx,
                                         const GraphDataChunk& chunk,
                                         size_t row) {
	for (size_t f = 0; f < agg_funcs_.size(); f++) {
		auto& base = agg_states_[f][group_idx];
		const auto& func = agg_funcs_[f];

		if (func.kind == AggrKind::kCount && func.input_tag < 0) {
			static_cast<CountState*>(base.get())->count++;
			continue;
		}

		int col_idx = chunk.FindColumnByTag(func.input_tag);
		if (col_idx < 0) {
			if (func.kind == AggrKind::kCount)
				static_cast<CountState*>(base.get())->count++;
			continue;
		}

		const auto& vec = chunk.GetVector(static_cast<size_t>(col_idx));

		switch (func.kind) {
		case AggrKind::kCount: {
			static_cast<CountState*>(base.get())->count++;
			break;
		}
		case AggrKind::kSum: {
			auto* st = static_cast<SumState*>(base.get());
			st->sum += ReadNumericValue(vec, row, func.input_type.id());
			break;
		}
		case AggrKind::kAvg: {
			auto* st = static_cast<SumState*>(base.get());
			st->sum += ReadNumericValue(vec, row, func.input_type.id());
			st->count++;
			break;
		}
		case AggrKind::kMin: {
			auto* st = static_cast<MinState*>(base.get());
			if (func.input_type.id() == DataTypeId::kInt64 ||
			    func.input_type.id() == DataTypeId::kInt32 ||
			    func.input_type.id() == DataTypeId::kUInt64) {
				int64_t val = static_cast<int64_t>(
				    ReadNumericValue(vec, row, func.input_type.id()));
				if (val < st->val_int) st->val_int = val;
			} else {
				double val = ReadNumericValue(vec, row, func.input_type.id());
				if (val < st->val_double) st->val_double = val;
			}
			break;
		}
		case AggrKind::kMax: {
			auto* st = static_cast<MaxState*>(base.get());
			if (func.input_type.id() == DataTypeId::kInt64 ||
			    func.input_type.id() == DataTypeId::kInt32 ||
			    func.input_type.id() == DataTypeId::kUInt64) {
				int64_t val = static_cast<int64_t>(
				    ReadNumericValue(vec, row, func.input_type.id()));
				if (val > st->val_int) st->val_int = val;
			} else {
				double val = ReadNumericValue(vec, row, func.input_type.id());
				if (val > st->val_double) st->val_double = val;
			}
			break;
		}
		case AggrKind::kFirst: {
			auto* st = static_cast<FirstState*>(base.get());
			if (!st->has_first) {
				st->has_first = true;
				if (func.input_type.id() == DataTypeId::kVertex) {
					st->first_vid = VertexVector::GetVids(vec)[row];
					if (VertexVector::IsConstantLabel(vec)) {
						st->first_label = VertexVector::GetConstantLabel(vec);
					} else {
						st->first_label = VertexVector::GetLabels(vec)[row];
					}
				} else if (func.input_type.id() == DataTypeId::kVarchar) {
					auto* str = StringVector::GetStringData(
					    const_cast<GraphVector&>(vec));
					st->first_string = str[row].GetString();
				} else {
					st->first_double =
					    ReadNumericValue(vec, row, func.input_type.id());
					st->first_int = static_cast<int64_t>(st->first_double);
				}
			}
			break;
		}
		case AggrKind::kToList:
		case AggrKind::kToSet:
		case AggrKind::kCountDistinct: {
			auto* st = static_cast<ListAccumState*>(base.get());
			if (func.input_type.id() == DataTypeId::kVarchar) {
				auto* str = StringVector::GetStringData(
				    const_cast<GraphVector&>(vec));
				st->list_string.push_back(std::string(str[row].GetString()));
			} else if (func.input_type.id() == DataTypeId::kDouble ||
			           func.input_type.id() == DataTypeId::kFloat) {
				st->list_double.push_back(
				    ReadNumericValue(vec, row, func.input_type.id()));
			} else {
				st->list_int.push_back(static_cast<int64_t>(
				    ReadNumericValue(vec, row, func.input_type.id())));
			}
			break;
		}
		default:
			break;
		}
	}
}

void AggregateHashTable::Scan(size_t offset, size_t count,
                              GraphDataChunk& output) {
	output = GraphDataChunk();
	if (count == 0) {
		output.SetCardinality(0);
		return;
	}

	if (has_keys_ && key_layout_.columns.size() > 0) {
		std::vector<uint32_t> indices(count);
		for (size_t i = 0; i < count; i++) {
			indices[i] = static_cast<uint32_t>(offset + i);
		}
		GraphDataChunk key_chunk;
		key_data_.Gather(indices.data(), count, key_chunk);
		for (size_t col = 0; col < key_chunk.ColumnCount(); col++) {
			output.AddColumn(key_chunk.GetTag(col),
			                 std::move(key_chunk.GetVector(col)));
		}
	}

	for (size_t f = 0; f < agg_funcs_.size(); f++) {
		const auto& func = agg_funcs_[f];

		// --- CountDistinct: accumulate then deduplicate ---
		if (func.kind == AggrKind::kCountDistinct) {
			GraphVector vec{DataType{DataTypeId::kInt64}};
			auto* data = vec.GetData<int64_t>();
			for (size_t i = 0; i < count; i++) {
				auto* st = static_cast<ListAccumState*>(
				    agg_states_[f][offset + i].get());
				if (func.input_type.id() == DataTypeId::kVarchar) {
					std::sort(st->list_string.begin(), st->list_string.end());
					auto it = std::unique(st->list_string.begin(),
					                      st->list_string.end());
					data[i] = std::distance(st->list_string.begin(), it);
				} else if (func.input_type.id() == DataTypeId::kDouble ||
				           func.input_type.id() == DataTypeId::kFloat) {
					std::sort(st->list_double.begin(), st->list_double.end());
					auto it = std::unique(st->list_double.begin(),
					                      st->list_double.end());
					data[i] = std::distance(st->list_double.begin(), it);
				} else {
					std::sort(st->list_int.begin(), st->list_int.end());
					auto it = std::unique(st->list_int.begin(),
					                      st->list_int.end());
					data[i] = std::distance(st->list_int.begin(), it);
				}
			}
			output.AddColumn(func.output_alias, std::move(vec));
			continue;
		}

		// --- ToList / ToSet ---
		if (func.kind == AggrKind::kToList || func.kind == AggrKind::kToSet) {
			DataType child_type = func.input_type;
			if (child_type.id() != DataTypeId::kVarchar &&
			    child_type.id() != DataTypeId::kDouble &&
			    child_type.id() != DataTypeId::kFloat) {
				child_type = DataType{DataTypeId::kInt64};
			}
			DataType list_type(DataTypeId::kList,
			                   std::make_shared<ListTypeInfo>(child_type));
			GraphVector vec(list_type, count);
			auto* entries = ListVector::GetEntries(vec);
			auto& child_vec = ListVector::GetChild(vec);

			size_t total_elems = 0;
			for (size_t i = 0; i < count; i++) {
				auto* st = static_cast<ListAccumState*>(
				    agg_states_[f][offset + i].get());
				if (func.kind == AggrKind::kToSet) {
					if (child_type.id() == DataTypeId::kVarchar) {
						std::sort(st->list_string.begin(),
						          st->list_string.end());
						st->list_string.erase(
						    std::unique(st->list_string.begin(),
						                st->list_string.end()),
						    st->list_string.end());
						total_elems += st->list_string.size();
					} else if (child_type.id() == DataTypeId::kDouble ||
					           child_type.id() == DataTypeId::kFloat) {
						std::sort(st->list_double.begin(),
						          st->list_double.end());
						st->list_double.erase(
						    std::unique(st->list_double.begin(),
						                st->list_double.end()),
						    st->list_double.end());
						total_elems += st->list_double.size();
					} else {
						std::sort(st->list_int.begin(), st->list_int.end());
						st->list_int.erase(
						    std::unique(st->list_int.begin(),
						                st->list_int.end()),
						    st->list_int.end());
						total_elems += st->list_int.size();
					}
				} else {
					if (child_type.id() == DataTypeId::kVarchar)
						total_elems += st->list_string.size();
					else if (child_type.id() == DataTypeId::kDouble ||
					         child_type.id() == DataTypeId::kFloat)
						total_elems += st->list_double.size();
					else
						total_elems += st->list_int.size();
				}
			}

			child_vec = GraphVector(child_type,
			                        total_elems > 0 ? total_elems : 1);
			size_t elem_offset = 0;

			if (child_type.id() == DataTypeId::kVarchar) {
				auto* str_data = StringVector::GetStringData(child_vec);
				for (size_t i = 0; i < count; i++) {
					auto* st = static_cast<ListAccumState*>(
					    agg_states_[f][offset + i].get());
					entries[i].offset = elem_offset;
					entries[i].length = st->list_string.size();
					for (auto& s : st->list_string) {
						str_data[elem_offset++] =
						    StringVector::AddString(child_vec, s);
					}
				}
			} else if (child_type.id() == DataTypeId::kDouble ||
			           child_type.id() == DataTypeId::kFloat) {
				auto* ddata = child_vec.GetData<double>();
				for (size_t i = 0; i < count; i++) {
					auto* st = static_cast<ListAccumState*>(
					    agg_states_[f][offset + i].get());
					entries[i].offset = elem_offset;
					entries[i].length = st->list_double.size();
					for (auto v : st->list_double) {
						ddata[elem_offset++] = v;
					}
				}
			} else {
				auto* idata = child_vec.GetData<int64_t>();
				for (size_t i = 0; i < count; i++) {
					auto* st = static_cast<ListAccumState*>(
					    agg_states_[f][offset + i].get());
					entries[i].offset = elem_offset;
					entries[i].length = st->list_int.size();
					for (auto v : st->list_int) {
						idata[elem_offset++] = v;
					}
				}
			}

			output.AddColumn(func.output_alias, std::move(vec));
			continue;
		}

		// --- Scalar aggregates: Count, Sum, Avg, Min, Max, First ---
		DataTypeId out_type;
		switch (func.kind) {
		case AggrKind::kCount:
			out_type = DataTypeId::kInt64; break;
		case AggrKind::kSum:
		case AggrKind::kAvg:
			out_type = DataTypeId::kDouble; break;
		case AggrKind::kMin:
		case AggrKind::kMax:
		case AggrKind::kFirst:
			out_type = func.input_type.id(); break;
		default:
			out_type = DataTypeId::kInt64; break;
		}

		if (out_type == DataTypeId::kInt64 || out_type == DataTypeId::kInt32 ||
		    out_type == DataTypeId::kUInt64) {
			GraphVector vec{DataType{DataTypeId::kInt64}};
			auto* data = vec.GetData<int64_t>();
			for (size_t i = 0; i < count; i++) {
				auto* base = agg_states_[f][offset + i].get();
				switch (func.kind) {
				case AggrKind::kCount:
					data[i] = static_cast<CountState*>(base)->count; break;
				case AggrKind::kMin:
					data[i] = static_cast<MinState*>(base)->val_int; break;
				case AggrKind::kMax:
					data[i] = static_cast<MaxState*>(base)->val_int; break;
				case AggrKind::kFirst:
					data[i] = static_cast<FirstState*>(base)->first_int; break;
				default:
					data[i] = 0; break;
				}
			}
			output.AddColumn(func.output_alias, std::move(vec));
		} else if (out_type == DataTypeId::kDouble ||
		           out_type == DataTypeId::kFloat) {
			GraphVector vec{DataType{DataTypeId::kDouble}};
			auto* data = vec.GetData<double>();
			for (size_t i = 0; i < count; i++) {
				auto* base = agg_states_[f][offset + i].get();
				switch (func.kind) {
				case AggrKind::kSum:
					data[i] = static_cast<SumState*>(base)->sum; break;
				case AggrKind::kMin:
					data[i] = static_cast<MinState*>(base)->val_double; break;
				case AggrKind::kMax:
					data[i] = static_cast<MaxState*>(base)->val_double; break;
				case AggrKind::kAvg: {
					auto* st = static_cast<SumState*>(base);
					data[i] = st->count > 0 ? st->sum / st->count : 0.0;
					break;
				}
				case AggrKind::kFirst:
					data[i] = static_cast<FirstState*>(base)->first_double;
					break;
				default:
					data[i] = 0.0; break;
				}
			}
			output.AddColumn(func.output_alias, std::move(vec));
		} else if (out_type == DataTypeId::kVarchar) {
			GraphVector vec{DataType{DataTypeId::kVarchar}};
			auto* str_data = StringVector::GetStringData(vec);
			for (size_t i = 0; i < count; i++) {
				auto* st = static_cast<FirstState*>(
				    agg_states_[f][offset + i].get());
				str_data[i] = StringVector::AddString(vec, st->first_string);
			}
			output.AddColumn(func.output_alias, std::move(vec));
		} else if (out_type == DataTypeId::kVertex) {
			auto vec = VertexVector::Create();
			vid_t* vids = VertexVector::GetVids(vec);
			label_t* labels = VertexVector::GetLabels(vec);
			for (size_t i = 0; i < count; i++) {
				auto* st = static_cast<FirstState*>(
				    agg_states_[f][offset + i].get());
				vids[i] = st->first_vid;
				labels[i] = st->first_label;
			}
			output.AddColumn(func.output_alias, std::move(vec));
		} else {
			GraphVector vec{DataType{DataTypeId::kInt64}};
			auto* data = vec.GetData<int64_t>();
			for (size_t i = 0; i < count; i++) {
				auto* st = static_cast<FirstState*>(
				    agg_states_[f][offset + i].get());
				data[i] = st->first_int;
			}
			output.AddColumn(func.output_alias, std::move(vec));
		}
	}

	output.SetCardinality(count);
}

}  // namespace neug::execution::vec
