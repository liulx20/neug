#include "neug/execution/vectorized/compiler/vec_result_collector.h"

#include <sstream>

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/core/list_vector_buffer.h"
#include "neug/execution/vectorized/core/string_vector_buffer.h"

namespace neug::execution::vec {

VecResultCollector::VecResultCollector(std::vector<int> output_tags,
                                       std::vector<DataType> output_types)
    : output_tags_(std::move(output_tags)),
      output_types_(std::move(output_types)) {}

std::unique_ptr<GlobalSinkState> VecResultCollector::GetGlobalSinkState()
    const {
	return std::make_unique<GlobalSinkState>();
}

std::unique_ptr<LocalSinkState> VecResultCollector::GetLocalSinkState(
    GlobalSinkState&) const {
	return std::make_unique<LocalSinkState>();
}

SinkResultType VecResultCollector::Sink(GraphDataChunk& chunk,
                                        GlobalSinkState&, LocalSinkState&) {
	total_rows_ += chunk.size();
	chunks_.push_back(std::move(chunk));
	return SinkResultType::kNeedMoreInput;
}

void VecResultCollector::SerializeToResponse(
    neug::QueryResponse* response) const {
	response->set_row_count(static_cast<int32_t>(total_rows_));

	for (size_t col = 0; col < output_tags_.size(); ++col) {
		int tag = output_tags_[col];
		auto* array = response->add_arrays();

		DataType col_type = (col < output_types_.size())
		                        ? output_types_[col]
		                        : DataType{DataTypeId::kUnknown};

		if (col_type.id() == DataTypeId::kUnknown && !chunks_.empty()) {
			int col_idx = chunks_[0].FindColumnByTag(tag);
			if (col_idx >= 0) {
				col_type = chunks_[0].GetVector(col_idx).type();
			}
		}

		switch (col_type.id()) {
		case DataTypeId::kInt32: {
			auto* typed = array->mutable_int32_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = vec.GetData<int32_t>();
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i]);
				}
			}
			break;
		}
		case DataTypeId::kUInt32: {
			auto* typed = array->mutable_uint32_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = vec.GetData<uint32_t>();
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i]);
				}
			}
			break;
		}
		case DataTypeId::kInt64: {
			auto* typed = array->mutable_int64_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = vec.GetData<int64_t>();
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i]);
				}
			}
			break;
		}
		case DataTypeId::kUInt64: {
			auto* typed = array->mutable_uint64_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = vec.GetData<uint64_t>();
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i]);
				}
			}
			break;
		}
		case DataTypeId::kFloat: {
			auto* typed = array->mutable_float_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = vec.GetData<float>();
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i]);
				}
			}
			break;
		}
		case DataTypeId::kDouble: {
			auto* typed = array->mutable_double_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = vec.GetData<double>();
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i]);
				}
			}
			break;
		}
		case DataTypeId::kVarchar: {
			auto* typed = array->mutable_string_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = StringVector::GetStringData(vec);
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i].GetString());
				}
			}
			break;
		}
		case DataTypeId::kBoolean: {
			auto* typed = array->mutable_bool_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* data = vec.GetData<bool>();
				for (size_t i = 0; i < chunk.size(); ++i) {
					typed->add_values(data[i]);
				}
			}
			break;
		}
		case DataTypeId::kPath: {
			auto* typed = array->mutable_path_array();
			typed->mutable_values()->Reserve(total_rows_);
			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const bool has_sel = vec.IsDictionary();
				const SelectionVector* sel_ptr =
				    has_sel ? &vec.sel() : nullptr;
				for (size_t i = 0; i < chunk.size(); ++i) {
					size_t idx = has_sel ? sel_ptr->GetIndex(i) : i;
					const auto& path = PathVector::GetPath(vec, idx);
					if (path.is_null()) {
						typed->add_values("");
					} else {
						std::ostringstream oss;
						oss << "{\"nodes\":[";
						auto nodes = path.nodes();
						for (size_t n = 0; n < nodes.size(); n++) {
							if (n > 0) oss << ",";
							oss << "{\"id\":" << nodes[n].vid() << "}";
						}
						oss << "],\"length\":" << path.length() << "}";
						typed->add_values(oss.str());
					}
				}
			}
			break;
		}
		case DataTypeId::kList: {
			auto* list_col = array->mutable_list_array();
			size_t current_offset = 0;
			DataTypeId elem_type = DataTypeId::kUnknown;

			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				if (elem_type == DataTypeId::kUnknown) {
					const auto& child = ListVector::GetChild(vec);
					elem_type = child.type_id();
				}
			}

			auto* inner = list_col->mutable_elements();
			list_col->mutable_offsets()->Reserve(total_rows_ + 1);

			for (auto& chunk : chunks_) {
				int col_idx = chunk.FindColumnByTag(tag);
				if (col_idx < 0) continue;
				const auto& vec = chunk.GetVector(col_idx);
				const auto* entries = ListVector::GetEntries(vec);
				const auto& child = ListVector::GetChild(vec);

				for (size_t i = 0; i < chunk.size(); ++i) {
					list_col->add_offsets(
					    static_cast<uint32_t>(current_offset));
					const auto& entry = entries[i];

					switch (elem_type) {
					case DataTypeId::kInt64: {
						auto* typed = inner->mutable_int64_array();
						const auto* data = child.GetData<int64_t>();
						for (size_t j = 0; j < entry.length; j++) {
							typed->add_values(data[entry.offset + j]);
						}
						break;
					}
					case DataTypeId::kInt32: {
						auto* typed = inner->mutable_int32_array();
						const auto* data = child.GetData<int32_t>();
						for (size_t j = 0; j < entry.length; j++) {
							typed->add_values(data[entry.offset + j]);
						}
						break;
					}
					case DataTypeId::kDouble: {
						auto* typed = inner->mutable_double_array();
						const auto* data = child.GetData<double>();
						for (size_t j = 0; j < entry.length; j++) {
							typed->add_values(data[entry.offset + j]);
						}
						break;
					}
					case DataTypeId::kVarchar: {
						auto* typed = inner->mutable_string_array();
						const auto* cdata =
						    StringVector::GetStringData(
						        const_cast<GraphVector&>(child));
						for (size_t j = 0; j < entry.length; j++) {
							typed->add_values(
							    cdata[entry.offset + j].GetString());
						}
						break;
					}
					default:
						break;
					}
					current_offset += entry.length;
				}
			}
			list_col->add_offsets(static_cast<uint32_t>(current_offset));
			break;
		}
		default:
			break;
		}
	}
}

}  // namespace neug::execution::vec
