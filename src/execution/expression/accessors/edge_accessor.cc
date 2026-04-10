/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/execution/expression/accessors/edge_accessor.h"

namespace neug {
namespace execution {

class BindedEdgePropertyAccessor : public EdgeExprBase {
 public:
  BindedEdgePropertyAccessor(const StorageReadInterface& graph,
                             const std::string& prop_name, const DataType& type)
      : type_(type) {
    label_t edge_label_num = graph.schema().edge_label_frontier();
    label_t vertex_label_num = graph.schema().vertex_label_frontier();
    for (label_t src_label = 0; src_label < vertex_label_num; ++src_label) {
      if (!graph.schema().vertex_label_valid(src_label)) {
        continue;
      }
      for (label_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
        if (!graph.schema().vertex_label_valid(dst_label)) {
          continue;
        }
        for (label_t edge_label = 0; edge_label < edge_label_num;
             ++edge_label) {
          if (!graph.schema().exist(src_label, dst_label, edge_label)) {
            continue;
          }
          const std::vector<std::string>& names =
              graph.schema().get_edge_property_names(src_label, dst_label,
                                                     edge_label);
          for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == prop_name) {
              LabelTriplet label{src_label, dst_label, edge_label};
              edge_accessors_[label] = graph.GetEdgeDataAccessor(
                  src_label, dst_label, edge_label, i);
              break;
            }
          }
        }
      }
    }
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    auto it = edge_accessors_.find(label);
    if (it == edge_accessors_.end()) {
      return Value(type_);  // return null value
    }
    auto accessor = it->second;
    auto prop = accessor.get_data_from_ptr(data_ptr);
    return property_to_value(prop);
  }

  bool typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                       const void* data_ptr, void* out_value) const override {
    auto it = edge_accessors_.find(label);
    if (it == edge_accessors_.end()) {
      return true;  // null
    }
    const auto& accessor = it->second;
    switch (type_.id()) {
    case DataTypeId::kBoolean:
      *static_cast<bool*>(out_value) =
          accessor.get_typed_data_from_ptr<bool>(data_ptr);
      return false;
    case DataTypeId::kInt32:
      *static_cast<int32_t*>(out_value) =
          accessor.get_typed_data_from_ptr<int32_t>(data_ptr);
      return false;
    case DataTypeId::kInt64:
      *static_cast<int64_t*>(out_value) =
          accessor.get_typed_data_from_ptr<int64_t>(data_ptr);
      return false;
    case DataTypeId::kUInt32:
      *static_cast<uint32_t*>(out_value) =
          accessor.get_typed_data_from_ptr<uint32_t>(data_ptr);
      return false;
    case DataTypeId::kUInt64:
      *static_cast<uint64_t*>(out_value) =
          accessor.get_typed_data_from_ptr<uint64_t>(data_ptr);
      return false;
    case DataTypeId::kFloat:
      *static_cast<float*>(out_value) =
          accessor.get_typed_data_from_ptr<float>(data_ptr);
      return false;
    case DataTypeId::kDouble:
      *static_cast<double*>(out_value) =
          accessor.get_typed_data_from_ptr<double>(data_ptr);
      return false;
    default:
      break;
    }
    // Fallback: go through Property → Value path
    return EdgeExprBase::typed_eval_edge(label, src, dst, data_ptr, out_value);
  }

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::map<LabelTriplet, EdgeDataAccessor> edge_accessors_;
};

class BindedEdgeLabelAccessor : public EdgeExprBase {
 public:
  explicit BindedEdgeLabelAccessor(const IStorageInterface& graph)
      : schema_(graph.schema()) {
    type_ = DataType(DataTypeId::kVarchar);
  }

  std::string typed_eval_edge(const LabelTriplet& label, vid_t, vid_t,
                              const void*) const {
    return schema_.get_edge_label_name(label.edge_label);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    return Value::STRING(schema_.get_edge_label_name(label.edge_label));
  }
  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  const Schema& schema_;
};

std::unique_ptr<BindedExprBase> EdgeAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  switch (access_type_) {
  case GraphAccessType::kProperty: {
    auto storage_read = dynamic_cast<const StorageReadInterface*>(storage);
    return std::make_unique<BindedEdgePropertyAccessor>(*storage_read,
                                                        property_name_, type_);
  }
  case GraphAccessType::kLabel: {
    return std::make_unique<BindedEdgeLabelAccessor>(*storage);
  }
  default:
    LOG(FATAL) << "Unknown GraphAccessorType: "
               << static_cast<int>(access_type_);
    break;
  }
  return nullptr;
}
}  // namespace execution
}  // namespace neug