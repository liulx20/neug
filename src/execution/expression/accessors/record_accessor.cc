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

#include "neug/execution/expression/accessors/record_accessor.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"

namespace neug {
namespace execution {

class BindedRecordAccessor : public RecordExprBase {
 public:
  BindedRecordAccessor(int tag, const DataType& type)
      : tag_(tag), type_(type) {}

  Value eval_record(const Context& ctx, size_t idx) const override {
    return ctx.get(tag_)->get_elem(idx);
  }

  const DataType& type() const override { return type_; }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto col = ctx.get(tag_);
    if (sel == nullptr) {
      return col;
    } else {
      return col->shuffle(*sel);
    }
  }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedRecordAccessor>(tag_, type_);
}

class BindedRecordVertexPropertyExpr : public RecordExprBase {
 public:
  BindedRecordVertexPropertyExpr(int tag, const IStorageInterface& storage,
                                 const std::string& property_name,
                                 const DataType& type)
      : tag_(tag), property_name_(property_name), type_(type) {
    const auto& storage_interface =
        dynamic_cast<const StorageReadInterface&>(storage);
    property_columns_.reserve(storage.schema().vertex_label_frontier());
    for (label_t label = 0; label < storage.schema().vertex_label_frontier();
         ++label) {
      if (!storage.schema().vertex_label_valid(label)) {
        continue;
      }
      property_columns_.emplace_back(
          storage_interface.GetVertexPropColumn(label, property_name_));
    }
  }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& vertex_val = ctx.get(tag_)->get_elem(idx);
    if (vertex_val.IsNull()) {
      return Value(type_);
    }
    vertex_t vertex = vertex_val.GetValue<vertex_t>();
    vid_t vid = static_cast<vid_t>(vertex.vid());
    label_t vlabel = static_cast<label_t>(vertex.label());

    auto column = property_columns_[vlabel];
    if (column == nullptr) {
      return Value(type_);  // return null value
    }

    return property_to_value(column->get(vid));
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    switch (type_.id()) {
#define TYPE_DISPATCHER(enum_val, cpp_type) \
  case DataTypeId::enum_val:                \
    return typed_eval_chunk<cpp_type>(ctx, sel);
      FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    default:
      LOG(WARNING) << "Unsupported data type for vertex property: "
                   << type_.ToString();
      return nullptr;
    }
  }

  const DataType& type() const override { return type_; }

 private:
  template <typename T>
  std::shared_ptr<IContextColumn> typed_eval_chunk(
      const Context& ctx, const select_vector_t* sel) const {
    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag_));
    const auto& labels = vertex_col->get_labels_set();
    using V =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    std::vector<const TypedRefColumn<V>*> typed_columns(
        property_columns_.size(), nullptr);
    for (auto label : labels) {
      auto column = dynamic_cast<const TypedRefColumn<V>*>(
          property_columns_[label].get());
      typed_columns[label] = column;
    }
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    ValueColumnBuilder<T> builder(row_num);
    if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
      const SLVertexColumn& sl_vertex_col =
          dynamic_cast<const SLVertexColumn&>(*vertex_col);
      auto col = typed_columns[sl_vertex_col.label()];
      if (sl_vertex_col.is_optional() || col == nullptr) {
        for (size_t i = 0; i < row_num; ++i) {
          size_t idx = (sel == nullptr) ? i : (*sel)[i];
          auto v = sl_vertex_col.get_vertex(idx);
          if (v.vid_ == std::numeric_limits<vid_t>::max() || col == nullptr) {
            builder.push_back_null();
          } else {
            if constexpr (std::is_same_v<T, std::string>) {
              builder.push_back_opt(std::string(col->get_view(v.vid_)));
            } else {
              builder.push_back_opt(col->get_view(v.vid_));
            }
          }
        }
      } else {
        for (size_t i = 0; i < row_num; ++i) {
          size_t idx = (sel == nullptr) ? i : (*sel)[i];
          auto v = sl_vertex_col.get_vertex(idx);
          if constexpr (std::is_same_v<T, std::string>) {
            builder.push_back_opt(std::string(col->get_view(v.vid_)));
          } else {
            builder.push_back_opt(col->get_view(v.vid_));
          }
        }
      }
    } else if (vertex_col->vertex_column_type() ==
               VertexColumnType::kMultiple) {
      const MLVertexColumn& ml_vertex_col =
          dynamic_cast<const MLVertexColumn&>(*vertex_col);
      bool is_optional = ml_vertex_col.is_optional();
      if (!is_optional) {
        for (auto label : ml_vertex_col.get_labels_set()) {
          if (typed_columns[label] == nullptr) {
            is_optional = true;
            break;
          }
        }
      }
      if (is_optional) {
        for (size_t i = 0; i < row_num; ++i) {
          size_t idx = (sel == nullptr) ? i : (*sel)[i];
          auto v = ml_vertex_col.get_vertex(idx);
          if (v.vid_ == std::numeric_limits<vid_t>::max() ||
              typed_columns[v.label_] == nullptr) {
            builder.push_back_null();
          } else {
            if constexpr (std::is_same_v<T, std::string>) {
              builder.push_back_opt(
                  std::string(typed_columns[v.label_]->get_view(v.vid_)));
            } else {
              builder.push_back_opt(typed_columns[v.label_]->get_view(v.vid_));
            }
          }
        }
      } else {
        for (size_t i = 0; i < row_num; ++i) {
          size_t idx = (sel == nullptr) ? i : (*sel)[i];
          auto v = ml_vertex_col.get_vertex(idx);
          if constexpr (std::is_same_v<T, std::string>) {
            builder.push_back_opt(
                std::string(typed_columns[v.label_]->get_view(v.vid_)));
          } else {
            builder.push_back_opt(typed_columns[v.label_]->get_view(v.vid_));
          }
        }
      }
    } else {
      const auto& ms_vertex_col =
          dynamic_cast<const MSVertexColumn&>(*vertex_col);
      bool is_optional = ms_vertex_col.is_optional();
      if (!is_optional) {
        for (auto label : ms_vertex_col.get_labels_set()) {
          if (typed_columns[label] == nullptr) {
            is_optional = true;
            break;
          }
        }
      }
      if (is_optional) {
        for (size_t i = 0; i < row_num; ++i) {
          size_t idx = (sel == nullptr) ? i : (*sel)[i];
          auto v = ms_vertex_col.get_vertex(idx);
          if (v.vid_ == std::numeric_limits<vid_t>::max() ||
              typed_columns[v.label_] == nullptr) {
            builder.push_back_null();
          } else {
            if constexpr (std::is_same_v<T, std::string>) {
              builder.push_back_opt(
                  std::string(typed_columns[v.label_]->get_view(v.vid_)));
            } else {
              builder.push_back_opt(typed_columns[v.label_]->get_view(v.vid_));
            }
          }
        }
      } else {
        for (size_t i = 0; i < row_num; ++i) {
          size_t idx = (sel == nullptr) ? i : (*sel)[i];
          auto v = ms_vertex_col.get_vertex(idx);
          if constexpr (std::is_same_v<T, std::string>) {
            builder.push_back_opt(
                std::string(typed_columns[v.label_]->get_view(v.vid_)));
          } else {
            builder.push_back_opt(typed_columns[v.label_]->get_view(v.vid_));
          }
        }
      }
    }
    return builder.finish();
  }

  int tag_;
  std::string property_name_;
  DataType type_;
  std::vector<std::shared_ptr<RefColumnBase>> property_columns_;
};

class BindedRecordVertexLabelExpr : public RecordExprBase {
 public:
  BindedRecordVertexLabelExpr(int tag, const Schema& schema)
      : tag_(tag), schema_(schema), type_(DataTypeId::kVarchar) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value vertex_val = ctx.get(tag_)->get_elem(idx);
    if (vertex_val.IsNull()) {
      return Value(type_);
    }
    vertex_t vertex = vertex_val.GetValue<vertex_t>();
    return Value::STRING(schema_.get_vertex_label_name(vertex.label()));
  }

  const DataType& type() const override { return type_; }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag_));
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();

    ValueColumnBuilder<std::string> builder(row_num);

    foreach_vertex(
        *vertex_col,
        [&](size_t, label_t label, vid_t vid) {
          if (vid == std::numeric_limits<vid_t>::max()) {
            builder.push_back_null();
          } else {
            builder.push_back_opt(schema_.get_vertex_label_name(label));
          }
        },
        sel);
    return builder.finish();
  }

 private:
  int tag_;
  const Schema& schema_;
  DataType type_;
};

class BindedRecordVertexGIdExpr : public RecordExprBase {
 public:
  BindedRecordVertexGIdExpr(int tag) : tag_(tag), type_(DataTypeId::kInt64) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value vertex_val = ctx.get(tag_)->get_elem(idx);
    if (vertex_val.IsNull()) {
      return Value(type_);
    }
    vertex_t vertex = vertex_val.GetValue<vertex_t>();
    int64_t gid = encode_unique_vertex_id(static_cast<label_t>(vertex.label()),
                                          static_cast<vid_t>(vertex.vid()));
    return Value::CreateValue<int64_t>(gid);
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag_));
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    ValueColumnBuilder<int64_t> builder(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      size_t idx = (sel == nullptr) ? i : (*sel)[i];
      auto v = vertex_col->get_vertex(idx);
      if (v.vid_ == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        int64_t gid = encode_unique_vertex_id(static_cast<label_t>(v.label_),
                                              static_cast<vid_t>(v.vid_));
        builder.push_back_opt(gid);
      }
    }
    return builder.finish();
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordVertexAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  switch (access_type_) {
  case GraphAccessType::kProperty:
    return std::make_unique<BindedRecordVertexPropertyExpr>(
        tag_, *storage, property_name_, data_type_);
  case GraphAccessType::kLabel:
    return std::make_unique<BindedRecordVertexLabelExpr>(tag_,
                                                         storage->schema());
  case GraphAccessType::kGid:
    return std::make_unique<BindedRecordVertexGIdExpr>(tag_);
  default:
    LOG(FATAL) << "Unknown RecordVertexAccessor GraphAccessType: "
               << static_cast<int>(access_type_);
    break;
  }
  return nullptr;
}

class BindedEdgeRecordPropertyExpr : public RecordExprBase {
 public:
  BindedEdgeRecordPropertyExpr(int tag, const IStorageInterface& storage,
                               const std::string& prop_name,
                               const DataType& type)
      : tag_(tag), type_(type) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(storage);
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
  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& edge_val = ctx.get(tag_)->get_elem(idx);
    if (edge_val.IsNull()) {
      return Value(type_);
    }
    edge_t edge = edge_val.GetValue<edge_t>();
    LabelTriplet label = edge.label;
    auto it = edge_accessors_.find(label);
    if (it == edge_accessors_.end()) {
      return Value(type_);  // return null value
    }
    auto accessor = it->second;
    auto prop = accessor.get_data_from_ptr(edge.prop);
    return property_to_value(prop);
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    switch (type_.id()) {
#define TYPE_DISPATCHER(enum_val, cpp_type) \
  case DataTypeId::enum_val:                \
    return typed_eval_chunk<cpp_type>(ctx, sel);
      FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    default:
      LOG(WARNING) << "Unsupported data type for edge property: "
                   << type_.ToString();
      return nullptr;
    }
  }

  const DataType& type() const override { return type_; }

 private:
  template <typename T>
  std::shared_ptr<IContextColumn> typed_eval_chunk(
      const Context& ctx, const select_vector_t* sel) const {
    auto edge_col = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag_));
    const auto& labels = edge_col->get_labels();
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    ValueColumnBuilder<T> builder(row_num);
    if constexpr (std::is_same_v<T, std::string>) {
      foreach_edge(
          *edge_col,
          [&](size_t, const LabelTriplet& label, Direction, vid_t src,
              vid_t dst, const void* prop_ptr) {
            if (src == std::numeric_limits<vid_t>::max() ||
                dst == std::numeric_limits<vid_t>::max()) {
              builder.push_back_null();
              return;
            }
            auto it = edge_accessors_.find(label);
            if (it == edge_accessors_.end()) {
              builder.push_back_null();
            } else {
              auto accessor = it->second;
              auto prop =
                  accessor.get_typed_data_from_ptr<std::string_view>(prop_ptr);
              // Assuming the property is stored as a null-terminated string
              builder.push_back_opt(std::string(prop));
            }
          },
          sel);
      return builder.finish();
    } else {
      foreach_edge(
          *edge_col,
          [&](size_t, const LabelTriplet& label, Direction, vid_t src,
              vid_t dst, const void* prop_ptr) {
            if (src == std::numeric_limits<vid_t>::max() ||
                dst == std::numeric_limits<vid_t>::max()) {
              builder.push_back_null();
              return;
            }
            auto it = edge_accessors_.find(label);
            if (it == edge_accessors_.end()) {
              builder.push_back_null();
            } else {
              auto accessor = it->second;
              auto prop =
                  accessor.template get_typed_data_from_ptr<T>(prop_ptr);
              // push back the property value to the builder
              builder.push_back_opt(prop);
            }
          },
          sel);
      return builder.finish();
    }
  }

  int tag_;
  DataType type_;
  std::map<LabelTriplet, EdgeDataAccessor> edge_accessors_;
};

class BindedEdgeRecordLabelExpr : public RecordExprBase {
 public:
  BindedEdgeRecordLabelExpr(int tag, const Schema& schema)
      : tag_(tag), schema_(schema), type_(DataTypeId::kVarchar) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value edge_val = ctx.get(tag_)->get_elem(idx);
    if (edge_val.IsNull()) {
      return Value(type_);
    }
    edge_t edge = edge_val.GetValue<edge_t>();
    return Value::STRING(schema_.get_edge_label_name(edge.label.edge_label));
  }

  const DataType& type() const override { return type_; }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto edge_col = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag_));
    ValueColumnBuilder<std::string> builder(ctx.row_num());
    foreach_edge(
        *edge_col,
        [&](size_t, const LabelTriplet& label, Direction, vid_t src, vid_t dst,
            const void*) {
          if (src == std::numeric_limits<vid_t>::max() ||
              dst == std::numeric_limits<vid_t>::max()) {
            builder.push_back_null();
          } else {
            builder.push_back_opt(
                schema_.get_edge_label_name(label.edge_label));
          }
        },
        sel);
    return builder.finish();
  }

 private:
  int tag_;
  const Schema& schema_;
  DataType type_;
};

class BindedEdgeRecordGIdExpr : public RecordExprBase {
 public:
  BindedEdgeRecordGIdExpr(int tag) : tag_(tag), type_(DataTypeId::kInt64) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& edge_val = ctx.get(tag_)->get_elem(idx);
    if (edge_val.IsNull()) {
      return Value(type_);
    }
    edge_t edge = edge_val.GetValue<edge_t>();
    auto label = generate_edge_label_id(
        edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
    int64_t gid = encode_unique_edge_id(label, static_cast<vid_t>(edge.src),
                                        static_cast<vid_t>(edge.dst));
    return Value::CreateValue<int64_t>(gid);
  }

  const DataType& type() const override { return type_; }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto edge_col = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag_));
    ValueColumnBuilder<int64_t> builder(ctx.row_num());
    foreach_edge(
        *edge_col,
        [&](size_t, const LabelTriplet& label, Direction, vid_t src, vid_t dst,
            const void*) {
          if (src == std::numeric_limits<vid_t>::max() ||
              dst == std::numeric_limits<vid_t>::max()) {
            builder.push_back_null();
          } else {
            auto edge_label_id = generate_edge_label_id(
                label.src_label, label.dst_label, label.edge_label);
            int64_t gid = encode_unique_edge_id(edge_label_id, src, dst);
            builder.push_back_opt(gid);
          }
        },
        sel);
    return builder.finish();
  }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordEdgeAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  switch (access_type_) {
  case GraphAccessType::kProperty:
    return std::make_unique<BindedEdgeRecordPropertyExpr>(
        tag_, *storage, property_name_, data_type_);
  case GraphAccessType::kLabel:
    return std::make_unique<BindedEdgeRecordLabelExpr>(tag_, storage->schema());
  case GraphAccessType::kGid:
    return std::make_unique<BindedEdgeRecordGIdExpr>(tag_);
  default:
    LOG(FATAL) << "Unknown RecordEdgeAccessor GraphAccessType: "
               << static_cast<int>(access_type_);
    break;
  }
  return nullptr;
}

class BindedRecordPathLengthExpr : public RecordExprBase {
 public:
  BindedRecordPathLengthExpr(int tag) : tag_(tag), type_(DataTypeId::kInt64) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = ctx.get(tag_)->get_elem(idx);
    if (path_val.IsNull()) {
      return Value(type_);
    }
    const auto& path = PathValue::Get(path_val);
    return Value::CreateValue<int64_t>(static_cast<int64_t>(path.length()));
  }

  const DataType& type() const override { return type_; }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto path_col = std::dynamic_pointer_cast<PathColumn>(ctx.get(tag_));
    ValueColumnBuilder<int64_t> builder(ctx.row_num());
    path_col->foreach_path(
        [&](size_t, const Path& path) {
          if (path.is_null()) {
            builder.push_back_null();
          } else {
            builder.push_back_opt(static_cast<int64_t>(path.length()));
          }
        },
        sel);
    return builder.finish();
  }

 private:
  int tag_;
  DataType type_;
};

class BindedPathWeightExpr : public RecordExprBase {
 public:
  BindedPathWeightExpr(int tag) : tag_(tag), type_(DataTypeId::kDouble) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = ctx.get(tag_)->get_elem(idx);
    if (path_val.IsNull()) {
      return Value(type_);
    }
    const auto& path = PathValue::Get(path_val);
    return Value::CreateValue<double>(path.get_weight());
  }

  const DataType& type() const override { return type_; }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto path_col = std::dynamic_pointer_cast<PathColumn>(ctx.get(tag_));
    ValueColumnBuilder<double> builder(ctx.row_num());
    path_col->foreach_path(
        [&](size_t, const Path& path) {
          if (path.is_null()) {
            builder.push_back_null();
          } else {
            builder.push_back_opt(path.get_weight());
          }
        },
        sel);
    return builder.finish();
  }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordPathAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  if (property_ == "length") {
    return std::make_unique<BindedRecordPathLengthExpr>(tag_);
  } else if (property_ == "cost") {
    return std::make_unique<BindedPathWeightExpr>(tag_);
  }
  LOG(FATAL) << "Unknown RecordPathAccessor property: " << property_;
  return nullptr;
}

}  // namespace execution
}  // namespace neug