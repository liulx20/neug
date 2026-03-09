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

#include "neug/execution/expression/exprs/path_expr.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"

namespace neug {
namespace execution {
class BindedPathNodesExpr : public RecordExprBase {
 public:
  BindedPathNodesExpr(std::unique_ptr<BindedExprBase>&& path_expr)
      : path_expr_(std::move(path_expr)) {}
  const DataType& type() const override {
    static DataType list_type = DataType::List(DataType::VERTEX);
    return list_type;
  }

  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = path_expr_->Cast<RecordExprBase>().eval_record(ctx, idx);
    const Path& path = PathValue::Get(path_val);
    std::vector<VertexRecord> nodes = path.nodes();
    std::vector<Value> node_values;
    for (const auto& node : nodes) {
      node_values.push_back(Value::VERTEX(node));
    }
    return Value::LIST(DataType::List(DataType::VERTEX),
                       std::move(node_values));
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    ListColumnBuilder builder(DataType::VERTEX);
    auto path_col = std::dynamic_pointer_cast<PathColumn>(
        path_expr_->Cast<RecordExprBase>().eval_chunk(ctx, sel));
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    for (size_t i = 0; i < row_num; ++i) {
      auto path = path_col->get_path(i);
      if (path.is_null()) {
        builder.push_back_null();
        continue;
      } else {
        std::vector<VertexRecord> nodes = path.nodes();
        std::vector<Value> node_values;
        for (const auto& node : nodes) {
          node_values.push_back(Value::VERTEX(node));
        }
        builder.push_back_elem(Value::LIST(DataType::List(DataType::VERTEX),
                                           std::move(node_values)));
      }
    }
    return builder.finish();
  }

 private:
  std::unique_ptr<BindedExprBase> path_expr_;
};

std::unique_ptr<BindedExprBase> PathNodesExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedPathNodesExpr>(
      path_expr_->bind(storage, params));
}

class BindedPathRelationsExpr : public RecordExprBase {
 public:
  BindedPathRelationsExpr(std::unique_ptr<BindedExprBase>&& path_expr)
      : path_expr_(std::move(path_expr)) {}
  const DataType& type() const override {
    static DataType list_type = DataType::List(DataType::EDGE);
    return list_type;
  }

  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = path_expr_->Cast<RecordExprBase>().eval_record(ctx, idx);
    const Path& path = PathValue::Get(path_val);
    std::vector<edge_t> edges = path.relationships();
    std::vector<Value> edge_values;
    for (const auto& edge : edges) {
      edge_values.push_back(Value::EDGE(edge));
    }
    return Value::LIST(DataType::List(DataType::EDGE), std::move(edge_values));
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    ListColumnBuilder builder(DataType::List(DataType::EDGE));
    auto path_col = std::dynamic_pointer_cast<PathColumn>(
        path_expr_->Cast<RecordExprBase>().eval_chunk(ctx, sel));
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    for (size_t i = 0; i < row_num; ++i) {
      const auto& path = path_col->get_path(i);
      if (path.is_null()) {
        builder.push_back_null();
        continue;
      } else {
        std::vector<edge_t> edges = path.relationships();
        std::vector<Value> edge_values;
        for (const auto& edge : edges) {
          edge_values.push_back(Value::EDGE(edge));
        }
        builder.push_back_elem(Value::LIST(DataType::List(DataType::EDGE),
                                           std::move(edge_values)));
      }
    }
    return builder.finish();
  }

 private:
  std::unique_ptr<BindedExprBase> path_expr_;
};

std::unique_ptr<BindedExprBase> PathRelationsExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedPathRelationsExpr>(
      path_expr_->bind(storage, params));
}

class BindedPathVerticesPropsExpr : public RecordExprBase {
 public:
  BindedPathVerticesPropsExpr(const StorageReadInterface& graph, int tag,
                              const std::string& prop, const DataType& type)
      : graph_(graph),
        tag_(tag),
        prop_(prop),
        elem_type_(ListType::GetChildType(type)) {
    type_ = type;
  }
  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = ctx.get(tag_)->get_elem(idx);
    const Path& path = PathValue::Get(path_val);
    std::vector<vertex_t> vertices = path.nodes();
    std::vector<Value> prop_values;
    for (const auto& vertex : vertices) {
      const auto& prop_names = graph_.schema().get_vertex_property_names(
          static_cast<label_t>(vertex.label()));
      auto it = std::find(prop_names.begin(), prop_names.end(), prop_);
      if (it == prop_names.end()) {
        prop_values.push_back(Value(type_));  // null value
      } else {
        int prop_id = std::distance(prop_names.begin(), it);
        Property prop =
            graph_.GetVertexProperty(vertex.label(), vertex.vid(), prop_id);
        prop_values.emplace_back(property_to_value(prop));
      }
    }
    return Value::LIST(elem_type_, std::move(prop_values));
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    ListColumnBuilder builder(elem_type_);
    auto path_col = std::dynamic_pointer_cast<PathColumn>(ctx.get(tag_));
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    for (size_t i = 0; i < row_num; ++i) {
      size_t idx = (sel == nullptr) ? i : (*sel)[i];
      const auto& path = path_col->get_path(idx);
      if (path.is_null()) {
        builder.push_back_null();
        continue;
      } else {
        std::vector<vertex_t> vertices = path.nodes();
        std::vector<Value> prop_values;
        for (const auto& vertex : vertices) {
          const auto& prop_names = graph_.schema().get_vertex_property_names(
              static_cast<label_t>(vertex.label()));
          auto it = std::find(prop_names.begin(), prop_names.end(), prop_);
          if (it == prop_names.end()) {
            prop_values.push_back(Value(type_));  // null value
          } else {
            int prop_id = std::distance(prop_names.begin(), it);
            Property prop =
                graph_.GetVertexProperty(vertex.label(), vertex.vid(), prop_id);
            prop_values.emplace_back(property_to_value(prop));
          }
        }
        builder.push_back_elem(Value::LIST(elem_type_, std::move(prop_values)));
      }
    }
    return builder.finish();
  }

 private:
  const StorageReadInterface& graph_;
  int tag_;
  std::string prop_;
  DataType type_;
  DataType elem_type_;
};

class BindedPathEdgesPropsExpr : public RecordExprBase {
 public:
  BindedPathEdgesPropsExpr(const StorageReadInterface& graph, int tag,
                           const std::string& prop, const DataType& type)
      : graph_(graph),
        tag_(tag),
        prop_(prop),
        elem_type_(ListType::GetChildType(type)) {
    type_ = type;
  }
  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = ctx.get(tag_)->get_elem(idx);
    const Path& path = PathValue::Get(path_val);
    std::vector<edge_t> edges = path.relationships();
    std::vector<Value> prop_values;
    for (const auto& edge : edges) {
      const auto& prop_names = graph_.schema().get_edge_property_names(
          edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
      auto it = std::find(prop_names.begin(), prop_names.end(), prop_);
      if (it == prop_names.end()) {
        prop_values.push_back(Value(type_));  // null value
      } else {
        int prop_id = std::distance(prop_names.begin(), it);
        const auto& accessor = graph_.GetEdgeDataAccessor(
            edge.label.src_label, edge.label.dst_label, edge.label.edge_label,
            prop_id);

        prop_values.emplace_back(
            property_to_value(accessor.get_data_from_ptr(edge.prop)));
      }
    }
    return Value::LIST(elem_type_, std::move(prop_values));
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    ListColumnBuilder builder(elem_type_);
    auto path_col = std::dynamic_pointer_cast<PathColumn>(ctx.get(tag_));
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    for (size_t i = 0; i < row_num; ++i) {
      size_t idx = (sel == nullptr) ? i : (*sel)[i];
      const auto& path = path_col->get_path(idx);
      if (path.is_null()) {
        builder.push_back_null();
        continue;
      } else {
        std::vector<edge_t> edges = path.relationships();
        std::vector<Value> prop_values;
        for (const auto& edge : edges) {
          const auto& prop_names = graph_.schema().get_edge_property_names(
              edge.label.src_label, edge.label.dst_label,
              edge.label.edge_label);
          auto it = std::find(prop_names.begin(), prop_names.end(), prop_);
          if (it == prop_names.end()) {
            prop_values.push_back(Value(type_));  // null value
          } else {
            int prop_id = std::distance(prop_names.begin(), it);
            const auto& accessor = graph_.GetEdgeDataAccessor(
                edge.label.src_label, edge.label.dst_label,
                edge.label.edge_label, prop_id);

            prop_values.emplace_back(
                property_to_value(accessor.get_data_from_ptr(edge.prop)));
          }
        }
        builder.push_back_elem(Value::LIST(elem_type_, std::move(prop_values)));
      }
    }
    return builder.finish();
  }

 private:
  const StorageReadInterface& graph_;
  int tag_;
  std::string prop_;
  DataType type_;
  DataType elem_type_;
};

std::unique_ptr<BindedExprBase> PathPropsExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  const auto* graph = dynamic_cast<const StorageReadInterface*>(storage);
  if (extract_vertex_prop_) {
    return std::make_unique<BindedPathVerticesPropsExpr>(*graph, tag_, prop_,
                                                         type_);
  } else {
    return std::make_unique<BindedPathEdgesPropsExpr>(*graph, tag_, prop_,
                                                      type_);
  }
}

class BindedStartEndNodeExpr : public RecordExprBase {
 public:
  BindedStartEndNodeExpr(std::unique_ptr<BindedExprBase>&& edge_expr,
                         bool is_start)
      : edge_expr_(std::move(edge_expr)),
        is_start_(is_start),
        type_(DataType::VERTEX) {}
  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    Value edge_val = edge_expr_->Cast<RecordExprBase>().eval_record(ctx, idx);
    const auto& edge = edge_val.GetValue<edge_t>();
    return Value::VERTEX(is_start_ ? edge.start_node() : edge.end_node());
  }

  std::shared_ptr<IContextColumn> eval_chunk(
      const Context& ctx, const select_vector_t* sel) const override {
    auto edge_col = std::dynamic_pointer_cast<IEdgeColumn>(
        edge_expr_->Cast<RecordExprBase>().eval_chunk(ctx, sel));
    size_t row_num = (sel == nullptr) ? ctx.row_num() : sel->size();
    MLVertexColumnBuilder builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      if (!edge_col->has_value(i)) {
        builder.push_back_null();
      } else {
        edge_t edge = edge_col->get_edge(i);
        builder.push_back_opt(is_start_ ? edge.start_node() : edge.end_node());
      }
    }
    return builder.finish();
  }

 private:
  std::unique_ptr<BindedExprBase> edge_expr_;
  bool is_start_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> StartEndNodeExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedStartEndNodeExpr>(
      path_expr_->bind(storage, params), is_start_);
}
}  // namespace execution
}  // namespace neug