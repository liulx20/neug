
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
#pragma once

#include <cstdint>

#include "neug/execution/common/context_chunk.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace common {
class Expression;
}

namespace neug {

namespace execution {

bool is_pk_oid_exact_check(const neug::Schema& schema, label_t label,
                           const common::Expression& expr);

enum class SPPredicateType {
  kPropertyGT,
  kPropertyLT,
  kPropertyLE,
  kPropertyGE,
  kPropertyEQ,
  kPropertyNE,
  kPropertyBetween,
  kWithIn,
  kUnknown
};

SPPredicateType parse_sp_pred(const common::Expression& expr);

template <typename T>
class SLEdgePropertyGetter {
 public:
  SLEdgePropertyGetter(const StorageReadInterface& graph,
                       const std::vector<LabelTriplet>& labels,
                       const std::string& property_name) {
    CHECK_EQ(labels.size(), 1);
    int prop_id = 0;
    for (auto& name : graph.schema().get_edge_property_names(
             labels[0].src_label, labels[0].dst_label, labels[0].edge_label)) {
      if (name == property_name) {
        break;
      }
      ++prop_id;
    }
    ed_accessor_ =
        graph.GetEdgeDataAccessor(labels[0].src_label, labels[0].dst_label,
                                  labels[0].edge_label, prop_id);
  }
  ~SLEdgePropertyGetter() = default;

  inline T get(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
               label_t edge_label, Direction dir, const void* data_ptr) const {
    return ed_accessor_.get_typed_data_from_ptr<T>(data_ptr);
  }

 private:
  EdgeDataAccessor ed_accessor_;
};

template <typename T>
class MLEdgePropertyGetter {
 public:
  MLEdgePropertyGetter(const IStorageInterface& gi,
                       const std::vector<LabelTriplet>& labels,
                       const std::string& property_name) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(gi);
    // property_name -> prop_id
    for (const auto& lt : labels) {
      int prop_id = 0;
      for (auto& name : graph.schema().get_edge_property_names(
               lt.src_label, lt.dst_label, lt.edge_label)) {
        if (name == property_name) {
          break;
        }
        ++prop_id;
      }
      ed_accessors_.emplace(
          lt, graph.GetEdgeDataAccessor(lt.src_label, lt.dst_label,
                                        lt.edge_label, prop_id));
    }
  }
  ~MLEdgePropertyGetter() = default;

  inline T get(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
               label_t edge_label, Direction dir, const void* data_ptr) const {
    auto label_triplet = (dir == Direction::kOut)
                             ? LabelTriplet{v_label, nbr_label, edge_label}
                             : LabelTriplet{nbr_label, v_label, edge_label};
    return ed_accessors_.at(label_triplet)
        .template get_typed_data_from_ptr<T>(data_ptr);
  }

 private:
  std::map<LabelTriplet, EdgeDataAccessor> ed_accessors_;
};

template <typename T>
class SLVertexPropertyGetter {
 public:
  SLVertexPropertyGetter(const IStorageInterface& graph, label_t label,
                         const std::string& property_name) {
    column_ =
        std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<T>>(
            dynamic_cast<const StorageReadInterface&>(graph)
                .GetVertexPropColumn(label, property_name));
  }
  ~SLVertexPropertyGetter() = default;

  inline T get(label_t label, vid_t v) const { return column_->get_view(v); }

 private:
  std::shared_ptr<StorageReadInterface::vertex_column_t<T>> column_;
};

template <typename T>
class MLVertexPropertyGetter {
 public:
  MLVertexPropertyGetter(const IStorageInterface& gi,
                         const std::string& property_name) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(gi);
    for (label_t i = 0; i < graph.schema().vertex_label_frontier(); ++i) {
      if (!graph.schema().is_vertex_label_valid(i)) {
        continue;
      }
      columns_.emplace_back(
          std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<T>>(
              graph.GetVertexPropColumn(i, property_name)));
    }
  }
  ~MLVertexPropertyGetter() = default;

  inline T get(label_t label, vid_t v) const {
    return columns_[label]->get_view(v);
  }

 private:
  std::vector<std::shared_ptr<StorageReadInterface::vertex_column_t<T>>>
      columns_;
};

template <typename T>
class GTCmp {
 public:
  using data_t = T;

  GTCmp() = default;
  explicit GTCmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return target_ < v; }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class LTCmp {
 public:
  using data_t = T;

  LTCmp() = default;
  explicit LTCmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return v < target_; }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class EQCmp {
 public:
  using data_t = T;

  EQCmp() = default;
  explicit EQCmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return target_ == v; }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class GECmp {
 public:
  using data_t = T;

  GECmp() = default;
  explicit GECmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return !(v < target_); }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class LECmp {
 public:
  using data_t = T;

  LECmp() = default;
  explicit LECmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return !(target_ < v); }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class NECmp {
 public:
  using data_t = T;

  NECmp() = default;
  explicit NECmp(const T& target) : target_(target) {}
  bool operator()(const T& v) const { return !(target_ == v); }

  void reset(const std::vector<T>& targets) { target_ = targets[0]; }

 private:
  T target_;
};

template <typename T>
class BetweenCmp {
 public:
  using data_t = T;

  BetweenCmp() = default;
  BetweenCmp(const T& from, const T& to) : from_(from), to_(to) {}

  bool operator()(const T& v) const { return (v < to_) && !(v < from_); }

  void reset(const std::vector<T>& targets) {
    from_ = targets[0];
    to_ = targets[1];
  }

 private:
  T from_;
  T to_;
};

template <typename T, typename GETTER_T, typename CMP_T>
class EdgePropertyCmpPredicate {
 public:
  using data_t = T;
  static constexpr bool is_dummy = false;

  EdgePropertyCmpPredicate(const GETTER_T& getter, const CMP_T& cmp)
      : getter_(getter), cmp_(cmp) {}
  ~EdgePropertyCmpPredicate() = default;

  bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                  label_t edge_label, Direction dir,
                  const void* data_ptr) const {
    T val = getter_.get(v_label, v, nbr_label, nbr, edge_label, dir, data_ptr);
    return cmp_(val);
  }

 private:
  GETTER_T getter_;
  CMP_T cmp_;
};

template <typename T, typename GETTER_T, typename CMP_T>
class VertexPropertyCmpPredicate {
 public:
  using data_t = T;
  static constexpr bool is_dummy = false;

  VertexPropertyCmpPredicate(const GETTER_T& getter, const CMP_T& cmp)
      : getter_(getter), cmp_(cmp) {}

  bool operator()(label_t label, vid_t v) const {
    T val = getter_.get(label, v);
    return cmp_(val);
  }

 private:
  GETTER_T getter_;
  CMP_T cmp_;
};
struct SpecialPredicateConfig {
  std::string property_name;
  SPPredicateType ptype;
  std::vector<std::string> param_names;
  DataTypeId param_type;
};

bool is_special_edge_predicate(const Schema& schema,
                               const std::vector<LabelTriplet>& labels,
                               const common::Expression& expr,
                               SpecialPredicateConfig& config);

bool is_special_vertex_predicate(const Schema& schema,
                                 const std::vector<label_t>& labels,
                                 const common::Expression& expr,
                                 SpecialPredicateConfig& config);

enum class SpecialVertexOpKind {
  kScan,
  kEdgeExpand,
  kShortestPath,
  kShortestPathOrderByLimit,
};

struct SpecialVertexOpParams {
  SpecialVertexOpKind kind;
  const void* params = nullptr;
  int32_t limit = 0;
};

neug::result<ContextChunk> dispatch_vertex_predicate(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialPredicateConfig& config, const ParamsMap& query_params,
    ContextChunk&& chunk, const SpecialVertexOpParams& op);

}  // namespace execution

}  // namespace neug
