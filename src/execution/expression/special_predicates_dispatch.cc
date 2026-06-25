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

#include "neug/execution/expression/special_predicates.h"

#include "neug/execution/common/operators/retrieve/edge_expand.h"
#include "neug/execution/common/operators/retrieve/path_expand.h"
#include "neug/execution/common/operators/retrieve/scan.h"
#include "neug/execution/expression/predicates.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {

namespace {

struct ScanVertexSPOp {
  template <typename PRED_T>
  static neug::result<ContextChunk> eval_with_predicate(
      const PRED_T& pred, const IStorageInterface& graph, ContextChunk&& chunk,
      const ScanParams& params) {
    return Scan::scan_vertex<PRED_T>(std::move(chunk), graph, params, pred);
  }
};

struct ExpandVertexSPOp {
  template <typename PRED_T>
  static neug::result<ContextChunk> eval_with_predicate(
      const PRED_T& pred, const StorageReadInterface& graph,
      ContextChunk&& chunk, const EdgeExpandParams& params) {
    return EdgeExpand::expand_vertex<EdgeNbrPredicate<PRED_T>>(
        graph, std::move(chunk), params, EdgeNbrPredicate(pred));
  }
};

struct SSSPSPOp {
  template <typename PRED_T>
  static neug::result<ContextChunk> eval_with_predicate(
      const PRED_T& pred, const StorageReadInterface& graph,
      ContextChunk&& chunk, const ShortestPathParams& params) {
    return PathExpand::single_source_shortest_path<PRED_T>(
        graph, std::move(chunk), params, pred);
  }
};

struct OrderByLimitSPOp {
  template <typename PRED_T>
  static neug::result<ContextChunk> eval_with_predicate(
      const PRED_T& pred, const IStorageInterface& graph_interface,
      ContextChunk&& chunk, const ShortestPathParams& spp, int limit) {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return PathExpand::single_source_shortest_path_with_order_by_length_limit(
        graph, std::move(chunk), spp, pred, limit);
  }
};

template <typename OP_T, typename CMP_T, typename... Args>
neug::result<ContextChunk> dispatch_vertex_predicate_impl_cmp_type(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialPredicateConfig& config, const ParamsMap& params,
    const CMP_T& cmp_val, Args&&... args) {
  if (expected_labels.size() == 1) {
    label_t label = *expected_labels.begin();
    using GETTER_T = SLVertexPropertyGetter<typename CMP_T::data_t>;
    GETTER_T getter(graph, label, config.property_name);
    using PRED_T =
        VertexPropertyCmpPredicate<typename CMP_T::data_t, GETTER_T, CMP_T>;
    auto pred = PRED_T(getter, cmp_val);
    return OP_T::template eval_with_predicate<PRED_T>(
        pred, std::forward<Args>(args)...);
  } else {
    using GETTER_T = MLVertexPropertyGetter<typename CMP_T::data_t>;
    GETTER_T getter(graph, config.property_name);
    using PRED_T =
        VertexPropertyCmpPredicate<typename CMP_T::data_t, GETTER_T, CMP_T>;
    auto pred = PRED_T(getter, cmp_val);
    return OP_T::template eval_with_predicate<PRED_T>(
        pred, std::forward<Args>(args)...);
  }
}

template <typename OP_T, typename T, typename... Args>
neug::result<ContextChunk> dispatch_vertex_predicate_impl_typed(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialPredicateConfig& config, const ParamsMap& params,
    Args&&... args) {
  auto get_value = [&](const std::string& param_name) -> T {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return StringValue::Get(params.at(param_name));
    } else {
      return params.at(param_name).template GetValue<T>();
    }
  };
  if (config.ptype == SPPredicateType::kPropertyLT) {
    using CMP_T = LTCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyGT) {
    using CMP_T = GTCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyEQ) {
    using CMP_T = EQCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyLE) {
    using CMP_T = LECmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyGE) {
    using CMP_T = GECmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyNE) {
    using CMP_T = NECmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  } else if (config.ptype == SPPredicateType::kPropertyBetween) {
    using CMP_T = BetweenCmp<T>;
    auto cmp_val = CMP_T(get_value(config.param_names[0]),
                         get_value(config.param_names[1]));
    return dispatch_vertex_predicate_impl_cmp_type<OP_T, CMP_T>(
        graph, expected_labels, config, params, cmp_val,
        std::forward<Args>(args)...);
  }
  LOG(ERROR) << "Unsupported predicate type for special vertex predicate: "
             << static_cast<int>(config.ptype);
  RETURN_UNSUPPORTED_ERROR(
      "Unsupported predicate type for special vertex predicate");
}

template <typename OP_T, typename... Args>
neug::result<ContextChunk> dispatch_vertex_predicate_impl(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialPredicateConfig& config, const ParamsMap& params,
    Args&&... args) {
  switch (config.param_type) {
#define TYPE_DISPATCHER(enum_val, type)                      \
  case DataTypeId::enum_val:                                 \
    return dispatch_vertex_predicate_impl_typed<OP_T, type>( \
        graph, expected_labels, config, params, std::forward<Args>(args)...);
    TYPE_DISPATCHER(kInt32, int32_t)
    TYPE_DISPATCHER(kInt64, int64_t)
    TYPE_DISPATCHER(kTimestampMs, DateTime)
    TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER
  default:
    break;
  }
  LOG(ERROR) << "Unsupported param type for special vertex predicate: "
             << static_cast<int>(config.param_type);
  RETURN_UNSUPPORTED_ERROR(
      "Unsupported param type for special vertex predicate");
}

}  // namespace

neug::result<ContextChunk> dispatch_vertex_predicate(
    const IStorageInterface& graph, const std::set<label_t>& expected_labels,
    const SpecialPredicateConfig& config, const ParamsMap& query_params,
    ContextChunk&& chunk, const SpecialVertexOpParams& op) {
  if (op.params == nullptr) {
    RETURN_INVALID_ARGUMENT_ERROR("Special vertex op params is null");
  }
  switch (op.kind) {
  case SpecialVertexOpKind::kScan: {
    const auto& params = *static_cast<const ScanParams*>(op.params);
    return dispatch_vertex_predicate_impl<ScanVertexSPOp>(
        graph, expected_labels, config, query_params, graph, std::move(chunk),
        params);
  }
  case SpecialVertexOpKind::kEdgeExpand: {
    const auto& storage_graph =
        dynamic_cast<const StorageReadInterface&>(graph);
    const auto& params = *static_cast<const EdgeExpandParams*>(op.params);
    return dispatch_vertex_predicate_impl<ExpandVertexSPOp>(
        graph, expected_labels, config, query_params, storage_graph,
        std::move(chunk), params);
  }
  case SpecialVertexOpKind::kShortestPath: {
    const auto& storage_graph =
        dynamic_cast<const StorageReadInterface&>(graph);
    const auto& params = *static_cast<const ShortestPathParams*>(op.params);
    return dispatch_vertex_predicate_impl<SSSPSPOp>(
        graph, expected_labels, config, query_params, storage_graph,
        std::move(chunk), params);
  }
  case SpecialVertexOpKind::kShortestPathOrderByLimit: {
    const auto& spp = *static_cast<const ShortestPathParams*>(op.params);
    return dispatch_vertex_predicate_impl<OrderByLimitSPOp>(
        graph, expected_labels, config, query_params, graph, std::move(chunk),
        spp, op.limit);
  }
  default:
    break;
  }
  LOG(ERROR) << "Unsupported special vertex op kind: "
             << static_cast<int>(op.kind);
  RETURN_UNSUPPORTED_ERROR("Unsupported special vertex op kind");
}

}  // namespace execution
}  // namespace neug
