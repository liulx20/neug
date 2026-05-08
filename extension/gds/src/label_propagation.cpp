/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "label_propagation.h"
#include <string>
#include "parallel_utils.h"

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/predicates.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace gds {
struct LabelPropagationInput : public function::CallFuncInputBase {
  ~LabelPropagationInput() = default;

  void parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    if (subgraph.vertex_entries_size() < 1) {
      throw std::runtime_error(
          "LabelPropagation requires exactly one vertex label.");
    }
    const auto& vertex_entry = subgraph.vertex_entries(0);
    if (vertex_entry.label_id() < 0) {
      throw std::runtime_error("Vertex label ID must be non-negative.");
    }
    vertex_label = static_cast<label_t>(vertex_entry.label_id());
    if (vertex_entry.has_predicate()) {
      vertex_pred = execution::parse_expression(
          vertex_entry.predicate(), ctx_meta, execution::VarType::kVertex);
    } else {
      vertex_pred = nullptr;
    }

    if (subgraph.edge_entries_size() < 1) {
      throw std::runtime_error(
          "LabelPropagation requires exactly one edge label.");
    }
    const auto& edge_entry = subgraph.edge_entries(0);
    if (edge_entry.src_label_id() < 0 || edge_entry.dst_label_id() < 0 ||
        edge_entry.edge_label_id() < 0) {
      throw std::runtime_error(
          "Source vertex label ID, destination vertex label ID and edge "
          "label ID must be non-negative.");
    }
    edge_triplet = execution::LabelTriplet(edge_entry.src_label_id(),
                                           edge_entry.dst_label_id(),
                                           edge_entry.edge_label_id());
    if (edge_entry.has_predicate()) {
      edge_pred = execution::parse_expression(edge_entry.predicate(), ctx_meta,
                                              execution::VarType::kEdge);
    } else {
      edge_pred = nullptr;
    }
  }

  label_t vertex_label;
  std::unique_ptr<execution::ExprBase> vertex_pred;
  execution::LabelTriplet edge_triplet;
  std::unique_ptr<execution::ExprBase> edge_pred;
  int32_t max_iterations;
  int32_t node_alias, label_alias;
  int32_t concurrency;
};

std::unique_ptr<function::CallFuncInputBase> LabelPropagationFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();
  auto input = std::make_unique<LabelPropagationInput>();
  input->parse_subgraph(subgraph, ctx_meta);
  const auto& max_iterations_it = options.find("max_iterations");
  if (max_iterations_it != options.end()) {
    try {
      input->max_iterations = std::stoi(max_iterations_it->second);
    } catch (const std::exception& e) {
      throw std::runtime_error("Invalid value for max_iterations: " +
                               max_iterations_it->second);
    }
  } else {
    input->max_iterations = 5;
  }
  const auto& concurrency = options.find("concurrency");
  if (concurrency != options.end()) {
    try {
      input->concurrency = std::stoi(concurrency->second);
    } catch (const std::exception& e) {
      throw std::runtime_error("Invalid value for concurrency: " +
                               concurrency->second);
    }
  } else {
    input->concurrency = 1;
  }

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->label_alias = plan.plan(op_idx).meta_data(1).alias();
  LOG(INFO) << "LabelPropagationFunction bind with max_iterations = "
            << input->max_iterations;
  return input;
}

struct LabelPropagation {
  LabelPropagation(const StorageReadInterface& graph, label_t vertex_label,
                   const execution::LabelTriplet& edge_triplet,
                   int max_iterations, int concurrency)
      : graph(graph),
        vertex_label(vertex_label),
        edge_triplet(edge_triplet),
        max_iterations(max_iterations),
        concurrency_(concurrency) {}
  template <typename PRED_T>
  void init_communities(const PRED_T& vertex_pred_fn) {
    auto vertex_set = graph.GetVertexSet(vertex_label);
    auto begin = std::chrono::high_resolution_clock::now();
    community.resize(vertex_set.size(), std::numeric_limits<vid_t>::max());
    next_community.resize(vertex_set.size(), std::numeric_limits<vid_t>::max());
    vertices.reserve(vertex_set.size());
    for (vid_t v : vertex_set) {
      if (vertex_pred_fn(vertex_label, v)) {
        community[v] = v;
        next_community[v] = v;
        vertices.push_back(v);
      }
    }
    LOG(INFO) << "Initialized communities for " << vertices.size()
              << " vertices in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::high_resolution_clock::now() - begin)
                     .count()
              << " ms.";
  }

  template <typename EDGE_PRED_T>
  void propagate_labels(const EDGE_PRED_T& edge_pred_fn) {
    const auto& ie_view = graph.GetGenericIncomingGraphView(
        edge_triplet.dst_label, edge_triplet.src_label,
        edge_triplet.edge_label);
    const auto& oe_view = graph.GetGenericOutgoingGraphView(
        edge_triplet.src_label, edge_triplet.dst_label,
        edge_triplet.edge_label);
    for (int iteration = 0; iteration < max_iterations; ++iteration) {
      std::atomic<bool> updated(false);
      auto begin = std::chrono::high_resolution_clock::now();
      ParallelUtils::parallel_for(
          vertices.data(), vertices.size(),
          [&](vid_t i) {
            vid_t dst_vid = vertices[i];
            auto edges = ie_view.get_edges(dst_vid);
            std::unordered_map<vid_t, int32_t> neighbor_communities;
            for (auto it = edges.begin(); it != edges.end(); ++it) {
              const vid_t src_vid = it.get_vertex();
              if (community[src_vid] != std::numeric_limits<vid_t>::max() &&
                  edge_pred_fn(edge_triplet, src_vid, dst_vid,
                               it.get_data_ptr())) {
                neighbor_communities[community[src_vid]]++;
              }
            }
            auto oe_edges = oe_view.get_edges(dst_vid);
            for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
              const vid_t src_vid = it.get_vertex();
              if (community[src_vid] != std::numeric_limits<vid_t>::max() &&
                  edge_pred_fn(edge_triplet, src_vid, dst_vid,
                               it.get_data_ptr())) {
                neighbor_communities[community[src_vid]]++;
              }
            }
            if (neighbor_communities.empty()) {
              return;
            }
            // Find the most frequent community among neighbors
            int32_t max_count = 0;
            vid_t max_community = community[dst_vid];
            for (const auto& pair : neighbor_communities) {
              if (pair.second > max_count) {
                max_count = pair.second;
                max_community = pair.first;
              } else if (pair.second == max_count &&
                         pair.first < max_community) {
                // Tie-breaking by smaller community ID
                max_community = pair.first;
              }
            }
            next_community[dst_vid] = max_community;
            if (max_community != community[dst_vid]) {
              updated.store(true, std::memory_order_relaxed);
            }
          },
          concurrency_);
      if (!updated.load(std::memory_order_relaxed)) {
        break;
      }
      community.swap(next_community);
      LOG(INFO) << "Iteration " << iteration + 1 << " completed in "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::high_resolution_clock::now() - begin)
                       .count()
                << " ms.";
    }
  }

  void sink(execution::Context& ctx, int32_t node_alias, int32_t label_alias) {
    execution::MSVertexColumnBuilder node_builder(vertex_label);
    execution::ValueColumnBuilder<int64_t> label_builder;
    node_builder.reserve(vertices.size());
    label_builder.reserve(vertices.size());

    for (vid_t vid : vertices) {
      node_builder.push_back_opt(vid);
      label_builder.push_back_opt(static_cast<int64_t>(community[vid]));
    }

    ctx.set(node_alias, node_builder.finish());
    ctx.set(label_alias, label_builder.finish());
  }

  const StorageReadInterface& graph;
  label_t vertex_label;
  execution::LabelTriplet edge_triplet;
  int max_iterations;
  std::vector<vid_t> community;
  std::vector<vid_t> next_community;
  std::vector<vid_t> vertices;
  int concurrency_;
};
execution::Context LabelPropagationFunction::exec(
    const function::CallFuncInputBase& input, neug::IStorageInterface& g,
    neug::execution::Context& ctx) {
  const auto& lp_input = dynamic_cast<const LabelPropagationInput&>(input);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  const label_t vertex_label = lp_input.vertex_label;
  const execution::LabelTriplet& edge_triplet = lp_input.edge_triplet;
  const int max_iterations = lp_input.max_iterations;

  LabelPropagation label_propagation(graph, vertex_label, edge_triplet,
                                     max_iterations, lp_input.concurrency);
  if (lp_input.vertex_pred) {
    auto expr = lp_input.vertex_pred->bind(&graph, {});
    execution::GeneralPred vertex_pred(std::move(expr));
    label_propagation.init_communities(vertex_pred);
  } else {
    execution::DummyPred vertex_pred;
    label_propagation.init_communities(vertex_pred);
  }
  if (lp_input.edge_pred) {
    auto expr = lp_input.edge_pred->bind(&graph, {});
    execution::GeneralPred edge_pred(std::move(expr));
    label_propagation.propagate_labels(edge_pred);
  } else {
    execution::DummyPred edge_pred;
    label_propagation.propagate_labels(edge_pred);
  }
  label_propagation.sink(ctx, lp_input.node_alias, lp_input.label_alias);
  return ctx;
}

function::function_set LabelPropagationFunction::getFunctionSet() {
  function::function_set funcSet;
  // two input params:
  // 1. subgraph name in string
  // 2. options in map
  std::vector<common::LogicalTypeID> inputTypes = {
      common::LogicalTypeID::STRING, common::LogicalTypeID::ANY};
  // two output columns:
  // 1. node type
  // 2. label id in int64
  function::call_output_columns outputColumns = {
      {"node", common::LogicalTypeID::NODE},
      {"label", common::LogicalTypeID::INT64}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;

  funcSet.emplace_back(std::move(function));
  return funcSet;
}
}  // namespace gds
}  // namespace neug
