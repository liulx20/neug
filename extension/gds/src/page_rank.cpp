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

#include "pagerank.h"

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/predicates.h"
#include "parallel_utils.h"

namespace neug {
namespace gds {

class UndirectedPageRank {
 public:
  UndirectedPageRank(const StorageReadInterface& graph, label_t vertex_label,
                     label_t edge_label, int max_iterations,
                     double damping_factor, int concurrency)
      : graph_(graph),
        vertex_label_(vertex_label),
        edge_label_(edge_label),
        max_iterations_(max_iterations),
        damping_factor_(damping_factor),
        concurrency_(concurrency) {
    size_t vertex_count = graph.GetVertexSet(vertex_label).size();
    pr_ = std::make_unique<double[]>(vertex_count);
    out_degree_ = std::make_unique<uint32_t[]>(vertex_count);
    auto oe_view = graph.GetGenericOutgoingGraphView(vertex_label, vertex_label,
                                                     edge_label);
    auto ie_view = graph.GetGenericIncomingGraphView(vertex_label, vertex_label,
                                                     edge_label);
    std::atomic<uint32_t> dangling_count = 0;
    double p = 1.0 / vertex_count;
    ParallelUtils::parallel_for(
        0, vertex_count,
        [&](vid_t v) {
          uint32_t degree = 0;
          auto oe_edges = oe_view.get_edges(v);
          for ([[maybe_unused]] auto it = oe_edges.begin();
               it != oe_edges.end(); ++it) {
            degree++;
          }
          auto ie_edges = ie_view.get_edges(v);
          for ([[maybe_unused]] auto it = ie_edges.begin();
               it != ie_edges.end(); ++it) {
            degree++;
          }
          out_degree_[v] = degree;
          if (degree == 0) {
            dangling_count.fetch_add(1);
            pr_[v] = p;
          } else {
            pr_[v] = p / degree;
          }
        },
        concurrency_);
    dangling_count_ = dangling_count.load();
    dangling_sum_ = p * dangling_count_;
  }

  void computePageRank() {
    size_t vertex_count = graph_.GetVertexSet(vertex_label_).size();
    std::unique_ptr<double[]> new_pr = std::make_unique<double[]>(vertex_count);
    auto ie_view = graph_.GetGenericIncomingGraphView(
        vertex_label_, vertex_label_, edge_label_);
    auto oe_view = graph_.GetGenericOutgoingGraphView(
        vertex_label_, vertex_label_, edge_label_);
    for (int iter = 0; iter <= max_iterations_; ++iter) {
      double base = (1.0 - damping_factor_) / vertex_count +
                    damping_factor_ * dangling_sum_ / vertex_count;
      dangling_sum_ = base * dangling_count_;
      ParallelUtils::parallel_for(
          0, vertex_count,
          [&](vid_t v) {
            double rank_sum = 0.0;
            auto ie_edges = ie_view.get_edges(v);
            for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
              vid_t src = it.get_vertex();
              rank_sum += pr_[src];
            }
            new_pr[v] = rank_sum;
          },
          concurrency_);

      ParallelUtils::parallel_for(
          0, vertex_count,
          [&](vid_t v) {
            double rank_sum = 0.0;
            auto oe_edges = oe_view.get_edges(v);
            for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
              vid_t src = it.get_vertex();
              rank_sum += pr_[src];
            }

            uint32_t degree = out_degree_[v];
            rank_sum += new_pr[v];  // add contribution from incoming edges
            new_pr[v] = degree > 0
                            ? (base + damping_factor_ * rank_sum) / degree
                            : base;
          },
          concurrency_);

      std::swap(pr_, new_pr);
    }

    ParallelUtils::parallel_for(
        0, vertex_count,
        [&](vid_t v) {
          if (out_degree_[v] != 0) {
            pr_[v] = pr_[v] * out_degree_[v];
          }
        },
        concurrency_);
  }

  void sink(execution::Context& ctx, int node_alias, int pr_alias) {
    execution::MSVertexColumnBuilder builder(vertex_label_);
    builder.reserve(graph_.GetVertexSet(vertex_label_).size());
    const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
    execution::ValueColumnBuilder<double> pr_builder;
    pr_builder.reserve(vertex_set.size());
    for (vid_t v : vertex_set) {
      builder.push_back_opt(v);
      pr_builder.push_back_opt(pr_[v]);
    }
    ctx.set(node_alias, builder.finish());
    ctx.set(pr_alias, pr_builder.finish());
  }

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  std::unique_ptr<double[]> pr_;

  std::unique_ptr<uint32_t[]> out_degree_;
  std::vector<vid_t> dangling_vertices_;
  int max_iterations_;
  double damping_factor_;
  int concurrency_;
  uint32_t dangling_count_;
  double dangling_sum_;
};

struct PageRankInput : public function::CallFuncInputBase {
  ~PageRankInput() = default;
  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      label_t& vertex_label, label_t& edge_label) {
    if (subgraph.vertex_entries().size() != 1) {
      LOG(ERROR) << "PageRank currently only supports subgraphs "
                    "with exactly one vertex label.";
      return false;
    }
    const auto& vertex_entry = subgraph.vertex_entries(0);
    if (vertex_entry.has_predicate()) {
      LOG(ERROR) << "Vertex predicates are not supported in PageRank.";
      return false;
    }
    vertex_label = vertex_entry.label_id();
    if (subgraph.edge_entries().size() != 1) {
      LOG(ERROR) << "PageRank currently only supports subgraphs "
                    "with exactly one edge label.";
      return false;
    }

    const auto& edge_entry = subgraph.edge_entries(0);
    if (edge_entry.has_predicate()) {
      LOG(ERROR) << "Edge predicates are not supported in PageRank.";
      return false;
    }
    edge_label = edge_entry.edge_label_id();
    if (edge_entry.src_label_id() != vertex_label ||
        edge_entry.dst_label_id() != vertex_label) {
      LOG(ERROR)
          << "Source and destination vertex labels of the edge must match "
             "the vertex label in PageRank.";
      return false;
    }
    return true;
  }

  label_t vertex_label;
  label_t edge_label;
  int max_iterations;
  double damping_factor;
  int concurrency;
  int32_t node_alias, pr_alias;
  bool directed;
};

template <typename T>
T get_option_value(
    const google::protobuf::Map<std::string, std::string>& options,
    const std::string& key, T default_value) {
  auto it = options.find(key);
  if (it != options.end()) {
    if constexpr (std::is_same_v<T, int32_t>) {
      try {
        return std::stoi(it->second);
      } catch (const std::exception& e) {
        throw std::runtime_error("Invalid value for " + key + ": " +
                                 it->second);
      }
    } else if constexpr (std::is_same_v<T, double>) {
      try {
        return std::stod(it->second);
      } catch (const std::exception& e) {
        throw std::runtime_error("Invalid value for " + key + ": " +
                                 it->second);
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      return it->second;
    } else {
      static_assert(std::false_type::value, "Unsupported option value type");
    }
  }
  return default_value;
}

std::unique_ptr<function::CallFuncInputBase> PageRankFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();
  label_t vertex_label, edge_label;
  auto input = std::make_unique<PageRankInput>();
  if (!input->parse_subgraph(subgraph, vertex_label, edge_label)) {
    LOG(ERROR) << "Failed to parse subgraph for PageRank.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for PageRank");
  }
  input->vertex_label = vertex_label;
  input->edge_label = edge_label;
  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->pr_alias = plan.plan(op_idx).meta_data(1).alias();
  input->damping_factor =
      get_option_value<double>(options, "damping_factor", 0.85);
  input->max_iterations =
      get_option_value<int32_t>(options, "max_iterations", 20);
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());
  input->directed =
      get_option_value<std::string>(options, "directed", "false") == "true";
  return input;
}

execution::Context PageRankFunction::exec(
    const function::CallFuncInputBase& input, neug::IStorageInterface& g,
    neug::execution::Context& ctx) {
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);
  const auto& func_input = dynamic_cast<const PageRankInput&>(input);
  auto start = std::chrono::high_resolution_clock::now();
  UndirectedPageRank pagerank(graph, func_input.vertex_label,
                              func_input.edge_label, func_input.max_iterations,
                              func_input.damping_factor,
                              func_input.concurrency);
  LOG(INFO) << "PageRank initialization took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms";
  start = std::chrono::high_resolution_clock::now();

  pagerank.computePageRank();
  LOG(INFO) << "PageRank computation took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms";
  start = std::chrono::high_resolution_clock::now();
  pagerank.sink(ctx, func_input.node_alias, func_input.pr_alias);
  LOG(INFO) << "PageRank sink took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms";
  return ctx;
}

function::function_set PageRankFunction::getFunctionSet() {
  function::function_set funcSet;
  // two input params:
  // 1. subgraph name in string
  // 2. options in map
  std::vector<common::LogicalTypeID> inputTypes = {
      common::LogicalTypeID::STRING, common::LogicalTypeID::ANY};
  // two output columns:
  // 1. node type
  // 2. personalized page rank value in double
  function::call_output_columns outputColumns = {
      {"node", common::LogicalTypeID::NODE},
      {"page_rank", common::LogicalTypeID::DOUBLE}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;
  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug