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

#include "neug/execution/execute/ops/retrieve/union.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/union.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/likely.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class UnionState final : public OperatorState {
 public:
  explicit UnionState(OperatorInputs inputs) : inputs_(std::move(inputs)) {}
  Stream<ContextChunk>::NextResult Next() override {
    while (index_ < inputs_.size()) {
      GS_AUTO(next, inputs_[index_].Next());
      if (next) {
        next->head().reset();
        return next;
      }
      ++index_;
    }
    return std::optional<ContextChunk>{};
  }

 private:
  OperatorInputs inputs_;
  size_t index_ = 0;
};

class UnionOpr : public IOperator {
 public:
  explicit UnionOpr(std::vector<Pipeline>&& sub_plans)
      : sub_plans_(std::move(sub_plans)) {}

  SubPipelines sub_pipelines() override {
    SubPipelines result{SubPipelineMode::kSequential, {}};
    for (auto& plan : sub_plans_) {
      result.plans.push_back(&plan);
    }
    return result;
  }
  std::string get_operator_name() const override { return "UnionOpr"; }
  bool supports_task_execution() const override {
    return std::all_of(
        sub_plans_.begin(), sub_plans_.end(),
        [](const auto& plan) { return plan.supports_task_execution(); });
  }

  Stream<ContextChunk> Eval(IStorageInterface& graph, const ParamsMap& params,
                            Stream<ContextChunk>&& input,
                            neug::execution::OprTimer* timer,
                            OperatorInputs branches) override {
    auto metadata =
        branches.empty() ? input.metadata() : branches[0].metadata();
    return Stream<ContextChunk>(
        std::make_shared<UnionState>(std::move(branches)), std::move(metadata));
  }

  void build_explain_children(OprTimer* parent_timer, const ParamsMap& params,
                              IStorageInterface& graph) override {
    // Build explain tree for each sub plan
    // and add them as children to the parent timer
    for (auto& plan : sub_plans_) {
      auto tree_result = plan.explain_tree(graph, params);
      if (tree_result && tree_result.value()) {
        parent_timer->add_child(std::move(tree_result.value()));
      }
    }
  }

 private:
  std::vector<Pipeline> sub_plans_;
};
neug::result<OpBuildResultT> UnionOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& union_op = plan.plan(op_idx).opr().union_();
  if (union_op.sub_plans_size() != 2) {
    RETURN_UNSUPPORTED_ERROR(
        "Union: exactly two sub-plans are supported, got " +
        std::to_string(union_op.sub_plans_size()));
  }

  std::vector<Pipeline> sub_plans;
  std::vector<ContextMeta> sub_metas;
  for (int i = 0; i < union_op.sub_plans_size(); ++i) {
    const auto& sub_plan = union_op.sub_plans(i);
    auto pair_res = PlanParser::get().parse_execute_pipeline_with_meta(
        schema, ctx_meta, sub_plan);
    if (!pair_res) {
      RETURN_ERROR(pair_res.error());
    }
    auto pair = std::move(pair_res.value());
    sub_plans.emplace_back(std::move(pair.first));
    sub_metas.push_back(pair.second);
  }

  ContextMeta ret_meta = ctx_meta;
  if (!sub_metas.empty()) {
    ret_meta = sub_metas.front();
    const auto& expected_columns = ret_meta.columns();
    for (size_t i = 1; i < sub_metas.size(); ++i) {
      const auto& actual_columns = sub_metas[i].columns();
      bool aliases_match = actual_columns.size() == expected_columns.size();
      if (aliases_match) {
        for (const auto& column : expected_columns) {
          if (actual_columns.find(column.first) == actual_columns.end()) {
            aliases_match = false;
            break;
          }
        }
      }
      if (!aliases_match) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_SCHEMA_MISMATCH,
                            "Union: output aliases of branch " +
                                std::to_string(i) + " do not match branch 0");
      }
    }
  }

  return std::make_pair(std::make_unique<UnionOpr>(std::move(sub_plans)),
                        ret_meta);
}
}  // namespace ops
}  // namespace execution
}  // namespace neug
