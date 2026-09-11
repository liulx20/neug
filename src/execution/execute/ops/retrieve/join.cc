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

#include "neug/execution/execute/ops/retrieve/join.h"

#include <glog/logging.h>

#include "neug/common/types/graph_types.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/join.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/likely.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {

// Build data belongs to this execution, independently of the cached plan.
class JoinState final : public BuildProbeState {
 public:
  JoinState(const JoinParams& params, Stream<ContextChunk> right)
      : params_(params), right_(std::move(right)) {}

  Status Build() override {
    if (!table_) {
      auto right = collect_chunk(std::move(right_));
      if (!right) {
        return right.error();
      }
      table_ = std::make_unique<JoinTable>(std::move(*right), params_);
    }
    return Status::OK();
  }

  void SetProbeInput(Stream<ContextChunk> input) override {
    left_ = std::move(input);
  }

  Stream<ContextChunk>::NextResult Next() override {
    auto status = Build();
    if (!status) {
      return tl::unexpected(status);
    }
    GS_AUTO(left, left_.Next());
    if (!left) {
      return std::optional<ContextChunk>{};
    }
    GS_AUTO(output, table_->Probe(std::move(*left)));
    return std::optional<ContextChunk>(std::move(output));
  }

 private:
  JoinParams params_;
  Stream<ContextChunk> left_;
  Stream<ContextChunk> right_;
  std::unique_ptr<JoinTable> table_;
};

class JoinOpr : public IOperator {
 public:
  JoinOpr(neug::execution::Pipeline&& left_pipeline,
          neug::execution::Pipeline&& right_pipeline,
          const JoinParams& join_params)
      : left_pipeline_(std::move(left_pipeline)),
        right_pipeline_(std::move(right_pipeline)),
        params_(join_params) {}

  SubPipelines sub_pipelines() override {
    return {SubPipelineMode::kBuildProbe, {&left_pipeline_, &right_pipeline_}};
  }
  std::string get_operator_name() const override { return "JoinOpr"; }
  bool supports_task_execution() const override {
    return left_pipeline_.supports_task_execution() &&
           right_pipeline_.supports_task_execution();
  }

  Stream<ContextChunk> Eval(IStorageInterface& graph, const ParamsMap& params,
                            Stream<ContextChunk>&& input,
                            neug::execution::OprTimer* timer,
                            OperatorInputs branches) override {
    if (branches.size() != 2) {
      return error_stream<ContextChunk>(
          Status::InternalError("Join requires two inputs"));
    }
    auto state = CreateBuildState(std::move(branches[1]));
    state->SetProbeInput(std::move(branches[0]));
    return Stream<ContextChunk>(std::move(state));
  }

  std::shared_ptr<BuildProbeState> CreateBuildState(
      Stream<ContextChunk> right) override {
    return std::make_shared<JoinState>(params_, std::move(right));
  }

  void build_explain_children(OprTimer* parent_timer, const ParamsMap& params,
                              IStorageInterface& graph) override {
    // Build explain tree for left and right pipelines
    // and add them as children to the parent timer
    auto left_tree_result = left_pipeline_.explain_tree(graph, params);
    auto right_tree_result = right_pipeline_.explain_tree(graph, params);

    if (left_tree_result && left_tree_result.value()) {
      parent_timer->add_child(std::move(left_tree_result.value()));
    }
    if (right_tree_result && right_tree_result.value()) {
      parent_timer->add_child(std::move(right_tree_result.value()));
    }
  }

 private:
  neug::execution::Pipeline left_pipeline_;
  neug::execution::Pipeline right_pipeline_;

  JoinParams params_;
};

neug::result<OpBuildResultT> JoinOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta;
  std::vector<int> right_columns;
  auto& opr = plan.plan(op_idx).opr().join();
  JoinParams p;
  if (opr.left_keys().size() != opr.right_keys().size()) {
    LOG(ERROR) << "join keys size mismatch";
    return std::make_pair(nullptr, ContextMeta());
  }
  const auto& left_keys = opr.left_keys();

  for (int i = 0; i < left_keys.size(); i++) {
    if (!left_keys.Get(i).has_tag()) {
      LOG(ERROR) << "left_keys should have tag";
      return std::make_pair(nullptr, ContextMeta());
    }
    p.left_columns.push_back(left_keys.Get(i).tag().id());
  }
  const auto& right_keys = opr.right_keys();
  for (int i = 0; i < right_keys.size(); i++) {
    if (!right_keys.Get(i).has_tag()) {
      LOG(ERROR) << "right_keys should have tag";
      return std::make_pair(nullptr, ContextMeta());
    }
    p.right_columns.push_back(right_keys.Get(i).tag().id());
  }

  p.join_type = parse_join_kind(opr.join_kind());
  auto join_kind = plan.plan(op_idx).opr().join().join_kind();

  auto pair1_res = PlanParser::get().parse_execute_pipeline_with_meta(
      schema, ctx_meta, plan.plan(op_idx).opr().join().left_plan());

  if (!pair1_res) {
    return std::make_pair(nullptr, ContextMeta());
  }
  auto pair2_res = PlanParser::get().parse_execute_pipeline_with_meta(
      schema, ctx_meta, plan.plan(op_idx).opr().join().right_plan());
  if (!pair2_res) {
    LOG(ERROR) << "failed to build right pipeline for join operator"
               << pair2_res.error().ToString();
    return std::make_pair(nullptr, ContextMeta());
  }
  auto pair1 = std::move(pair1_res.value());
  auto pair2 = std::move(pair2_res.value());
  const auto& ctx_meta1 = pair1.second;
  const auto& ctx_meta2 = pair2.second;
  if (join_kind == physical::Join_JoinKind::Join_JoinKind_SEMI ||
      join_kind == physical::Join_JoinKind::Join_JoinKind_ANTI) {
    ret_meta = ctx_meta1;
  } else if (join_kind == physical::Join_JoinKind::Join_JoinKind_INNER) {
    ret_meta = ctx_meta1;
    for (auto k : ctx_meta2.columns()) {
      ret_meta.set(k.first, k.second);
    }
  } else if (join_kind == physical::Join_JoinKind::Join_JoinKind_TIMES) {
    ret_meta = ctx_meta1;
    for (auto k : ctx_meta2.columns()) {
      ret_meta.set(k.first, k.second);
    }
  } else {
    if (join_kind != physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER) {
      LOG(ERROR) << "unsupported join kind" << join_kind;
      return std::make_pair(nullptr, ContextMeta());
    }
    ret_meta = ctx_meta1;
    for (const auto& k : ctx_meta2.columns()) {
      if (std::find(p.right_columns.begin(), p.right_columns.end(), k.first) ==
          p.right_columns.end()) {
        ret_meta.set(k.first, k.second);
      }
    }
  }
  return std::make_pair(std::make_unique<JoinOpr>(std::move(pair1.first),
                                                  std::move(pair2.first), p),
                        ret_meta);
}

class PrimaryKeyJoinOpr : public IOperator {
 public:
  PrimaryKeyJoinOpr(neug::execution::Pipeline&& right_pipeline,
                    const std::vector<label_t>& labels, int tag, int alias)
      : right_pipeline_(std::move(right_pipeline)),
        labels_(labels),
        tag_(tag),
        alias_(alias) {}

  SubPipelines sub_pipelines() override {
    return {SubPipelineMode::kStreaming, {&right_pipeline_}};
  }
  std::string get_operator_name() const override { return "PrimaryJoinOpr"; }
  bool supports_task_execution() const override {
    return right_pipeline_.supports_task_execution();
  }

  Stream<ContextChunk> Eval(IStorageInterface& graph, const ParamsMap& params,
                            Stream<ContextChunk>&& input,
                            neug::execution::OprTimer* timer,
                            OperatorInputs branches) override {
    if (branches.size() != 1) {
      return error_stream<ContextChunk>(
          Status::InternalError("PK Join requires one input"));
    }
    return map_chunks(
        std::move(branches[0]),
        [this, &graph](ContextChunk&& chunk) -> result<ContextChunk> {
          return Join::pk_join(graph, std::move(chunk), labels_, tag_, alias_);
        });
  }

 private:
  neug::execution::Pipeline right_pipeline_;
  std::vector<label_t> labels_;
  int tag_, alias_;
};

neug::result<OpBuildResultT> PrimaryKeyJoinOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta;
  auto& opr = plan.plan(op_idx).opr().join();
  const auto& left_keys = opr.left_keys();
  const auto& right_keys = opr.right_keys();
  if (right_keys.size() != 1) {
    return std::make_pair(nullptr, ContextMeta());
  }
  int tag = right_keys.Get(0).tag().id();
  if (left_keys.size() != 1) {
    return std::make_pair(nullptr, ContextMeta());
  }
  const auto& left_key = left_keys.Get(0);
  const auto& right_key = right_keys.Get(0);
  JoinKind join_kind = parse_join_kind(opr.join_kind());
  if (join_kind != JoinKind::kInnerJoin) {
    return std::make_pair(nullptr, ContextMeta());
  }
  if ((!left_key.has_property()) || right_key.has_property() ||
      (!left_key.property().has_key())) {
    return std::make_pair(nullptr, ContextMeta());
  }

  auto name = left_key.property().key().name();
  auto left_plan = plan.plan(op_idx).opr().join().left_plan();
  if (left_plan.plan_size() != 1 || (!left_plan.plan(0).opr().has_scan())) {
    LOG(ERROR) << "SPJoin left plan should be a scan operator";
    return std::make_pair(nullptr, ContextMeta());
  }

  const auto& scan_opr = left_plan.plan(0).opr().scan();
  if (scan_opr.has_idx_predicate() ||
      (scan_opr.has_params() && scan_opr.params().has_predicate())) {
    LOG(ERROR) << "PKJoin left scan operator should not have predicate";
    return std::make_pair(nullptr, ContextMeta());
  }
  int alias = scan_opr.has_alias() ? scan_opr.alias().value() : -1;
  const auto& vec = parse_tables(scan_opr.params());
  if (vec.size() != 1) {
    LOG(ERROR) << "PKJoin left scan operator should scan only one table";
    return std::make_pair(nullptr, ContextMeta());
  }
  for (label_t label : vec) {
    auto pk_name = std::get<1>(schema.get_vertex_primary_key(label)[0]);
    if (pk_name != name) {
      LOG(ERROR) << "PKJoin left key property should be the primary key of the "
                    "scanned table";
      return std::make_pair(nullptr, ContextMeta());
    }
  }

  auto right_res = PlanParser::get().parse_execute_pipeline_with_meta(
      schema, ctx_meta, plan.plan(op_idx).opr().join().right_plan());
  if (!right_res) {
    LOG(ERROR) << "failed to build right pipeline for join operator"
               << right_res.error().ToString();
    return std::make_pair(nullptr, ContextMeta());
  }
  auto pair = std::move(right_res.value());
  const auto& ctx_meta2 = pair.second;
  ret_meta.set(alias, DataType::VERTEX);
  for (auto k : ctx_meta2.columns()) {
    ret_meta.set(k.first, k.second);
  }
  return std::make_pair(std::make_unique<PrimaryKeyJoinOpr>(
                            std::move(pair.first), vec, tag, alias),
                        ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug