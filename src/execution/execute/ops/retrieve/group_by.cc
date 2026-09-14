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

#include "neug/execution/execute/ops/retrieve/group_by.h"
#include "group_by_state.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/group_by.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/execute/ops/retrieve/group_by_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace execution {
class OprTimer;

namespace ops {

class GroupByOpr : public IOperator {
 public:
  GroupByOpr(std::vector<std::pair<int, int>>&& mappings,
             std::vector<physical::GroupBy_AggFunc>&& aggrs,
             const ContextMeta& meta)
      : mappings_(std::move(mappings)), aggrs_(std::move(aggrs)) {
    partitioned_ = !aggrs_.empty();
    for (auto [source, dest] : mappings_) {
      if (source < 0 || dest < 0 || !meta.exist(source)) {
        partitioned_ = false;
        return;
      }
      key_types_.push_back(meta.get(source));
    }
    for (const auto& aggr : aggrs_) {
      auto kind = parse_aggregate(aggr.aggregate());
      int input = -1;
      DataType type(DataTypeId::kInt64);
      if (aggr.vars_size() == 1 && aggr.vars(0).has_tag() &&
          !aggr.vars(0).has_property() && meta.exist(aggr.vars(0).tag().id())) {
        input = aggr.vars(0).tag().id();
        type = meta.get(input);
      } else if (aggr.vars_size() != 0 || kind != AggrKind::kCount) {
        partitioned_ = false;
        return;
      }
      if (!aggr.has_alias() || aggr.alias().value() < 0 ||
          !Supports(kind, type)) {
        partitioned_ = false;
        return;
      }
      specs_.push_back({kind, input, aggr.alias().value(), type});
    }
  }

  PipelineBehavior pipeline_behavior() const override {
    return partitioned_ ? PipelineBehavior::kPartitioned
                        : PipelineBehavior::kGlobal;
  }
  std::shared_ptr<PartitionState> CreatePartitionState(
      size_t workers) override {
    return std::make_shared<GroupByState>(mappings_, key_types_, specs_,
                                          workers);
  }
  std::optional<std::vector<int>> output_columns() const override {
    std::vector<int> columns;
    for (auto [source, dest] : mappings_) {
      columns.push_back(dest);
    }
    for (const auto& aggr : aggrs_) {
      if (aggr.has_alias()) {
        columns.push_back(aggr.alias().value());
      }
    }
    return columns;
  }

  std::string get_operator_name() const override { return "GroupByOpr"; }

  Kernel CreateState(IStorageInterface& graph, const ParamsMap& params,
                     neug::execution::OprTimer* timer) override {
    return make_global_kernel(
        [this, &graph, params,
         timer](ContextChunk&& chunk) -> result<ContextChunk> {
          {
            auto key = create_key_func(mappings_, graph, chunk.chunk());
            std::vector<ReduceOp> reducers;
            for (auto& aggr : aggrs_) {
              reducers.push_back(create_reduce_op(aggr, graph, chunk.chunk()));
            }
            return GroupBy::group_by(std::move(chunk), std::move(key),
                                     std::move(reducers));
          }
        });
  }

 private:
  static bool Supports(AggrKind kind, const DataType& type) {
    if (kind == AggrKind::kCount) {
      return true;
    }
    if (kind != AggrKind::kSum && kind != AggrKind::kAvg &&
        kind != AggrKind::kMin && kind != AggrKind::kMax) {
      return false;
    }
    switch (type.id()) {
    case DataTypeId::kInt32:
    case DataTypeId::kInt64:
    case DataTypeId::kUInt32:
    case DataTypeId::kUInt64:
      return true;
    case DataTypeId::kFloat:
    case DataTypeId::kDouble:
      // MIN/MAX with NaN depends on row order even inside a local batch.
      return kind == AggrKind::kSum || kind == AggrKind::kAvg;
    case DataTypeId::kBoolean:
    case DataTypeId::kVarchar:
    case DataTypeId::kDate:
    case DataTypeId::kTimestampMs:
    case DataTypeId::kInterval:
      return kind == AggrKind::kMin || kind == AggrKind::kMax;
    default:
      return false;
    }
  }
  bool partitioned_ = false;
  std::vector<DataType> key_types_;
  std::vector<AggregateSpec> specs_;
  std::vector<std::pair<int, int>> mappings_;
  std::vector<physical::GroupBy_AggFunc> aggrs_;
};

neug::result<OpBuildResultT> GroupByOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  int mappings_num = plan.plan(op_idx).opr().group_by().mappings_size();
  int func_num = plan.plan(op_idx).opr().group_by().functions_size();
  ContextMeta meta;
  int metadata_num = plan.plan(op_idx).meta_data_size();
  if (func_num + mappings_num == metadata_num) {
    for (int i = 0; i < metadata_num; ++i) {
      meta.set(plan.plan(op_idx).meta_data(i).alias(),
               parse_from_ir_data_type(plan.plan(op_idx).meta_data(i).type()));
    }
  } else {
    THROW_INTERNAL_EXCEPTION("GroupBy metadata number mismatch.");
  }
  auto opr = plan.plan(op_idx).opr().group_by();
  std::vector<std::pair<int, int>> mappings;
  std::vector<physical::GroupBy_AggFunc> reduce_funcs;

  if (!BuildGroupByUtils(opr, mappings, reduce_funcs)) {
    return std::make_pair(nullptr, ContextMeta());
  }

  return std::make_pair(
      std::make_unique<GroupByOpr>(std::move(mappings),

                                   std::move(reduce_funcs), ctx_meta),

      meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug