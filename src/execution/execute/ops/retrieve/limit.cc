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

#include "neug/execution/execute/ops/retrieve/limit.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/limit.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class LimitState final : public OperatorState {
 public:
  LimitState(size_t lower, size_t upper)
      : skip_(lower), remaining_(upper > lower ? upper - lower : 0) {}
  KernelResult Process(ContextChunk chunk) override {
    auto rows = chunk.row_num();
    auto begin = std::min(skip_, rows);
    skip_ -= begin;
    auto count = std::min(remaining_, rows - begin);
    remaining_ -= count;
    GS_AUTO(output, Limit::limit(std::move(chunk), begin, begin + count));
    done_ = remaining_ == 0;
    return one_chunk(std::move(output));
  }
  bool Finished() const override { return done_; }

 private:
  size_t skip_;
  size_t remaining_;
  bool done_ = false;
};

class LimitOpr : public IOperator {
 public:
  explicit LimitOpr(const algebra::Limit& opr) {
    lower_ = 0;
    upper_ = std::numeric_limits<size_t>::max();
    if (opr.has_range()) {
      lower_ = std::max(lower_, static_cast<size_t>(opr.range().lower()));
      upper_ = std::min(upper_, static_cast<size_t>(opr.range().upper()));
    }
  }

  std::string get_operator_name() const override { return "LimitOpr"; }

  Kernel CreateState(IStorageInterface& graph, const ParamsMap& params,
                     neug::execution::OprTimer* timer) override {
    return std::make_unique<LimitState>(lower_, upper_);
  }

 private:
  size_t lower_;
  size_t upper_;
};

neug::result<OpBuildResultT> LimitOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(
      std::make_unique<LimitOpr>(plan.plan(op_idx).opr().limit()), ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug