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
class LimitOpr : public IOperator {
 public:
  bool supports_task_execution() const override { return true; }

  explicit LimitOpr(const algebra::Limit& opr) {
    lower_ = 0;
    upper_ = std::numeric_limits<size_t>::max();
    if (opr.has_range()) {
      lower_ = std::max(lower_, static_cast<size_t>(opr.range().lower()));
      upper_ = std::min(upper_, static_cast<size_t>(opr.range().upper()));
    }
  }

  std::string get_operator_name() const override { return "LimitOpr"; }

  Stream<ContextChunk> Eval(IStorageInterface& graph, const ParamsMap& params,
                            Stream<ContextChunk>&& input,
                            neug::execution::OprTimer* timer,
                            TaskScheduler* scheduler) override {
    auto metadata = input.metadata();
    class LimitState final : public OperatorState {
     public:
      LimitState(Stream<ContextChunk> input, size_t lower, size_t upper)
          : input_(std::move(input)),
            skip_(lower),
            remaining_(upper > lower ? upper - lower : 0) {}
      Stream<ContextChunk>::NextResult Next() override {
        if (done_) {
          return std::optional<ContextChunk>{};
        }
        GS_AUTO(next, input_.Next());
        if (!next) {
          return std::optional<ContextChunk>{};
        }
        ContextChunk chunk = std::move(*next);
        auto rows = chunk.row_num();
        auto begin = std::min(skip_, rows);
        skip_ -= begin;
        auto count = std::min(remaining_, rows - begin);
        remaining_ -= count;
        GS_AUTO(output, Limit::limit(std::move(chunk), begin, begin + count));
        if (remaining_ == 0) {
          done_ = true;
          input_ = Stream<ContextChunk>();
        }
        return std::optional<ContextChunk>(std::move(output));
      }

     private:
      Stream<ContextChunk> input_;
      size_t skip_;
      size_t remaining_;
      bool done_ = false;
    };
    return Stream<ContextChunk>(
        std::make_shared<LimitState>(std::move(input), lower_, upper_),
        std::move(metadata));
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