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

#include <stdexcept>
#include <string>
#include <vector>

#include "neug/execution/common/params_map.h"
#include "neug/execution/common/stream.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

class Pipeline;
struct BuildProbeInputs {
  Stream<ContextChunk> probe;
  Stream<ContextChunk> build;
};

class OperatorInputs {
 public:
  template <typename... Args>
  void Add(Args&&... args) {
    streams_.emplace_back(std::forward<Args>(args)...);
  }

  Stream<ContextChunk> TakeSingle() {
    if (streams_.size() != 1) {
      throw std::logic_error("Operator requires one input");
    }
    auto stream = std::move(streams_.front());
    streams_.clear();
    return stream;
  }

  BuildProbeInputs TakeBuildProbe() {
    if (streams_.size() != 2) {
      throw std::logic_error("Operator requires probe and build inputs");
    }
    BuildProbeInputs ports{std::move(streams_[0]), std::move(streams_[1])};
    streams_.clear();
    return ports;
  }

  std::vector<Stream<ContextChunk>> TakeAll() {
    return std::exchange(streams_, {});
  }

 private:
  std::vector<Stream<ContextChunk>> streams_;
};

// Declares data dependencies, never scheduling operations. Materialized inputs
// form pipeline barriers. Sequential inputs retain demand-driven consumption.
enum class SubPipelineMode {
  kNone,
  kMaterialized,
  kStreaming,
  kSequential,
  kBuildProbe
};
struct SubPipelines {
  SubPipelineMode mode = SubPipelineMode::kNone;
  std::vector<Pipeline*> plans;

  Pipeline& probe_plan() const {
    CheckBuildProbe();
    return *plans[0];
  }
  Pipeline& build_plan() const {
    CheckBuildProbe();
    return *plans[1];
  }

 private:
  void CheckBuildProbe() const {
    if (mode != SubPipelineMode::kBuildProbe || plans.size() != 2) {
      throw std::logic_error("Build/probe requires two plans");
    }
  }
};

// Execution-owned blocking build phase followed by a streaming probe phase.
// The pipeline builder controls when Build runs and connects the probe input.
class BuildProbeState : public OperatorState {
 public:
  virtual Status Build() = 0;
  virtual void SetProbeInput(Stream<ContextChunk> input) = 0;
};

class IOperator {
 public:
  virtual ~IOperator() = default;

  virtual SubPipelines sub_pipelines() { return {}; }

  virtual std::shared_ptr<BuildProbeState> CreateBuildState(
      Stream<ContextChunk> input) {
    throw std::logic_error("Operator has no build phase");
  }

  // Source-like operators can replace the incoming data flow. The builder
  // retains enclosing fork barriers while pruning unused data dependencies.
  virtual bool consumes_input() const { return true; }

  virtual std::string get_operator_name() const = 0;

  virtual Stream<ContextChunk> Eval(IStorageInterface& graph,
                                    const ParamsMap& params,
                                    OperatorInputs inputs, OprTimer* timer) = 0;

  virtual void build_explain_children(OprTimer* parent_timer,
                                      const ParamsMap& params,
                                      IStorageInterface& graph) {}
};

// Both execution modes create the same state. Only the pipeline builder
// decides whether its build phase runs as a separate task.
class BuildProbeOperator : public IOperator {
 public:
  Stream<ContextChunk> Eval(IStorageInterface&, const ParamsMap&,
                            OperatorInputs inputs, OprTimer*) final {
    auto ports = inputs.TakeBuildProbe();
    auto state = CreateBuildState(std::move(ports.build));
    state->SetProbeInput(std::move(ports.probe));
    return Stream<ContextChunk>(std::move(state));
  }
};

using OpBuildResultT = std::pair<std::unique_ptr<IOperator>, ContextMeta>;

class IOperatorBuilder {
 public:
  virtual ~IOperatorBuilder() = default;
  virtual neug::result<OpBuildResultT> Build(const neug::Schema& schema,
                                             const ContextMeta& ctx_meta,
                                             const physical::PhysicalPlan& plan,
                                             int op_idx) = 0;
  virtual int stepping(int i) { return i + GetOpKinds().size(); }

  virtual std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const = 0;
};

}  // namespace execution

}  // namespace neug
