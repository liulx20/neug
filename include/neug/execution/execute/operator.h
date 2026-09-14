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
#include "neug/execution/execute/morsel.h"
#include "neug/execution/execute/operator_state.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

class Pipeline;
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

// Execution-owned partition accumulation. The executor orders each partition
// and bounds pending batches; operators only implement data processing.
class PartitionState {
 public:
  virtual ~PartitionState() = default;
  // Immutable partition output. The executor orders appends within each
  // bucket and bounds the number of batches waiting to be appended.
  struct Batch {
    virtual ~Batch() = default;
  };
  // A null batch needs no append work (for example, redundant empty input).
  virtual std::shared_ptr<Batch> PartitionBuild(ContextChunk input) const = 0;
  virtual size_t BuildPartitions() const = 0;
  virtual Status BuildPartition(size_t partition, const Batch& batch) = 0;
  virtual Status FinalizeBuild() = 0;
  virtual ChunkBatch TakeOutput() { return {}; }
};

class BuildProbeState : public PartitionState {
 public:
  virtual result<ContextChunk> ProbeChunk(ContextChunk chunk) const = 0;
};

enum class PipelineBehavior {
  kGlobal,
  kChunkLocal,
  kMorselSource,
  kPartitioned
};

class IOperator {
 public:
  virtual ~IOperator() = default;

  virtual SubPipelines sub_pipelines() { return {}; }

  // Chunk-local operators may be instantiated independently for each work
  // range. Global state (Limit, distinct, aggregation...) creates a boundary.
  virtual PipelineBehavior pipeline_behavior() const {
    return PipelineBehavior::kGlobal;
  }
  virtual std::unique_ptr<MorselSource> CreateMorselSource(IStorageInterface&,
                                                           const ParamsMap&) {
    throw std::logic_error("Operator has no morsel source");
  }

  virtual std::shared_ptr<BuildProbeState> CreateBuildState(size_t workers) {
    throw std::logic_error("Operator has no build phase");
  }

  virtual std::shared_ptr<PartitionState> CreatePartitionState(size_t workers) {
    throw std::logic_error("Operator has no partitioned state");
  }

  // Source-like operators can replace the incoming data flow. The builder
  // retains enclosing fork barriers while pruning unused data dependencies.
  virtual bool consumes_input() const { return true; }

  virtual std::string get_operator_name() const = 0;

  virtual Kernel CreateState(IStorageInterface& graph, const ParamsMap& params,
                             OprTimer* timer) = 0;

  virtual std::optional<std::vector<int>> output_columns() const {
    return std::nullopt;
  }

  virtual void build_explain_children(OprTimer* parent_timer,
                                      const ParamsMap& params,
                                      IStorageInterface& graph) {}
};

class MorselSourceOperator : public IOperator {
 public:
  PipelineBehavior pipeline_behavior() const final {
    return PipelineBehavior::kMorselSource;
  }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) final {
    throw std::logic_error("Source state is created by the pipeline builder");
  }
};

class BuildProbeOperator : public IOperator {
 public:
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) final {
    throw std::logic_error("Join phases are created by the pipeline builder");
  }
};

class PartitionedOperator : public IOperator {
 public:
  PipelineBehavior pipeline_behavior() const final {
    return PipelineBehavior::kPartitioned;
  }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) final {
    throw std::logic_error(
        "Partition phases are created by the pipeline builder");
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
