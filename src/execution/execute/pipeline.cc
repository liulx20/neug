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

#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/pipeline_graph.h"

#include <algorithm>
#include <memory>
#include <optional>

#include "neug/execution/common/context.h"

namespace neug::execution {
namespace {
Status operator_error(const Status& error, const std::string& name) {
  return Status(error.error_code(), "Execution failed at operator: [" + name +
                                        "], " + error.error_message());
}

class StreamTimerScope {
 public:
  StreamTimerScope(OprTimer& timer, double& charged)
      : timer_(timer), charged_(charged), before_(charged) {
    clock_.start();
  }
  ~StreamTimerScope() {
    double elapsed = std::max(0.0, clock_.elapsed() - (charged_ - before_));
    timer_.add_elapsed(elapsed);
    charged_ += elapsed;
  }

 private:
  OprTimer& timer_;
  double& charged_;
  double before_;
  TimerUnit clock_;
};

class PipelineOperatorState final : public OperatorState {
 public:
  PipelineOperatorState(Stream<ContextChunk> producer, std::string name,
                        OprTimer* timer, std::shared_ptr<double> charged)
      : producer_(std::move(producer)),
        name_(std::move(name)),
        timer_(timer),
        charged_(std::move(charged)) {}
  Stream<ContextChunk>::NextResult Next() override {
    if (timer_) {
      StreamTimerScope scope(*timer_, *charged_);
      auto next = Pull();
      if (next && *next) {
        timer_->add_num_tuples((**next).row_num());
      }
      return next;
    }
    return Pull();
  }

 private:
  Stream<ContextChunk>::NextResult Pull() {
    auto next = producer_.Next();
    if (!next) {
      return tl::unexpected(operator_error(next.error(), name_));
    }
    return next;
  }
  Stream<ContextChunk> producer_;
  std::string name_;
  OprTimer* timer_;
  std::shared_ptr<double> charged_;
};

// A pipeline boundary's shared data. Filling finishes before any concurrent
// reader starts. Each reader has its own cursor; columns are shared read-only.
struct PipelineBuffer {
  explicit PipelineBuffer(Stream<ContextChunk> input)
      : metadata(input.metadata()), input(std::move(input)) {}
  Status Fill() {
    if (!data) {
      data.emplace(collect_batches(std::move(input)));
    }
    return *data ? Status::OK() : data->error();
  }
  StreamMetadata metadata;
  Stream<ContextChunk> input;
  std::optional<result<std::vector<ContextChunk>>> data;
};

class BufferReaderState final : public OperatorState {
 public:
  explicit BufferReaderState(std::shared_ptr<PipelineBuffer> buffer)
      : buffer_(std::move(buffer)) {}
  Stream<ContextChunk>::NextResult Next() override {
    if (!buffer_->data) {
      return tl::unexpected(
          Status::InternalError("Pipeline input is not ready"));
    }
    if (!*buffer_->data) {
      return tl::unexpected(buffer_->data->error());
    }
    const auto& chunks = buffer_->data->value();
    if (index_ == chunks.size()) {
      return std::optional<ContextChunk>{};
    }
    return std::optional<ContextChunk>(chunks[index_++]);
  }

 private:
  std::shared_ptr<PipelineBuffer> buffer_;
  size_t index_ = 0;
};

Stream<ContextChunk> ReadBuffer(const std::shared_ptr<PipelineBuffer>& buffer) {
  return Stream<ContextChunk>(std::make_shared<BufferReaderState>(buffer),
                              buffer->metadata);
}

// Sequential branch groups stay in one pull pipeline. This preserves Union
// short-circuiting: an unconsumed branch is neither initialized nor executed.
class InlineBranchState final : public OperatorState {
 public:
  InlineBranchState(Pipeline& plan, IStorageInterface& storage,
                    ParamsMap params, std::shared_ptr<PipelineBuffer> seed,
                    OprTimer* timer)
      : plan_(plan),
        storage_(storage),
        params_(std::move(params)),
        seed_(std::move(seed)),
        timer_(timer) {}
  Stream<ContextChunk>::NextResult Next() override {
    if (!output_) {
      auto status = seed_->Fill();
      if (!status) {
        return tl::unexpected(status);
      }
      output_.emplace(
          plan_.ExecuteStream(storage_, ReadBuffer(seed_), params_, timer_));
    }
    return output_->Next();
  }

 private:
  Pipeline& plan_;
  IStorageInterface& storage_;
  ParamsMap params_;
  std::shared_ptr<PipelineBuffer> seed_;
  OprTimer* timer_;
  std::optional<Stream<ContextChunk>> output_;
};

struct PipelineFragment {
  Stream<ContextChunk> output;
  std::vector<PipelineGraph::NodeId> dependencies;
  std::vector<PipelineGraph::NodeId> barriers;
};

class ScheduledPipelineState final : public OperatorState {
 public:
  ScheduledPipelineState(size_t workers, std::shared_ptr<PipelineGraph> graph,
                         PipelineFragment fragment)
      : scheduler_(workers),
        graph_(std::move(graph)),
        fragment_(std::move(fragment)) {}
  Stream<ContextChunk>::NextResult Next() override {
    auto status = graph_->Execute(scheduler_, fragment_.dependencies);
    if (!status) {
      return tl::unexpected(status);
    }
    auto output = scheduler_.Submit([this] { return fragment_.output.Next(); });
    // Only the external consumer waits. Worker tasks never wait for children.
    return output.get();
  }

 private:
  TaskScheduler scheduler_;
  std::shared_ptr<PipelineGraph> graph_;
  PipelineFragment fragment_;
};
}  // namespace

// The sole owner of execution-flow construction. Operators declare subplans
// and receive prepared inputs; they have no access to the scheduler or DAG.
class PipelineBuilder {
 public:
  PipelineBuilder(IStorageInterface& storage, const ParamsMap& params,
                  PipelineGraph* graph)
      : storage_(storage), params_(params), graph_(graph) {}

  PipelineFragment Build(Pipeline& plan, PipelineFragment fragment,
                         OprTimer* timer) {
    auto charged = timer ? std::make_shared<double>(0.0) : nullptr;
    auto* current_timer = timer;
    for (size_t i = 0; i < plan.operators_.size(); ++i) {
      auto& op = *plan.operators_[i];
      auto name = op.get_operator_name();
      if (current_timer) {
        current_timer->set_name(name);
      }
      if (!op.consumes_input()) {
        fragment.dependencies = fragment.barriers;
      }
      auto children = op.sub_pipelines();
      OperatorInputs inputs;
      std::shared_ptr<BuildProbeState> build_state;
      if (children.mode == SubPipelineMode::kBuildProbe && graph_) {
        if (children.plans.size() != 2) {
          throw std::logic_error("Build/probe requires two inputs");
        }
        auto seed =
            std::make_shared<PipelineBuffer>(std::move(fragment.output));
        auto seed_id =
            graph_->Add(name + "/input", std::move(fragment.dependencies),
                        [seed] { return seed->Fill(); });
        auto* left_timer = ChildTimer(current_timer);
        auto* right_timer = ChildTimer(current_timer);
        auto right =
            Build(children.build_plan(),
                  {ReadBuffer(seed), {seed_id}, {seed_id}}, right_timer);
        build_state = op.CreateBuildState(std::move(right.output));
        auto build_id =
            graph_->Add(name + "/build", std::move(right.dependencies),
                        [state = build_state, current_timer] {
                          if (current_timer) {
                            double charged = 0;
                            StreamTimerScope scope(*current_timer, charged);
                            return state->Build();
                          }
                          return state->Build();
                        });
        auto left =
            Build(children.probe_plan(),
                  {ReadBuffer(seed), {build_id}, {build_id}}, left_timer);
        build_state->SetProbeInput(std::move(left.output));
        fragment.dependencies = std::move(left.dependencies);
      } else if (children.mode == SubPipelineMode::kMaterialized && graph_) {
        auto seed =
            std::make_shared<PipelineBuffer>(std::move(fragment.output));
        auto seed_id =
            graph_->Add(name + "/input", std::move(fragment.dependencies),
                        [seed] { return seed->Fill(); });
        fragment.dependencies.clear();
        for (size_t child = 0; child < children.plans.size(); ++child) {
          auto branch = Build(*children.plans[child],
                              {ReadBuffer(seed), {seed_id}, {seed_id}},
                              ChildTimer(current_timer));
          auto result =
              std::make_shared<PipelineBuffer>(std::move(branch.output));
          auto id = graph_->Add(name + "/branch" + std::to_string(child),
                                std::move(branch.dependencies),
                                [result] { return result->Fill(); });
          fragment.dependencies.push_back(id);
          inputs.Add(ReadBuffer(result));
        }
      } else if (children.mode == SubPipelineMode::kStreaming) {
        if (children.plans.size() != 1) {
          throw std::logic_error("Streaming subpipeline requires one input");
        }
        fragment = Build(*children.plans[0], std::move(fragment),
                         ChildTimer(current_timer));
        inputs.Add(std::move(fragment.output));
      } else if (!children.plans.empty()) {
        auto seed =
            std::make_shared<PipelineBuffer>(std::move(fragment.output));
        for (auto* child : children.plans) {
          inputs.Add(
              std::make_shared<InlineBranchState>(
                  *child, storage_, params_, seed, ChildTimer(current_timer)),
              seed->metadata);
        }
      }
      if (children.mode == SubPipelineMode::kNone) {
        inputs.Add(std::move(fragment.output));
      }
      auto output = build_state ? Stream<ContextChunk>(std::move(build_state))
                                : op.Eval(storage_, params_, std::move(inputs),
                                          current_timer);
      auto metadata = output.metadata();
      fragment.output = Stream<ContextChunk>(
          std::make_shared<PipelineOperatorState>(std::move(output), name,
                                                  current_timer, charged),
          std::move(metadata));
      if (current_timer && i + 1 < plan.operators_.size()) {
        current_timer->set_next(std::make_unique<OprTimer>());
        current_timer = current_timer->next();
      }
    }
    return fragment;
  }

 private:
  OprTimer* ChildTimer(OprTimer* parent) {
    if (!parent) {
      return nullptr;
    }
    auto child = std::make_unique<OprTimer>();
    auto* result = child.get();
    parent->add_child(std::move(child));
    return result;
  }
  IStorageInterface& storage_;
  const ParamsMap& params_;
  PipelineGraph* graph_;
};

result<Context> Pipeline::Execute(IStorageInterface& graph, Context&& ctx,
                                  const ParamsMap& params, OprTimer* timer) {
  return materialize(
      ExecuteStream(graph, stream_from_context(std::move(ctx)), params, timer));
}

Stream<ContextChunk> Pipeline::ExecuteStream(IStorageInterface& graph,
                                             Stream<ContextChunk> input,
                                             const ParamsMap& params,
                                             OprTimer* timer) {
  return PipelineBuilder(graph, params, nullptr)
      .Build(*this, {std::move(input), {}, {}}, timer)
      .output;
}

bool Pipeline::supports_task_execution() const {
  return std::all_of(operators_.begin(), operators_.end(), [](const auto& op) {
    return op->supports_task_execution();
  });
}

Stream<ContextChunk> Pipeline::ExecuteScheduled(IStorageInterface& graph,
                                                Stream<ContextChunk> input,
                                                const ParamsMap& params,
                                                size_t workers,
                                                OprTimer* timer) {
  if (graph.writable() || !graph.readable()) {
    return error_stream<ContextChunk>(
        Status(StatusCode::ERR_INVALID_ARGUMENT,
               "Scheduled execution requires a read-only snapshot"));
  }
  if (!supports_task_execution()) {
    return error_stream<ContextChunk>(Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Pipeline contains an operator that does not support task execution"));
  }
  if (workers == 0) {
    return error_stream<ContextChunk>(
        Status(StatusCode::ERR_INVALID_ARGUMENT,
               "Scheduled execution requires at least one worker"));
  }
  auto execution_graph = std::make_shared<PipelineGraph>();
  auto fragment = PipelineBuilder(graph, params, execution_graph.get())
                      .Build(*this, {std::move(input), {}, {}}, timer);
  auto metadata = fragment.output.metadata();
  return Stream<ContextChunk>(
      std::make_shared<ScheduledPipelineState>(
          workers, std::move(execution_graph), std::move(fragment)),
      std::move(metadata));
}

neug::result<std::unique_ptr<OprTimer>> Pipeline::explain_tree(
    IStorageInterface& graph, const ParamsMap& params) {
  std::unique_ptr<OprTimer> root = nullptr;
  OprTimer* current = nullptr;

  for (size_t i = 0; i < operators_.size(); ++i) {
    auto timer_node = std::make_unique<OprTimer>();
    timer_node->set_name(operators_[i]->get_operator_name());

    // Add current timer_node to the linked list
    if (!root) {
      root = std::move(timer_node);
      current = root.get();
    } else {
      auto next = std::move(timer_node);
      current->set_next(std::move(next));
      current = current->next();
    }
  }

  // process children for each operator
  if (root) {
    OprTimer* op_timer = root.get();
    for (size_t i = 0; i < operators_.size(); ++i) {
      if (op_timer) {
        operators_[i]->build_explain_children(op_timer, params, graph);
        op_timer = op_timer->next();
      }
    }
  }

  return root;
}

}  // namespace neug::execution
