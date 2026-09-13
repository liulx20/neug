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
Stream<ContextChunk> BuildExecution(Pipeline& plan, IStorageInterface& storage,
                                    Stream<ContextChunk> input,
                                    const ParamsMap& params, OprTimer* timer,
                                    size_t workers, TaskScheduler::Mode mode);

Status operator_error(const Status& error, const std::string& name) {
  return Status(error.error_code(), "Execution failed at operator: [" + name +
                                        "], " + error.error_message());
}

class PhaseTimerScope {
 public:
  PhaseTimerScope(OprTimer& timer, double& charged)
      : timer_(timer), charged_(charged), before_(charged) {
    clock_.start();
  }
  ~PhaseTimerScope() {
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

template <typename F>
auto CaptureWork(F&& function) -> decltype(function()) {
  using Result = decltype(function());
  Result output =
      tl::unexpected(Status::InternalError("Unfinished pipeline work"));
  TRY_HANDLE_ALL_WITH_EXCEPTION(
      Result, [&]() { return function(); },
      [&](const Status& status) { output = tl::unexpected(status); },
      [&](Result&& result) { output = std::move(result); });
  return output;
}

using KernelFactory = std::function<Kernel(OprTimer*)>;
struct KernelStep {
  std::string name;
  OprTimer* timer;
  KernelFactory factory;
  std::vector<int> input_columns;
};

// One iterative driver for a segment. Kernels never call another kernel or
// input.
class KernelChain {
 public:
  explicit KernelChain(std::vector<KernelStep> steps)
      : steps_(std::move(steps)) {}
  void Append(KernelStep step) { steps_.push_back(std::move(step)); }
  bool Stopped() const { return stopped_; }
  KernelResult Process(ContextChunk chunk, size_t begin = 0) {
    ChunkBatch pending = one_chunk(std::move(chunk));
    for (size_t i = begin; i < steps_.size(); ++i) {
      ChunkBatch output;
      for (auto& input : pending) {
        GS_AUTO(produced, Invoke(i, &input));
        for (auto& item : produced) {
          output.push_back(std::move(item));
        }
        if (states_[i]->Finished()) {
          stopped_ = true;
          finish_from_ = std::max(finish_from_, i + 1);
          break;
        }
      }
      pending = std::move(output);
      if (pending.empty()) {
        break;
      }
    }
    return pending;
  }
  KernelResult Finish() {
    ChunkBatch output;
    for (size_t i = finish_from_; i < steps_.size(); ++i) {
      GS_AUTO(produced, Invoke(i, nullptr));
      for (auto& chunk : produced) {
        GS_AUTO(tail, Process(std::move(chunk), i + 1));
        for (auto& item : tail) {
          output.push_back(std::move(item));
        }
        if (finish_from_ > i + 1) {
          break;
        }
      }
      i = std::max(i, finish_from_ == 0 ? size_t{0} : finish_from_ - 1);
    }
    return output;
  }

 private:
  KernelResult Invoke(size_t i, ContextChunk* chunk) {
    auto& step = steps_[i];
    auto run = [&]() -> KernelResult {
      if (states_.size() < steps_.size()) {
        states_.resize(steps_.size());
      }
      if (!states_[i]) {
        states_[i] = step.factory(step.timer);
        states_[i]->input_columns = step.input_columns;
      }
      return chunk ? states_[i]->Process(std::move(*chunk))
                   : states_[i]->Finalize();
    };
    TimerUnit clock;
    if (step.timer) {
      clock.start();
    }
    auto output = CaptureWork(run);
    if (step.timer) {
      step.timer->add_elapsed(clock.elapsed());
      if (output) {
        for (const auto& chunk : *output) {
          step.timer->add_num_tuples(chunk.row_num());
        }
      }
    }
    if (!output) {
      return tl::unexpected(operator_error(output.error(), step.name));
    }
    return output;
  }
  std::vector<KernelStep> steps_;
  std::vector<Kernel> states_;
  size_t finish_from_ = 0;
  bool stopped_ = false;
};

class LinearPipelineState final : public ResultReaderState {
 public:
  explicit LinearPipelineState(Stream<ContextChunk> input)
      : chain_({}), input_(std::move(input)) {}
  void Append(std::string name, OprTimer* timer, KernelFactory factory,
              std::vector<int> input_columns) {
    chain_.Append(
        {std::move(name), timer, std::move(factory), std::move(input_columns)});
  }
  Stream<ContextChunk>::NextResult Next() override {
    while (true) {
      if (index_ < pending_.size()) {
        return std::optional<ContextChunk>(std::move(pending_[index_++]));
      }
      pending_.clear();
      index_ = 0;
      if (finalized_) {
        return std::optional<ContextChunk>{};
      }
      if (chain_.Stopped()) {
        input_ = {};
        GS_AUTO(output, chain_.Finish());
        pending_ = std::move(output);
        finalized_ = true;
      } else {
        GS_AUTO(input, input_.Next());
        if (!input) {
          GS_AUTO(output, chain_.Finish());
          pending_ = std::move(output);
          finalized_ = true;
        } else {
          GS_AUTO(output, chain_.Process(std::move(*input)));
          pending_ = std::move(output);
        }
      }
    }
  }

 private:
  KernelChain chain_;
  Stream<ContextChunk> input_;
  ChunkBatch pending_;
  size_t index_ = 0;
  bool finalized_ = false;
};

// Shared step control plus independently owned worker-lane state. One queued
// task processes several ranges; no queue is placed between local transforms.
class MorselPipelineState final : public ResultReaderState {
 public:
  using SourceFactory = std::function<std::unique_ptr<MorselSource>()>;
  using Transform = KernelFactory;
  MorselPipelineState(TaskScheduler& scheduler, SourceFactory source,
                      std::string name, OprTimer* timer)
      : scheduler_(scheduler), factory_(std::move(source)) {
    names_.push_back(std::move(name));
    timers_.push_back(timer);
  }
  void Append(std::string name, OprTimer* timer, Transform transform) {
    names_.push_back(std::move(name));
    timers_.push_back(timer);
    transforms_.push_back(std::move(transform));
  }

  Stream<ContextChunk>::NextResult Next() override {
    while (true) {
      while (pending_index_ < pending_.size()) {
        auto& work = pending_[pending_index_];
        if (!work.output) {
          return tl::unexpected(work.output.error());
        }
        if (chunk_index_ < work.output->size()) {
          return std::optional<ContextChunk>(
              std::move((*work.output)[chunk_index_++]));
        }
        ++pending_index_;
        chunk_index_ = 0;
      }
      pending_.clear();
      pending_index_ = 0;
      if (finished_) {
        return std::optional<ContextChunk>{};
      }
      if (!source_) {
        source_ = factory_();
        factory_ = {};
        if (!source_) {
          return tl::unexpected(Status::InternalError("Missing morsel source"));
        }
        workers_.resize(scheduler_.concurrency());
      }
      RunWave();
    }
  }

 private:
  struct Output {
    size_t sequence;
    result<std::vector<ContextChunk>> output;
  };
  struct LocalState {
    std::unique_ptr<MorselReader> reader;
    std::vector<std::unique_ptr<OprTimer>> timers;
  };

  result<std::vector<ContextChunk>> RunMorsel(LocalState& local,
                                              const Morsel& work) {
    if (!local.reader) {
      local.reader = source_->CreateReader();
    }
    local.reader->Start(work);
    std::vector<KernelStep> steps;
    for (size_t i = 1; i < names_.size(); ++i) {
      steps.push_back({names_[i], local.timers[i].get(), transforms_[i - 1]});
    }
    KernelChain chain(std::move(steps));
    ChunkBatch output;
    while (true) {
      TimerUnit clock;
      auto* timer = local.timers[0].get();
      if (timer) {
        clock.start();
      }
      auto next = CaptureWork([&] { return local.reader->Next(); });
      if (timer) {
        timer->add_elapsed(clock.elapsed());
        if (next && *next) {
          timer->add_num_tuples((**next).row_num());
        }
      }
      if (!next) {
        return tl::unexpected(operator_error(next.error(), names_[0]));
      }
      if (!*next) {
        break;
      }
      GS_AUTO(produced, chain.Process(std::move(**next)));
      for (auto& chunk : produced) {
        output.push_back(std::move(chunk));
      }
    }
    GS_AUTO(tail, chain.Finish());
    for (auto& chunk : tail) {
      output.push_back(std::move(chunk));
    }
    return output;
  }

  std::vector<Output> RunTask(size_t lane) {
    auto& local = workers_[lane];
    local.timers.clear();
    for (size_t i = 0; i < names_.size(); ++i) {
      auto timer = timers_[i] ? std::make_unique<OprTimer>() : nullptr;
      if (timer) {
        timer->set_name(names_[i]);
      }
      local.timers.push_back(std::move(timer));
    }
    std::vector<Output> outputs;
    for (size_t count = 0; count < (workers_.size() == 1 ? 1 : 2); ++count) {
      size_t sequence;
      result<std::optional<Morsel>> work = std::optional<Morsel>{};
      {
        std::lock_guard<std::mutex> lock(pick_mutex_);
        if (exhausted_) {
          break;
        }
        work = CaptureWork([&] { return source_->Pick(); });
        sequence = next_sequence_++;
        if (!work || !*work) {
          exhausted_ = true;
        }
      }
      if (!work) {
        outputs.push_back({sequence, tl::unexpected(work.error())});
        break;
      }
      if (!*work) {
        break;
      }
      auto output = CaptureWork([&] { return RunMorsel(local, **work); });
      outputs.push_back({sequence, std::move(output)});
    }
    return outputs;
  }

  void RunWave() {
    std::vector<std::future<std::vector<Output>>> tasks;
    tasks.reserve(workers_.size());
    std::exception_ptr failure;
    try {
      for (size_t lane = 0; lane < workers_.size(); ++lane) {
        tasks.push_back(
            scheduler_.Submit([this, lane] { return RunTask(lane); }));
      }
    } catch (...) { failure = std::current_exception(); }
    // Every submitted task is joined, including after a submit/task failure.
    for (auto& task : tasks) {
      try {
        auto output = scheduler_.Wait(task);
        for (auto& item : output) {
          pending_.push_back(std::move(item));
        }
      } catch (...) {
        if (!failure) {
          failure = std::current_exception();
        }
      }
    }
    if (failure) {
      std::rethrow_exception(failure);
    }
    for (auto& worker : workers_) {
      for (size_t i = 0; i < timers_.size(); ++i) {
        if (timers_[i]) {
          timers_[i]->add_local_metrics(*worker.timers[i]);
        }
      }
    }
    std::sort(pending_.begin(), pending_.end(),
              [](const Output& a, const Output& b) {
                return a.sequence < b.sequence;
              });
    if (std::any_of(pending_.begin(), pending_.end(),
                    [](const Output& output) { return !output.output; })) {
      finished_ = true;
      return;
    }
    if (exhausted_) {
      // Exhaustion is observed only after all in-flight ranges have completed.
      auto status = source_->Finalize();
      finished_ = true;
      if (!status) {
        pending_.push_back({next_sequence_, tl::unexpected(status)});
      }
    }
  }

  TaskScheduler& scheduler_;
  SourceFactory factory_;
  std::unique_ptr<MorselSource> source_;
  std::vector<std::string> names_;
  std::vector<OprTimer*> timers_;
  std::vector<Transform> transforms_;
  std::vector<LocalState> workers_;
  std::mutex pick_mutex_;
  bool exhausted_ = false;
  bool finished_ = false;
  size_t next_sequence_ = 0;
  std::vector<Output> pending_;
  size_t pending_index_ = 0;
  size_t chunk_index_ = 0;
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

class BufferReaderState final : public ResultReaderState {
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
class LazyBranchState final : public ResultReaderState {
 public:
  LazyBranchState(Pipeline& plan, IStorageInterface& storage, ParamsMap params,
                  std::shared_ptr<PipelineBuffer> seed, OprTimer* timer)
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
      output_.emplace(BuildExecution(plan_, storage_, ReadBuffer(seed_),
                                     params_, timer_, 1,
                                     TaskScheduler::Mode::kInline));
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
  std::shared_ptr<MorselPipelineState> morsel_step;
  std::shared_ptr<LinearPipelineState> linear_step;
};

class PipelineExecutionState final : public ResultReaderState {
 public:
  PipelineExecutionState(std::shared_ptr<TaskScheduler> scheduler,
                         std::shared_ptr<PipelineGraph> graph,
                         PipelineFragment fragment)
      : scheduler_(std::move(scheduler)),
        graph_(std::move(graph)),
        fragment_(std::move(fragment)) {}
  Stream<ContextChunk>::NextResult Next() override {
    auto status = graph_->Execute(*scheduler_, fragment_.dependencies);
    if (!status) {
      return tl::unexpected(status);
    }
    auto output =
        scheduler_->Submit([this] { return fragment_.output.Next(); });
    // The external consumer waits for pool work. In a conditional group the
    // inline task has already completed, so its worker never blocks here.
    return output.get();
  }

 private:
  std::shared_ptr<TaskScheduler> scheduler_;
  std::shared_ptr<PipelineGraph> graph_;
  PipelineFragment fragment_;
};
}  // namespace

// The sole owner of execution-flow construction. Operators declare subplans
// and receive prepared inputs; they have no access to the scheduler or DAG.
class PipelineBuilder {
 public:
  PipelineBuilder(IStorageInterface& storage, const ParamsMap& params,
                  PipelineGraph& graph, TaskScheduler& scheduler)
      : storage_(storage),
        params_(params),
        graph_(graph),
        scheduler_(scheduler) {}

  PipelineFragment Build(Pipeline& plan, PipelineFragment fragment,
                         OprTimer* timer) {
    auto* current_timer = timer;
    for (size_t i = 0; i < plan.operators_.size(); ++i) {
      auto& op = *plan.operators_[i];
      auto name = op.get_operator_name();
      if (current_timer) {
        current_timer->set_name(name);
      }
      if (!op.consumes_input()) {
        fragment.dependencies = fragment.barriers;
        fragment.output = {};
        fragment.linear_step.reset();
        fragment.morsel_step.reset();
      }
      if (op.pipeline_behavior() == PipelineBehavior::kMorselSource) {
        if (op.consumes_input()) {
          auto before = std::make_shared<Stream<ContextChunk>>(
              std::move(fragment.output));
          auto id = graph_.Add(name + "/input",
                               std::move(fragment.dependencies), [before] {
                                 while (true) {
                                   auto next = before->Next();
                                   if (!next) {
                                     return next.error();
                                   }
                                   if (!*next) {
                                     return Status::OK();
                                   }
                                 }
                               });
          fragment.dependencies = {id};
        }
        fragment.linear_step.reset();
        auto* source = &op;
        auto* storage = &storage_;
        auto params = params_;
        fragment.morsel_step = std::make_shared<MorselPipelineState>(
            scheduler_,
            [source, storage, params] {
              return source->CreateMorselSource(*storage, params);
            },
            name, current_timer);
        fragment.output = Stream<ContextChunk>(fragment.morsel_step);
        AdvanceTimer(current_timer, i, plan.operators_.size());
        continue;
      }
      if (fragment.morsel_step &&
          op.pipeline_behavior() == PipelineBehavior::kChunkLocal) {
        auto* transform = &op;
        auto* storage = &storage_;
        auto params = params_;
        fragment.morsel_step->Append(
            name, current_timer, [transform, storage, params](OprTimer* timer) {
              return transform->CreateState(*storage, params, timer);
            });
        AdvanceTimer(current_timer, i, plan.operators_.size());
        continue;
      }
      fragment.morsel_step.reset();
      auto children = op.sub_pipelines();
      std::vector<Stream<ContextChunk>> inputs;
      std::shared_ptr<BuildProbeState> build_state;
      if (children.mode == SubPipelineMode::kBuildProbe) {
        if (children.plans.size() != 2) {
          throw std::logic_error("Build/probe requires two inputs");
        }
        auto seed =
            std::make_shared<PipelineBuffer>(std::move(fragment.output));
        auto seed_id =
            graph_.Add(name + "/input", std::move(fragment.dependencies),
                       [seed] { return seed->Fill(); });
        auto* left_timer = ChildTimer(current_timer);
        auto* right_timer = ChildTimer(current_timer);
        auto right =
            Build(children.build_plan(),
                  {ReadBuffer(seed), {seed_id}, {seed_id}}, right_timer);
        auto build_input =
            std::make_shared<Stream<ContextChunk>>(std::move(right.output));
        build_state = op.CreateBuildState(scheduler_.concurrency());
        auto prepare_id =
            graph_.Add(name + "/prepare", std::move(right.dependencies),
                       [state = build_state, current_timer, build_input] {
                         auto prepare = [&] {
                           auto input = collect_chunk(std::move(*build_input));
                           if (!input) {
                             return input.error();
                           }
                           return state->PrepareBuild(std::move(*input));
                         };
                         if (current_timer) {
                           double charged = 0;
                           PhaseTimerScope scope(*current_timer, charged);
                           return prepare();
                         }
                         return prepare();
                       });
        std::vector<PipelineGraph::NodeId> partitions;
        auto times = std::make_shared<std::vector<double>>(
            build_state->BuildPartitions(), 0.0);
        for (size_t part = 0; part < build_state->BuildPartitions(); ++part) {
          partitions.push_back(
              graph_.Add(name + "/build" + std::to_string(part), {prepare_id},
                         [state = build_state, part, times, current_timer] {
                           if (!current_timer) {
                             return state->BuildPartition(part);
                           }
                           TimerUnit clock;
                           clock.start();
                           auto status = state->BuildPartition(part);
                           (*times)[part] = clock.elapsed();
                           return status;
                         }));
        }
        if (partitions.empty()) {
          partitions.push_back(prepare_id);
        }
        auto build_id =
            graph_.Add(name + "/finalize", std::move(partitions),
                       [state = build_state, current_timer, times] {
                         if (current_timer) {
                           for (auto elapsed : *times) {
                             current_timer->add_elapsed(elapsed);
                           }
                           double charged = 0;
                           PhaseTimerScope scope(*current_timer, charged);
                           return state->FinalizeBuild();
                         }
                         return state->FinalizeBuild();
                       });
        auto left =
            Build(children.probe_plan(),
                  {ReadBuffer(seed), {build_id}, {build_id}}, left_timer);
        fragment.dependencies = std::move(left.dependencies);
        if (left.morsel_step) {
          left.morsel_step->Append(
              name, current_timer, [state = build_state](OprTimer*) {
                return make_chunk_kernel([state](ContextChunk chunk) {
                  return state->ProbeChunk(std::move(chunk));
                });
              });
          fragment.output = std::move(left.output);
          fragment.morsel_step = std::move(left.morsel_step);
          fragment.linear_step.reset();
          AdvanceTimer(current_timer, i, plan.operators_.size());
          continue;
        }
        fragment.output = std::move(left.output);
        fragment.linear_step.reset();
      } else if (children.mode == SubPipelineMode::kMaterialized) {
        auto seed =
            std::make_shared<PipelineBuffer>(std::move(fragment.output));
        auto seed_id =
            graph_.Add(name + "/input", std::move(fragment.dependencies),
                       [seed] { return seed->Fill(); });
        fragment.dependencies.clear();
        for (size_t child = 0; child < children.plans.size(); ++child) {
          auto branch = Build(*children.plans[child],
                              {ReadBuffer(seed), {seed_id}, {seed_id}},
                              ChildTimer(current_timer));
          auto result =
              std::make_shared<PipelineBuffer>(std::move(branch.output));
          auto id = graph_.Add(name + "/branch" + std::to_string(child),
                               std::move(branch.dependencies),
                               [result] { return result->Fill(); });
          fragment.dependencies.push_back(id);
          inputs.emplace_back(ReadBuffer(result));
        }
      } else if (children.mode == SubPipelineMode::kStreaming) {
        if (children.plans.size() != 1) {
          throw std::logic_error("Streaming subpipeline requires one input");
        }
        fragment = Build(*children.plans[0], std::move(fragment),
                         ChildTimer(current_timer));
        inputs.emplace_back(std::move(fragment.output));
      } else if (!children.plans.empty()) {
        auto seed =
            std::make_shared<PipelineBuffer>(std::move(fragment.output));
        for (auto* child : children.plans) {
          inputs.emplace_back(
              std::make_shared<LazyBranchState>(*child, storage_, params_, seed,
                                                ChildTimer(current_timer)),
              seed->metadata);
        }
      }

      if (!inputs.empty()) {
        auto branches = std::make_shared<std::vector<Stream<ContextChunk>>>(
            std::move(inputs));
        auto index = std::make_shared<size_t>(0);
        auto metadata = branches->front().metadata();
        fragment.output = Stream<ContextChunk>(
            [branches, index]() -> Stream<ContextChunk>::NextResult {
              while (*index < branches->size()) {
                GS_AUTO(next, (*branches)[*index].Next());
                if (next) {
                  return next;
                }
                ++*index;
              }
              return std::optional<ContextChunk>{};
            },
            std::move(metadata));
        fragment.linear_step.reset();
      }
      fragment.morsel_step.reset();
      if (!fragment.linear_step) {
        auto metadata = fragment.output.metadata();
        fragment.linear_step =
            std::make_shared<LinearPipelineState>(std::move(fragment.output));
        fragment.output =
            Stream<ContextChunk>(fragment.linear_step, std::move(metadata));
      }
      auto* kernel = &op;
      auto* storage = &storage_;
      auto params = params_;
      fragment.linear_step->Append(
          name, current_timer,
          [kernel, storage, params, build_state](OprTimer* timer) -> Kernel {
            if (build_state) {
              return make_chunk_kernel([build_state](ContextChunk chunk) {
                return build_state->ProbeChunk(std::move(chunk));
              });
            }
            return kernel->CreateState(*storage, params, timer);
          },
          fragment.output.metadata().output_columns);
      if (auto columns = op.output_columns()) {
        fragment.output.set_metadata(StreamMetadata{std::move(*columns)});
      }
      AdvanceTimer(current_timer, i, plan.operators_.size());
    }
    return fragment;
  }

 private:
  void AdvanceTimer(OprTimer*& timer, size_t index, size_t count) {
    if (timer && index + 1 < count) {
      timer->set_next(std::make_unique<OprTimer>());
      timer = timer->next();
    }
  }
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
  PipelineGraph& graph_;
  TaskScheduler& scheduler_;
};

result<Context> Pipeline::Execute(IStorageInterface& graph, Context&& ctx,
                                  const ParamsMap& params, OprTimer* timer) {
  return materialize(
      ExecuteStream(graph, stream_from_context(std::move(ctx)), params, timer));
}

namespace {
Stream<ContextChunk> BuildExecution(Pipeline& plan, IStorageInterface& storage,
                                    Stream<ContextChunk> input,
                                    const ParamsMap& params, OprTimer* timer,
                                    size_t workers, TaskScheduler::Mode mode) {
  auto graph = std::make_shared<PipelineGraph>();
  auto scheduler = std::make_shared<TaskScheduler>(workers, mode);
  auto fragment = PipelineBuilder(storage, params, *graph, *scheduler)
                      .Build(plan, {std::move(input), {}, {}}, timer);
  auto metadata = fragment.output.metadata();
  return Stream<ContextChunk>(
      std::make_shared<PipelineExecutionState>(
          std::move(scheduler), std::move(graph), std::move(fragment)),
      std::move(metadata));
}
}  // namespace

Stream<ContextChunk> Pipeline::ExecuteStream(IStorageInterface& graph,
                                             Stream<ContextChunk> input,
                                             const ParamsMap& params,
                                             OprTimer* timer, size_t workers) {
  if (workers == 0) {
    return error_stream<ContextChunk>(
        Status(StatusCode::ERR_INVALID_ARGUMENT,
               "Execution requires at least one worker"));
  }
  return BuildExecution(*this, graph, std::move(input), params, timer,
                        graph.writable() ? 1 : workers,
                        TaskScheduler::Mode::kWorkerPool);
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
