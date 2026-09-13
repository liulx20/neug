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
#include <algorithm>
#include <deque>
#include <memory>
#include <optional>
#include "neug/execution/execute/task_scheduler.h"

namespace neug::execution {
namespace {
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

struct PipelineTask {
  virtual ~PipelineTask() = default;
  virtual bool Advance(QueueExecution&) = 0;
  std::vector<PipelineTask*> gates;
  std::vector<PipelineTask*> waiters;
  bool queued = false;
  // One-slot mailbox: publishing consumes downstream demand. A producer may
  // retain the finite output of its current invocation, but cannot read ahead.
  std::optional<ContextChunk> output;
  std::deque<result<ContextChunk>> pending;
  size_t running = 0;
  bool requested = false;
  bool finished = false;
  bool done = false;
};
}  // namespace

class QueueExecution {
 public:
  explicit QueueExecution(size_t workers) : scheduler_(workers) {}
  ~QueueExecution() { Drain(); }
  template <typename T, typename... Args>
  T* Add(Args&&... args) {
    auto node = std::make_unique<T>(std::forward<Args>(args)...);
    auto* ptr = node.get();
    tasks_.push_back(std::move(node));
    return ptr;
  }
  size_t workers() const { return scheduler_.concurrency(); }
  void SetRoot(PipelineTask* root) { root_ = root; }
  // All graph, mailbox and readiness changes happen on the consumer thread.
  // A completion event publishes worker writes before Advance resumes a task.
  template <typename F>
  void Submit(PipelineTask& owner, F work) {
    ++owner.running;
    ++active_;
    try {
      scheduler_.Submit([this, &owner, work = std::move(work)]() mutable {
        auto captured = CaptureWork([&]() -> result<bool> {
          auto status = work();
          if (!status) {
            return tl::unexpected(status);
          }
          return true;
        });
        std::lock_guard<std::mutex> lock(mutex_);
        completed_.push_back(
            {&owner, captured ? Status::OK() : captured.error()});
        ready_.notify_one();
      });
    } catch (...) {
      --owner.running;
      --active_;
      throw;
    }
  }
  bool Need(PipelineTask& node) {
    if (node.done || node.output) {
      return false;
    }
    if (current_ && std::find(node.waiters.begin(), node.waiters.end(),
                              current_) == node.waiters.end()) {
      node.waiters.push_back(current_);
    }
    if (node.requested) {
      return false;
    }
    node.requested = true;
    Enqueue(node);
    return true;
  }
  QueryResultReader::NextResult Next() {
    if (error_) {
      return tl::unexpected(*error_);
    }
    Need(*root_);
    while (true) {
      Receive(false);
      if (error_) {
        Drain();
        return tl::unexpected(*error_);
      }
      if (root_->output) {
        auto out = std::move(root_->output);
        root_->output.reset();
        return out;
      }
      if (root_->done) {
        Drain();
        return std::optional<ContextChunk>{};
      }
      if (runnable_.empty()) {
        if (!active_) {
          error_ = Status::InternalError("Pipeline has no ready task");
        } else {
          Receive(true);
        }
        continue;
      }
      auto& task = *runnable_.front();
      runnable_.pop_front();
      task.queued = false;
      if (!task.requested || task.running || task.done || task.output) {
        continue;
      }
      current_ = &task;
      bool blocked = false;
      for (auto* gate : task.gates) {
        if (!gate->done) {
          Need(*gate);
          blocked = true;
        }
      }
      if (!blocked) {
        if (!task.pending.empty()) {
          auto next = std::move(task.pending.front());
          task.pending.pop_front();
          if (!next) {
            error_ = next.error();
          } else {
            task.output = std::move(*next);
            task.requested = false;
            Wake(task);
          }
        } else if (task.finished) {
          task.done = true;
          Wake(task);
        } else if (task.Advance(*this)) {
          Enqueue(task);
        }
      }
      current_ = nullptr;
    }
  }
  void Drain() {
    // No further demand is issued. All submitted tasks finish before state,
    // storage references or profiling pointers can be released by the caller.
    while (active_) {
      Receive(true);
    }
  }

 private:
  void Enqueue(PipelineTask& task) {
    if (!task.queued && !task.running) {
      task.queued = true;
      runnable_.push_back(&task);
    }
  }
  void Wake(PipelineTask& task) {
    for (auto* waiter : task.waiters) {
      Enqueue(*waiter);
    }
    task.waiters.clear();
  }
  void Receive(bool wait) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (wait) {
      ready_.wait(lock, [&] { return !completed_.empty(); });
    }
    for (auto& event : completed_) {
      --event.first->running;
      --active_;
      if (!event.first->running) {
        Enqueue(*event.first);
      }
      if (!event.second && !error_) {
        error_ = event.second;
      }
    }
    completed_.clear();
  }
  std::vector<std::unique_ptr<PipelineTask>> tasks_;
  TaskScheduler scheduler_;
  PipelineTask* root_ = nullptr;
  PipelineTask* current_ = nullptr;
  std::deque<PipelineTask*> runnable_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::vector<std::pair<PipelineTask*, Status>> completed_;
  size_t active_ = 0;
  std::optional<Status> error_;
};

namespace {
void Publish(PipelineTask& task, ChunkBatch chunks) {
  for (auto& chunk : chunks) {
    task.pending.emplace_back(std::move(chunk));
  }
}
class InputTask final : public PipelineTask {
 public:
  explicit InputTask(ChunkBatch chunks) { Publish(*this, std::move(chunks)); }
  bool Advance(QueueExecution&) override {
    finished = true;
    return true;
  }
};

class LinearPipelineTask final : public PipelineTask {
 public:
  explicit LinearPipelineTask(PipelineTask* input)
      : chain_({}), input_(input) {}
  void Append(std::string name, OprTimer* timer, KernelFactory factory,
              std::vector<int> columns) {
    chain_.Append(
        {std::move(name), timer, std::move(factory), std::move(columns)});
  }
  bool Advance(QueueExecution& execution) override {
    if (chain_.Stopped() || input_->done) {
      execution.Submit(*this, [this] {
        auto result = chain_.Finish();
        if (!result) {
          return result.error();
        }
        Publish(*this, std::move(*result));
        finished = true;
        return Status::OK();
      });
      return true;
    }
    if (!input_->output) {
      return execution.Need(*input_);
    }
    auto chunk = std::move(*input_->output);
    input_->output.reset();
    execution.Submit(*this, [this, chunk = std::move(chunk)]() mutable {
      auto result = chain_.Process(std::move(chunk));
      if (!result) {
        return result.error();
      }
      Publish(*this, std::move(*result));
      return Status::OK();
    });
    return true;
  }

 private:
  KernelChain chain_;
  PipelineTask* input_;
};
class MorselPipelineTask final : public PipelineTask {
 public:
  using SourceFactory = std::function<std::unique_ptr<MorselSource>()>;
  using Transform = KernelFactory;
  MorselPipelineTask(size_t workers, SourceFactory source, std::string name,
                     OprTimer* timer)
      : worker_count_(workers), factory_(std::move(source)) {
    names_.push_back(std::move(name));
    timers_.push_back(timer);
  }
  void Append(std::string name, OprTimer* timer, Transform transform) {
    names_.push_back(std::move(name));
    timers_.push_back(timer);
    transforms_.push_back(std::move(transform));
  }

  bool Advance(QueueExecution& execution) override {
    if (!source_) {
      execution.Submit(*this, [this] {
        source_ = factory_();
        factory_ = {};
        if (!source_) {
          return Status::InternalError("Missing morsel source");
        }
        workers_.resize(worker_count_);
        return Status::OK();
      });
      return true;
    }
    if (wave_) {
      wave_ = false;
      execution.Submit(*this, [this] { return FinishWave(); });
      return true;
    }
    wave_ = true;
    wave_outputs_.clear();
    wave_outputs_.resize(workers_.size());
    for (size_t lane = 0; lane < workers_.size(); ++lane) {
      execution.Submit(*this, [this, lane] {
        wave_outputs_[lane] = RunTask(lane);
        return Status::OK();
      });
    }
    return true;
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

  Status FinishWave() {
    std::vector<Output> outputs;
    for (auto& lane : wave_outputs_) {
      for (auto& item : lane) {
        outputs.push_back(std::move(item));
      }
    }
    wave_outputs_.clear();
    for (auto& worker : workers_) {
      for (size_t i = 0; i < timers_.size(); ++i) {
        if (timers_[i]) {
          timers_[i]->add_local_metrics(*worker.timers[i]);
        }
      }
    }
    std::sort(outputs.begin(), outputs.end(),
              [](const Output& a, const Output& b) {
                return a.sequence < b.sequence;
              });
    for (auto& item : outputs) {
      if (!item.output) {
        pending.emplace_back(tl::unexpected(item.output.error()));
        finished = true;
        return Status::OK();
      }
      Publish(*this, std::move(*item.output));
    }
    if (exhausted_) {
      auto status = source_->Finalize();
      finished = true;
      if (!status) {
        pending.emplace_back(tl::unexpected(status));
      }
    }
    return Status::OK();
  }

  size_t worker_count_;
  bool wave_ = false;
  std::vector<std::vector<Output>> wave_outputs_;
  SourceFactory factory_;
  std::unique_ptr<MorselSource> source_;
  std::vector<std::string> names_;
  std::vector<OprTimer*> timers_;
  std::vector<Transform> transforms_;
  std::vector<LocalState> workers_;
  std::mutex pick_mutex_;
  bool exhausted_ = false;
  size_t next_sequence_ = 0;
};

// Materialization is explicit only at replay/build barriers. This node drains
// its mailbox into shared immutable batches; each replay owns its own cursor.
class BufferTask final : public PipelineTask {
 public:
  BufferTask(PipelineTask* input, bool retain = true)
      : input_(input), retain_(retain) {}
  bool Advance(QueueExecution& execution) override {
    if (input_->output) {
      if (retain_) {
        chunks.push_back(std::move(*input_->output));
      }
      input_->output.reset();
      return true;
    }
    if (input_->done) {
      finished = true;
      return true;
    }
    return execution.Need(*input_);
  }
  ChunkBatch chunks;

 private:
  PipelineTask* input_;
  bool retain_;
};
class ReplayTask final : public PipelineTask {
 public:
  explicit ReplayTask(BufferTask* buffer) : buffer_(buffer) {
    gates = {buffer};
  }
  bool Advance(QueueExecution&) override {
    if (index_ < buffer_->chunks.size()) {
      pending.emplace_back(buffer_->chunks[index_++]);
    } else {
      finished = true;
    }
    return true;
  }

 private:
  BufferTask* buffer_;
  size_t index_ = 0;
};
// The build barrier advances through explicit phases. State construction is
// deferred as well, so an unused conditional Join allocates no hash tables.
class BuildPipelineTask final : public PipelineTask {
 public:
  using Factory = std::function<std::shared_ptr<BuildProbeState>()>;
  BuildPipelineTask(BufferTask* input, Factory factory, OprTimer* timer)
      : input_(input), factory_(std::move(factory)), timer_(timer) {
    gates = {input};
  }
  BuildProbeState& state() const { return *state_; }
  bool Advance(QueueExecution& execution) override {
    if (phase_ == Phase::kPrepare) {
      phase_ = Phase::kBuild;
      execution.Submit(*this, [this] {
        return Timed([&] {
          state_ = factory_();
          factory_ = {};
          ChunkAccumulator chunks;
          for (auto& chunk : input_->chunks) {
            chunks.Add(std::move(chunk));
          }
          input_->chunks.clear();
          auto input = chunks.Finish();
          return state_->PrepareBuild(input ? std::move(*input)
                                            : ContextChunk{});
        });
      });
    } else if (phase_ == Phase::kBuild) {
      phase_ = Phase::kFinalize;
      times_.resize(state_->BuildPartitions());
      for (size_t part = 0; part < times_.size(); ++part) {
        execution.Submit(*this, [this, part] {
          if (!timer_) {
            return state_->BuildPartition(part);
          }
          TimerUnit clock;
          clock.start();
          auto status = state_->BuildPartition(part);
          times_[part] = clock.elapsed();
          return status;
        });
      }
    } else {
      execution.Submit(*this, [this] {
        if (timer_) {
          for (auto elapsed : times_) {
            timer_->add_elapsed(elapsed);
          }
        }
        auto status = Timed([&] { return state_->FinalizeBuild(); });
        finished = true;
        return status;
      });
    }
    return true;
  }

 private:
  template <typename F>
  Status Timed(F work) {
    if (!timer_) {
      return work();
    }
    double charged = 0;
    PhaseTimerScope scope(*timer_, charged);
    return work();
  }
  enum class Phase { kPrepare, kBuild, kFinalize };
  Phase phase_ = Phase::kPrepare;
  BufferTask* input_;
  Factory factory_;
  OprTimer* timer_;
  std::shared_ptr<BuildProbeState> state_;
  std::vector<double> times_;
};
class ConcatTask final : public PipelineTask {
 public:
  explicit ConcatTask(std::vector<PipelineTask*> inputs)
      : inputs_(std::move(inputs)) {}
  bool Advance(QueueExecution& execution) override {
    if (index_ == inputs_.size()) {
      finished = true;
      return true;
    }
    auto& input = *inputs_[index_];
    if (input.output) {
      pending.emplace_back(std::move(*input.output));
      input.output.reset();
      return true;
    }
    if (input.done) {
      ++index_;
      return true;
    }
    return execution.Need(input);
  }

 private:
  std::vector<PipelineTask*> inputs_;
  size_t index_ = 0;
};
struct PipelineFragment {
  PipelineTask* output;
  std::vector<PipelineTask*> barriers;
  std::vector<int> columns;
  MorselPipelineTask* morsel_step = nullptr;
  LinearPipelineTask* linear_step = nullptr;
};
}  // namespace

class PipelineBuilder {
 public:
  PipelineBuilder(IStorageInterface& storage, const ParamsMap& params,
                  QueueExecution& execution)
      : storage_(storage), params_(params), execution_(execution) {}
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
        fragment.output = execution_.Add<InputTask>(ChunkBatch{});
        fragment.output->gates = fragment.barriers;
        fragment.columns.clear();
        fragment.linear_step = nullptr;
        fragment.morsel_step = nullptr;
      }
      auto* storage = &storage_;
      auto params = params_;
      auto* operator_plan = &op;
      if (op.pipeline_behavior() == PipelineBehavior::kMorselSource) {
        auto* before = execution_.Add<BufferTask>(fragment.output, false);
        auto* task = execution_.Add<MorselPipelineTask>(
            execution_.workers(),
            [operator_plan, storage, params] {
              return operator_plan->CreateMorselSource(*storage, params);
            },
            name, current_timer);
        task->gates = {before};
        fragment.output = task;
        fragment.morsel_step = task;
        fragment.linear_step = nullptr;
        AdvanceTimer(current_timer, i, plan.operators_.size());
        continue;
      }
      if (fragment.morsel_step &&
          op.pipeline_behavior() == PipelineBehavior::kChunkLocal) {
        fragment.morsel_step->Append(
            name, current_timer,
            [operator_plan, storage, params](OprTimer* timer) {
              return operator_plan->CreateState(*storage, params, timer);
            });
        AdvanceTimer(current_timer, i, plan.operators_.size());
        continue;
      }
      fragment.morsel_step = nullptr;
      auto children = op.sub_pipelines();
      BuildPipelineTask* build_state = nullptr;
      if (children.mode == SubPipelineMode::kBuildProbe) {
        if (children.plans.size() != 2) {
          throw std::logic_error("Build/probe requires two inputs");
        }
        auto* seed = execution_.Add<BufferTask>(fragment.output);
        auto* left_timer = ChildTimer(current_timer);
        auto* right_timer = ChildTimer(current_timer);
        auto right = Build(children.build_plan(),
                           Replay(seed, fragment.columns, {seed}), right_timer);
        auto* build_input = execution_.Add<BufferTask>(right.output);
        build_state = execution_.Add<BuildPipelineTask>(
            build_input,
            [operator_plan, workers = execution_.workers()] {
              return operator_plan->CreateBuildState(workers);
            },
            current_timer);
        auto left =
            Build(children.probe_plan(),
                  Replay(seed, fragment.columns, {build_state}), left_timer);
        fragment.output = left.output;
        fragment.columns = left.columns;
        fragment.linear_step = nullptr;
        if (left.morsel_step) {
          left.morsel_step->Append(
              name, current_timer, [state = build_state](OprTimer*) {
                return make_chunk_kernel([state](ContextChunk chunk) {
                  return state->state().ProbeChunk(std::move(chunk));
                });
              });
          fragment.morsel_step = left.morsel_step;
          AdvanceTimer(current_timer, i, plan.operators_.size());
          continue;
        }
      } else if (children.mode == SubPipelineMode::kStreaming) {
        if (children.plans.size() != 1) {
          throw std::logic_error("Streaming subpipeline requires one input");
        }
        fragment = Build(*children.plans[0], std::move(fragment),
                         ChildTimer(current_timer));
        fragment.linear_step = nullptr;
      } else if (!children.plans.empty()) {
        auto* seed = execution_.Add<BufferTask>(fragment.output);
        std::vector<PipelineTask*> inputs, completed;
        std::vector<int> columns = fragment.columns;
        PipelineTask* previous = seed;
        for (auto* child : children.plans) {
          auto branch =
              Build(*child, Replay(seed, fragment.columns, {previous}),
                    ChildTimer(current_timer));
          if (inputs.empty()) {
            columns = branch.columns;
          }
          if (children.mode == SubPipelineMode::kMaterialized) {
            auto* buffer = execution_.Add<BufferTask>(branch.output);
            completed.push_back(buffer);
            inputs.push_back(execution_.Add<ReplayTask>(buffer));
            // Writes stay in plan order even when several branch gates become
            // ready.
            if (storage_.writable()) {
              previous = buffer;
            }
          } else {
            inputs.push_back(branch.output);
          }
        }
        auto* concat = execution_.Add<ConcatTask>(std::move(inputs));
        concat->gates = std::move(completed);
        fragment.output = concat;
        fragment.columns = std::move(columns);
        fragment.linear_step = nullptr;
      }
      fragment.morsel_step = nullptr;
      if (!fragment.linear_step) {
        fragment.linear_step =
            execution_.Add<LinearPipelineTask>(fragment.output);
        fragment.output = fragment.linear_step;
      }
      fragment.linear_step->Append(
          name, current_timer,
          [operator_plan, storage, params,
           build_state](OprTimer* timer) -> Kernel {
            if (build_state) {
              return make_chunk_kernel([build_state](ContextChunk chunk) {
                return build_state->state().ProbeChunk(std::move(chunk));
              });
            }
            return operator_plan->CreateState(*storage, params, timer);
          },
          fragment.columns);
      if (auto columns = op.output_columns()) {
        fragment.columns = std::move(*columns);
      }
      AdvanceTimer(current_timer, i, plan.operators_.size());
    }
    return fragment;
  }

 private:
  PipelineFragment Replay(BufferTask* buffer, const std::vector<int>& columns,
                          std::vector<PipelineTask*> gates) {
    auto* task = execution_.Add<ReplayTask>(buffer);
    task->gates.insert(task->gates.end(), gates.begin(), gates.end());
    return {task, std::move(gates), columns};
  }
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
  QueueExecution& execution_;
};

QueryResultReader::QueryResultReader() = default;
QueryResultReader::~QueryResultReader() = default;
QueryResultReader::QueryResultReader(QueryResultReader&&) noexcept = default;
QueryResultReader& QueryResultReader::operator=(QueryResultReader&&) noexcept =
    default;
QueryResultReader::QueryResultReader(std::unique_ptr<QueueExecution> execution,
                                     std::vector<int> columns)
    : execution_(std::move(execution)), output_columns_(std::move(columns)) {}
QueryResultReader::NextResult QueryResultReader::Next() {
  if (error_) {
    return tl::unexpected(*error_);
  }
  if (!execution_) {
    return std::optional<ContextChunk>{};
  }
  auto output = CaptureWork([&] { return execution_->Next(); });
  if (!output) {
    error_ = output.error();
    execution_.reset();
  } else if (!*output) {
    execution_.reset();
  }
  return output;
}

result<Context> Pipeline::Execute(IStorageInterface& graph, Context&& ctx,
                                  const ParamsMap& params, OprTimer* timer) {
  return materialize(ExecuteReader(graph, std::move(ctx), params, timer));
}
QueryResultReader Pipeline::ExecuteReader(IStorageInterface& storage,
                                          Context input,
                                          const ParamsMap& params,
                                          OprTimer* timer, size_t workers) {
  if (!workers) {
    QueryResultReader reader;
    reader.error_ = Status(StatusCode::ERR_INVALID_ARGUMENT,
                           "Execution requires at least one worker");
    return reader;
  }
  auto execution =
      std::make_unique<QueueExecution>(storage.writable() ? 1 : workers);
  auto columns = std::move(input.tag_ids);
  auto* source = execution->Add<InputTask>(std::move(input.chunks()));
  auto fragment = PipelineBuilder(storage, params, *execution)
                      .Build(*this, {source, {}, std::move(columns)}, timer);
  execution->SetRoot(fragment.output);
  return QueryResultReader(std::move(execution), std::move(fragment.columns));
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
