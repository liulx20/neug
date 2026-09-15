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
#include <atomic>
#include <deque>
#include <map>
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
  virtual void Completed() {}
  virtual void Cancel() {}
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
  explicit QueueExecution(size_t workers, std::shared_ptr<TaskPool> pool)
      : scheduler_(workers, std::move(pool)) {}
  ~QueueExecution() {
    Drain();
    // A completion event can wake the coordinator before its callback returns.
    // Join this query's callbacks while completion mutex/state still exist.
    scheduler_.Drain();
  }
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
  void Submit(PipelineTask& owner, F work, PipelineTask* resume = nullptr) {
    ++owner.running;
    ++active_;
    try {
      scheduler_.Submit(
          [this, &owner, resume, work = std::move(work)]() mutable {
            auto captured = CaptureWork([&]() -> result<bool> {
              auto status = work();
              if (!status) {
                return tl::unexpected(status);
              }
              return true;
            });
            std::lock_guard<std::mutex> lock(mutex_);
            completed_.push_back(
                {&owner, resume, captured ? Status::OK() : captured.error()});
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
    for (auto& task : tasks_) {
      task->Cancel();
    }
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
      --event.owner->running;
      --active_;
      if (!event.owner->running) {
        event.owner->Completed();
        Enqueue(*event.owner);
      }
      if (event.resume) {
        Enqueue(*event.resume);
      }
      if (!event.status && !error_) {
        error_ = event.status;
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
  struct Completion {
    PipelineTask* owner;
    PipelineTask* resume;
    Status status;
  };
  std::vector<Completion> completed_;
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
  MorselPipelineTask(size_t workers, PipelineTask* input)
      : MorselPipelineTask(workers, SourceFactory{}, "IntermediateRanges",
                           nullptr) {
    input_ = input;
  }
  void Append(std::string name, OprTimer* timer, Transform transform,
              std::vector<int> columns = {}) {
    names_.push_back(std::move(name));
    timers_.push_back(timer);
    transforms_.push_back(std::move(transform));
    columns_.push_back(std::move(columns));
  }

  void Cancel() override { cancelled_ = true; }
  bool Advance(QueueExecution& execution) override {
    if (!source_ && input_) {
      if (!input_->output) {
        if (input_->done) {
          finished = true;
          return true;
        }
        return execution.Need(*input_);
      }
      auto chunk = std::move(input_->output);
      input_->output.reset();
      // Intermediate partitions are often only one default range large.
      // Split useful work across lanes without creating sub-1024-row tasks.
      auto range_size = std::min(
          size_t{4096},
          std::max(size_t{1024},
                   (chunk->row_num() + worker_count_ - 1) / worker_count_));
      source_ = std::make_unique<ChunkMorselSource>(
          [chunk =
               std::move(chunk)]() mutable -> QueryResultReader::NextResult {
            return std::exchange(chunk, std::nullopt);
          },
          range_size);
    }
    if (!source_) {
      execution.Submit(*this, [this] {
        source_ = factory_();
        factory_ = {};
        if (!source_) {
          return Status::InternalError("Missing morsel source");
        }
        return Status::OK();
      });
      return true;
    }
    if (workers_.empty()) {
      for (size_t lane = 0; lane < worker_count_; ++lane) {
        workers_.push_back(execution.Add<LocalState>(*this));
      }
    }
    auto ready = completed_ranges_.find(deliver_sequence_);
    if (ready != completed_ranges_.end()) {
      auto output = std::move(ready->second);
      completed_ranges_.erase(ready);
      ++deliver_sequence_;
      if (!output) {
        pending.emplace_back(tl::unexpected(output.error()));
        finished = true;
        return true;
      }
      Publish(*this, std::move(*output));
      // Refill only while serving demand, never from completion callbacks.
      // A single worker retains exact pull-through behavior for sequential IO.
      if (worker_count_ > 1) {
        FillSlots(execution);
      }
      return true;
    }
    if (exhausted_ && in_flight_ == 0 && input_) {
      // The current input chunk has been consumed and every range delivered.
      // Advance upstream only on downstream demand; never drain it eagerly.
      for (auto* worker : workers_) {
        worker->reader.reset();
      }
      source_.reset();
      exhausted_ = false;
      return true;
    }
    if (exhausted_ && in_flight_ == 0) {
      execution.Submit(*this, [this] {
        auto status = source_->Finalize();
        finished = true;
        return status;
      });
      return true;
    }
    return FillSlots(execution);
  }

 private:
  struct LocalState final : public PipelineTask {
    explicit LocalState(MorselPipelineTask& owner) : owner(owner) {}
    bool Advance(QueueExecution&) override { return false; }
    void Completed() override {
      --owner.in_flight_;
      for (size_t i = 0; i < timers.size(); ++i) {
        if (owner.timers_[i]) {
          owner.timers_[i]->add_local_metrics(*timers[i]);
        }
      }
      if (sequence) {
        owner.completed_ranges_.emplace(*sequence, std::move(result));
        sequence.reset();
      }
    }
    MorselPipelineTask& owner;
    std::unique_ptr<MorselReader> reader;
    std::vector<std::unique_ptr<OprTimer>> timers;
    std::optional<size_t> sequence;
    KernelResult result = ChunkBatch{};
  };

  bool FillSlots(QueueExecution& execution) {
    bool submitted = false;
    for (auto* local : workers_) {
      if (exhausted_ || failed_ ||
          in_flight_ + completed_ranges_.size() >= worker_count_) {
        break;
      }
      if (local->running) {
        continue;
      }
      ++in_flight_;
      try {
        execution.Submit(
            *local,
            [this, local] {
              RunTask(*local);
              return Status::OK();
            },
            this);
      } catch (...) {
        --in_flight_;
        throw;
      }
      submitted = true;
    }
    return submitted;
  }

  result<std::vector<ContextChunk>> RunMorsel(LocalState& local,
                                              const Morsel& work) {
    if (!local.reader) {
      local.reader = source_->CreateReader();
    }
    local.reader->Start(work);
    std::vector<KernelStep> steps;
    for (size_t i = 1; i < names_.size(); ++i) {
      steps.push_back({names_[i], local.timers[i].get(), transforms_[i - 1],
                       columns_[i - 1]});
    }
    KernelChain chain(std::move(steps));
    ChunkBatch output;
    while (true) {
      if (cancelled_) {
        return ChunkBatch{};
      }
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

  void RunTask(LocalState& local) {
    local.timers.clear();
    for (size_t i = 0; i < names_.size(); ++i) {
      auto timer = timers_[i] ? std::make_unique<OprTimer>() : nullptr;
      if (timer) {
        timer->set_name(names_[i]);
      }
      local.timers.push_back(std::move(timer));
    }
    result<std::optional<Morsel>> work = std::optional<Morsel>{};
    {
      std::lock_guard<std::mutex> lock(pick_mutex_);
      if (exhausted_ || failed_ || cancelled_) {
        return;
      }
      work = CaptureWork([&] { return source_->Pick(); });
      if (work && !*work) {
        exhausted_ = true;
        return;
      }
      local.sequence = next_sequence_++;
      if (!work) {
        failed_ = true;
      }
    }
    if (!work) {
      local.result = tl::unexpected(work.error());
      return;
    }
    local.result = CaptureWork([&] { return RunMorsel(local, **work); });
    if (!local.result) {
      failed_ = true;
    }
  }

  size_t worker_count_;
  PipelineTask* input_ = nullptr;
  SourceFactory factory_;
  std::unique_ptr<MorselSource> source_;
  std::vector<std::string> names_;
  std::vector<OprTimer*> timers_;
  std::vector<Transform> transforms_;
  std::vector<std::vector<int>> columns_;
  std::vector<LocalState*> workers_;
  std::mutex pick_mutex_;
  std::atomic<bool> exhausted_{false};
  std::atomic<bool> failed_{false};
  std::atomic<bool> cancelled_{false};
  size_t next_sequence_ = 0;
  size_t deliver_sequence_ = 0;
  size_t in_flight_ = 0;
  std::map<size_t, KernelResult> completed_ranges_;
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
// Partition accumulation uses the same bounded slots for Join and reductions.
// State construction is deferred so unused branches allocate no operator state.
class PartitionPipelineTask final : public PipelineTask {
 public:
  using Factory = std::function<std::shared_ptr<PartitionState>()>;
  PartitionPipelineTask(PipelineTask* input, Factory factory, OprTimer* timer)
      : input_(input), factory_(std::move(factory)), timer_(timer) {}
  BuildProbeState& probe_state() const {
    return static_cast<BuildProbeState&>(*state_);
  }

  bool Advance(QueueExecution& execution) override {
    if (!state_) {
      execution.Submit(*this, [this] {
        return Timed([&] {
          state_ = factory_();
          factory_ = {};
          return Status::OK();
        });
      });
      return true;
    }
    if (slots_.empty()) {
      for (size_t i = 0; i < execution.workers(); ++i) {
        slots_.push_back(execution.Add<Slot>(timer_));
      }
      for (size_t i = 0; i < state_->BuildPartitions(); ++i) {
        lanes_.push_back(execution.Add<Lane>(timer_));
      }
      if (lanes_.empty()) {
        throw std::logic_error("Build requires at least one partition");
      }
    }
    while (!pending_.empty() && pending_.front()->remaining == 0) {
      auto* slot = pending_.front();
      slot->batch.reset();
      slot->occupied = false;
      pending_.pop_front();
    }
    bool progress = false;
    // One job per bucket at a time. Buckets progress independently, but each
    // consumes batches in input order regardless of partition-task completion.
    for (size_t part = 0; part < lanes_.size(); ++part) {
      auto* lane = lanes_[part];
      if (lane->running) {
        continue;
      }
      for (auto* slot : pending_) {
        if (slot->sequence != lane->sequence || slot->running ||
            !slot->partitioned) {
          continue;
        }
        if (!slot->batch) {
          ++lane->sequence;
          --slot->remaining;
          progress = true;
          continue;
        }
        lane->slot = slot;
        execution.Submit(
            *lane,
            [this, lane, slot, part] {
              return lane->Measure(
                  [&] { return state_->BuildPartition(part, *slot->batch); });
            },
            this);
        progress = true;
        break;
      }
    }
    if (input_->output && pending_.size() < slots_.size()) {
      auto* slot = *std::find_if(slots_.begin(), slots_.end(),
                                 [](Slot* item) { return !item->occupied; });
      slot->occupied = true;
      slot->partitioned = false;
      slot->sequence = sequence_++;
      slot->remaining = lanes_.size();
      auto chunk = std::move(*input_->output);
      input_->output.reset();
      pending_.push_back(slot);
      execution.Submit(
          *slot,
          [this, slot, chunk = std::move(chunk)]() mutable {
            return slot->Measure([&] {
              slot->batch = state_->PartitionBuild(std::move(chunk));
              return Status::OK();
            });
          },
          this);
      progress = true;
    }
    if (input_->done && pending_.empty()) {
      if (!finalizers_started_) {
        finalizers_started_ = true;
        for (size_t part = 0; part < state_->FinalizePartitions(); ++part) {
          auto* finalizer = execution.Add<Finalizer>(timer_);
          finalizers_.push_back(finalizer);
          execution.Submit(
              *finalizer,
              [this, finalizer, part] {
                return finalizer->Measure(
                    [&] { return state_->FinalizePartition(part); });
              },
              this);
        }
        if (!finalizers_.empty()) {
          return true;
        }
      }
      for (auto* finalizer : finalizers_) {
        if (!finalizer->complete) {
          return false;
        }
      }
      execution.Submit(*this, [this] {
        auto status = Timed([&] {
          auto status = state_->FinalizeBuild();
          if (status) {
            auto output = state_->TakeOutput();
            if (timer_) {
              for (const auto& chunk : output) {
                timer_->add_num_tuples(chunk.row_num());
              }
            }
            Publish(*this, std::move(output));
          }
          return status;
        });
        finished = true;
        return status;
      });
      return true;
    }
    if (!input_->done && pending_.size() < slots_.size()) {
      progress = execution.Need(*input_) || progress;
    }
    return progress;
  }

 private:
  struct Work : PipelineTask {
    explicit Work(OprTimer* timer) : timer(timer) {}
    bool Advance(QueueExecution&) override { return false; }
    template <typename F>
    Status Measure(F work) {
      TimerUnit clock;
      if (timer) {
        clock.start();
      }
      // Capture exceptions here too, so failed work still contributes timing.
      auto output = CaptureWork([&]() -> result<bool> {
        auto status = work();
        if (!status) {
          return tl::unexpected(status);
        }
        return true;
      });
      elapsed = timer ? clock.elapsed() : 0;
      return output ? Status::OK() : output.error();
    }
    void Completed() override {
      if (timer) {
        timer->add_elapsed(elapsed);
      }
    }
    OprTimer* timer;
    double elapsed = 0;
  };
  struct Finalizer final : Work {
    using Work::Work;
    void Completed() override {
      Work::Completed();
      complete = true;
    }
    bool complete = false;
  };
  struct Slot final : Work {
    using Work::Work;
    void Completed() override {
      Work::Completed();
      partitioned = true;
    }
    bool partitioned = false;
    std::shared_ptr<PartitionState::Batch> batch;
    size_t sequence = 0;
    size_t remaining = 0;
    bool occupied = false;
  };
  struct Lane final : Work {
    using Work::Work;
    void Completed() override {
      Work::Completed();
      --slot->remaining;
      ++sequence;
      slot = nullptr;
    }
    Slot* slot = nullptr;
    size_t sequence = 0;
  };
  template <typename F>
  Status Timed(F work) {
    if (!timer_) {
      return work();
    }
    double charged = 0;
    PhaseTimerScope scope(*timer_, charged);
    return work();
  }
  PipelineTask* input_;
  Factory factory_;
  OprTimer* timer_;
  std::shared_ptr<PartitionState> state_;
  std::vector<Slot*> slots_;
  std::vector<Lane*> lanes_;
  std::vector<Finalizer*> finalizers_;
  bool finalizers_started_ = false;
  std::deque<Slot*> pending_;
  size_t sequence_ = 0;
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
  BufferTask* replay_buffer = nullptr;
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
        fragment.replay_buffer = nullptr;
        fragment.output = execution_.Add<InputTask>(ChunkBatch{});
        fragment.output->gates = fragment.barriers;
        fragment.columns.clear();
        fragment.linear_step = nullptr;
        fragment.morsel_step = nullptr;
      }
      auto* storage = &storage_;
      auto params = params_;
      auto* operator_plan = &op;
      if (op.pipeline_behavior() == PipelineBehavior::kPartitioned) {
        fragment.output = execution_.Add<PartitionPipelineTask>(
            fragment.output,
            [operator_plan, workers = execution_.workers()] {
              return operator_plan->CreatePartitionState(workers);
            },
            current_timer);
        fragment.morsel_step = nullptr;
        fragment.linear_step = nullptr;
        fragment.replay_buffer = nullptr;
        if (auto columns = op.output_columns()) {
          fragment.columns = std::move(*columns);
        }
        AdvanceTimer(current_timer, i, plan.operators_.size());
        continue;
      }
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
      if (op.consumes_input() &&
          op.pipeline_behavior() == PipelineBehavior::kChunkLocal) {
        Parallelize(fragment);
      }
      if (fragment.morsel_step &&
          op.pipeline_behavior() == PipelineBehavior::kChunkLocal) {
        fragment.morsel_step->Append(
            name, current_timer,
            [operator_plan, storage, params](OprTimer* timer) {
              return operator_plan->CreateState(*storage, params, timer);
            },
            fragment.columns);
        if (auto columns = op.output_columns()) {
          fragment.columns = std::move(*columns);
        }
        AdvanceTimer(current_timer, i, plan.operators_.size());
        continue;
      }
      fragment.morsel_step = nullptr;
      auto children = op.sub_pipelines();
      PartitionPipelineTask* build_state = nullptr;
      if (children.mode == SubPipelineMode::kBuildProbe) {
        if (children.plans.size() != 2) {
          throw std::logic_error("Build/probe requires two inputs");
        }
        auto* seed = execution_.Add<BufferTask>(fragment.output);
        auto* left_timer = ChildTimer(current_timer);
        auto* right_timer = ChildTimer(current_timer);
        auto right = Build(children.build_plan(),
                           Replay(seed, fragment.columns, {seed}), right_timer);
        build_state = execution_.Add<PartitionPipelineTask>(
            right.output,
            [operator_plan, workers = execution_.workers()] {
              return operator_plan->CreateBuildState(workers);
            },
            current_timer);
        auto left =
            Build(children.probe_plan(),
                  Replay(seed, fragment.columns, {build_state}), left_timer);
        Parallelize(left);
        fragment.output = left.output;
        fragment.columns = left.columns;
        fragment.linear_step = nullptr;
        if (left.morsel_step) {
          left.morsel_step->Append(
              name, current_timer,
              [state = build_state](OprTimer*) {
                return make_chunk_kernel([state](ContextChunk chunk) {
                  return state->probe_state().ProbeChunk(std::move(chunk));
                });
              },
              left.columns);
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
      fragment.replay_buffer = nullptr;
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
                return build_state->probe_state().ProbeChunk(std::move(chunk));
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
  void Parallelize(PipelineFragment& fragment) {
    if (fragment.morsel_step || execution_.workers() == 1 ||
        storage_.writable()) {
      return;
    }
    MorselPipelineTask* task;
    if (fragment.replay_buffer) {
      auto* buffer = fragment.replay_buffer;
      task = execution_.Add<MorselPipelineTask>(
          execution_.workers(),
          [buffer] {
            return std::make_unique<ChunkMorselSource>(
                [buffer,
                 index = size_t{0}]() mutable -> QueryResultReader::NextResult {
                  if (index == buffer->chunks.size()) {
                    return std::optional<ContextChunk>{};
                  }
                  return std::optional<ContextChunk>{buffer->chunks[index++]};
                });
          },
          "ReplayRanges", nullptr);
      task->gates = fragment.output->gates;
    } else {
      task = execution_.Add<MorselPipelineTask>(execution_.workers(),
                                                fragment.output);
    }
    fragment.output = task;
    fragment.morsel_step = task;
    fragment.linear_step = nullptr;
    fragment.replay_buffer = nullptr;
  }
  PipelineFragment Replay(BufferTask* buffer, const std::vector<int>& columns,
                          std::vector<PipelineTask*> gates) {
    auto* task = execution_.Add<ReplayTask>(buffer);
    task->gates.insert(task->gates.end(), gates.begin(), gates.end());
    return {task, std::move(gates), columns, nullptr, nullptr, buffer};
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
                                          OprTimer* timer, size_t workers,
                                          std::shared_ptr<TaskPool> pool) {
  if (!workers) {
    QueryResultReader reader;
    reader.error_ = Status(StatusCode::ERR_INVALID_ARGUMENT,
                           "Execution requires at least one worker");
    return reader;
  }
  auto execution = std::make_unique<QueueExecution>(
      storage.writable() ? 1 : workers, std::move(pool));
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
