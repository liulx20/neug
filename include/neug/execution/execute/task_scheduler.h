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

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace neug::execution {

// Workers execute ready tasks only. PipelineGraph manages dependencies.
// Conditional groups run local ready tasks inline without waiting on the pool.
class TaskScheduler {
 public:
  // Conditional subgraphs execute ready tasks inline on their current worker.
  // They use the same graph runner without waiting on the enclosing pool.
  enum class Mode { kWorkerPool, kInline };
  explicit TaskScheduler(size_t workers, Mode mode = Mode::kWorkerPool)
      : worker_count_(workers), mode_(mode) {
    if (workers == 0) {
      throw std::invalid_argument("TaskScheduler needs at least one worker");
    }
  }
  ~TaskScheduler() { Stop(); }
  TaskScheduler(const TaskScheduler&) = delete;
  TaskScheduler& operator=(const TaskScheduler&) = delete;

  template <typename F>
  auto Submit(F&& fn) {
    using T = std::invoke_result_t<F>;
    auto task = std::make_shared<std::packaged_task<T()>>(std::forward<F>(fn));
    auto future = task->get_future();
    if (mode_ == Mode::kInline) {
      (*task)();
      return future;
    }
    Start();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) {
        throw std::runtime_error("TaskScheduler is stopped");
      }
      if (current_ == this) {
        // Finish the current step's child work before unrelated ready nodes.
        // This also preserves serial transaction order with one worker.
        tasks_.emplace_front([task] { (*task)(); });
      } else {
        tasks_.emplace_back([task] { (*task)(); });
      }
    }
    ready_.notify_one();
    return future;
  }

  size_t concurrency() const {
    return mode_ == Mode::kInline ? 1 : worker_count_;
  }

  // Pipeline-step coordination can run on a pool worker. While waiting for
  // its finite work batch, that worker helps the queue instead of occupying a
  // thread that its children need. SQL operators never call this interface.
  template <typename T>
  T Wait(std::future<T>& future) {
    while (current_ == this && future.wait_for(std::chrono::seconds(0)) !=
                                   std::future_status::ready) {
      std::function<void()> task;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!tasks_.empty()) {
          task = std::move(tasks_.front());
          tasks_.pop_front();
        }
      }
      if (task) {
        task();
      } else {
        future.wait_for(std::chrono::milliseconds(1));
      }
    }
    return future.get();
  }

 private:
  void Start() {
    std::call_once(start_, [this] {
      try {
        for (size_t i = 0; i < worker_count_; ++i) {
          workers_.emplace_back([this] { Run(); });
        }
      } catch (...) {
        Stop();
        throw;
      }
    });
  }
  void Run() {
    current_ = this;
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [&] { return stopping_ || !tasks_.empty(); });
        if (tasks_.empty()) {
          break;
        }
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      task();
    }
  }
  void Stop() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    ready_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

  inline static thread_local TaskScheduler* current_ = nullptr;
  size_t worker_count_;
  Mode mode_;
  std::once_flag start_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<std::function<void()>> tasks_;
  std::vector<std::thread> workers_;
  bool stopping_ = false;
};

}  // namespace neug::execution
