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

// Workers execute ready tasks only. Pipeline dependency management belongs to
// PipelineGraph; tasks never submit or wait for other tasks.
class TaskScheduler {
 public:
  explicit TaskScheduler(size_t workers) : worker_count_(workers) {
    if (workers == 0) {
      throw std::invalid_argument("TaskScheduler needs at least one worker");
    }
  }
  ~TaskScheduler() { Stop(); }
  TaskScheduler(const TaskScheduler&) = delete;
  TaskScheduler& operator=(const TaskScheduler&) = delete;

  template <typename F>
  auto Submit(F&& fn) {
    Start();
    using T = std::invoke_result_t<F>;
    auto task = std::make_shared<std::packaged_task<T()>>(std::forward<F>(fn));
    auto future = task->get_future();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) {
        throw std::runtime_error("TaskScheduler is stopped");
      }
      tasks_.emplace_back([task] { (*task)(); });
    }
    ready_.notify_one();
    return future;
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

  size_t worker_count_;
  std::once_flag start_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<std::function<void()>> tasks_;
  std::vector<std::thread> workers_;
  bool stopping_ = false;
};

}  // namespace neug::execution
