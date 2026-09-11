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
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace neug::execution {

// One queue per execution. Tasks own their execution state; a stream must never
// have two Next tasks in flight. Waiting workers execute queued dependencies so
// nested pipelines also make progress with a single worker.
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

  template <typename T>
  T Wait(std::future<T>& future) {
    if (current_ != this) {
      return future.get();
    }
    while (future.wait_for(std::chrono::seconds(0)) !=
           std::future_status::ready) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [&] {
          return !tasks_.empty() || future.wait_for(std::chrono::seconds(0)) ==
                                        std::future_status::ready;
        });
        if (future.wait_for(std::chrono::seconds(0)) ==
            std::future_status::ready) {
          break;
        }
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      task();
      NotifyCompletion();
    }
    return future.get();
  }

  // The current pipeline task executes the right branch while another worker
  // can run the queued left branch. Packaged tasks retain exceptions until both
  // branches have finished. No sibling task is abandoned on an error path.
  template <typename Left, typename Right>
  auto RunPair(Left left, Right right) {
    using R = std::invoke_result_t<Right>;
    std::packaged_task<R()> right_task(std::move(right));
    auto right_result = right_task.get_future();
    auto left_result = Submit(std::move(left));
    right_task();
    auto left_value = Wait(left_result);
    return std::make_pair(std::move(left_value), right_result.get());
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
      NotifyCompletion();
    }
    current_ = nullptr;
  }
  void NotifyCompletion() {
    // Synchronize completion notification with Wait's condition check.
    std::lock_guard<std::mutex> lock(mutex_);
    ready_.notify_all();
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
  std::once_flag start_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<std::function<void()>> tasks_;
  std::vector<std::thread> workers_;
  bool stopping_ = false;
};

}  // namespace neug::execution
