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

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace neug::execution {

// Database-owned workers. Ready queries take turns; quotas count running work,
// so a query waiting for its quota never occupies a worker thread.
class TaskPool {
 public:
  struct Query {
    explicit Query(size_t limit) : limit(limit) {}
    size_t limit;
    size_t running = 0;
    bool ready = false;
    std::deque<std::function<void()>> tasks;
  };
  explicit TaskPool(size_t workers) : worker_count_(workers) {
    if (!workers) {
      throw std::invalid_argument("TaskPool needs at least one worker");
    }
  }
  ~TaskPool() { Stop(); }
  size_t concurrency() const { return worker_count_; }
  void Submit(const std::shared_ptr<Query>& query, std::function<void()> task) {
    Start();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) {
        throw std::runtime_error("TaskPool is stopped");
      }
      query->tasks.push_back(std::move(task));
      Ready(query);
    }
    ready_.notify_one();
  }
  void Drain(const std::shared_ptr<Query>& query) {
    std::unique_lock<std::mutex> lock(mutex_);
    idle_.wait(lock, [&] { return query->tasks.empty() && !query->running; });
  }

 private:
  void Ready(const std::shared_ptr<Query>& query) {
    if (!query->ready && !query->tasks.empty() &&
        query->running < query->limit) {
      queries_.push_back(query);
      query->ready = true;
    }
  }
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
      std::shared_ptr<Query> query;
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [&] { return stopping_ || !queries_.empty(); });
        if (queries_.empty()) {
          return;
        }
        query = std::move(queries_.front());
        queries_.pop_front();
        query->ready = false;
        task = std::move(query->tasks.front());
        query->tasks.pop_front();
        ++query->running;
        Ready(query);
      }
      // Execution callbacks translate operator exceptions into query errors.
      task();
      bool idle, ready;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        --query->running;
        Ready(query);
        idle = query->tasks.empty() && query->running == 0;
        ready = !queries_.empty();
      }
      if (idle) {
        idle_.notify_all();
      }
      if (ready) {
        ready_.notify_one();
      }
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
  std::condition_variable ready_, idle_;
  std::deque<std::shared_ptr<Query>> queries_;
  std::vector<std::thread> workers_;
  bool stopping_ = false;
};

// Per-query submission handle. Destruction drains only this query's callbacks.
class TaskScheduler {
 public:
  explicit TaskScheduler(size_t workers, std::shared_ptr<TaskPool> pool = {})
      : pool_(pool ? std::move(pool) : std::make_shared<TaskPool>(workers)),
        query_(std::make_shared<TaskPool::Query>(
            std::min(workers, pool_->concurrency()))) {
    if (!workers) {
      throw std::invalid_argument("TaskScheduler needs at least one worker");
    }
  }
  ~TaskScheduler() { Drain(); }
  void Drain() { pool_->Drain(query_); }
  TaskScheduler(const TaskScheduler&) = delete;
  TaskScheduler& operator=(const TaskScheduler&) = delete;
  void Submit(std::function<void()> task) {
    pool_->Submit(query_, std::move(task));
  }
  size_t concurrency() const { return query_->limit; }

 private:
  std::shared_ptr<TaskPool> pool_;
  std::shared_ptr<TaskPool::Query> query_;
};
}  // namespace neug::execution
