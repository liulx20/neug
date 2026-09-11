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
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "neug/execution/execute/task_scheduler.h"
#include "neug/utils/result.h"

namespace neug::execution {

// Execution-owned DAG. Operators declare inputs; the builder creates these
// nodes. The coordinator submits ready nodes. Conditional groups use an inline
// task runner so their coordinator never waits on the enclosing worker pool.
class PipelineGraph {
 public:
  using NodeId = size_t;
  NodeId Add(std::string name, std::vector<NodeId> dependencies,
             std::function<Status()> run) {
    if (executed_) {
      throw std::logic_error("Cannot extend an executing pipeline graph");
    }
    std::sort(dependencies.begin(), dependencies.end());
    dependencies.erase(std::unique(dependencies.begin(), dependencies.end()),
                       dependencies.end());
    for (auto dependency : dependencies) {
      if (dependency >= nodes_.size()) {
        throw std::invalid_argument(
            "Pipeline dependency must precede its consumer");
      }
    }
    nodes_.push_back(
        {std::move(name), std::move(dependencies), std::move(run)});
    return nodes_.size() - 1;
  }

  size_t size() const { return nodes_.size(); }
  const std::vector<NodeId>& dependencies(NodeId id) const {
    return nodes_.at(id).dependencies;
  }

  Status Execute(TaskScheduler& scheduler) {
    std::vector<NodeId> targets;
    for (NodeId id = 0; id < nodes_.size(); ++id) {
      targets.push_back(id);
    }
    return Execute(scheduler, targets);
  }

  Status Execute(TaskScheduler& scheduler, const std::vector<NodeId>& targets) {
    if (executed_) {
      if (exception_) {
        std::rethrow_exception(exception_);
      }
      return status_;
    }
    std::vector<bool> active(nodes_.size(), false);
    for (auto target : targets) {
      if (target >= nodes_.size()) {
        throw std::invalid_argument("Unknown pipeline target");
      }
      active[target] = true;
    }
    for (size_t i = nodes_.size(); i > 0; --i) {
      if (active[i - 1]) {
        for (auto dependency : nodes_[i - 1].dependencies) {
          active[dependency] = true;
        }
      }
    }
    struct Completion {
      NodeId id = 0;
      Status status;
      std::exception_ptr exception;
    };
    struct Completions {
      std::mutex mutex;
      std::condition_variable ready;
      std::vector<Completion> slots;
      size_t written = 0;
      size_t read = 0;
    };
    auto completions = std::make_shared<Completions>();
    completions->slots.resize(nodes_.size());
    std::vector<size_t> pending(nodes_.size());
    std::vector<std::vector<NodeId>> successors(nodes_.size());
    std::deque<NodeId> ready;
    for (NodeId id = 0; id < nodes_.size(); ++id) {
      if (!active[id]) {
        continue;
      }
      pending[id] = nodes_[id].dependencies.size();
      if (pending[id] == 0) {
        ready.push_back(id);
      }
      for (auto dependency : nodes_[id].dependencies) {
        successors[dependency].push_back(id);
      }
    }
    executed_ = true;
    size_t running = 0;
    try {
      while (!ready.empty() || running != 0) {
        while (!ready.empty() && status_ && !exception_) {
          auto id = ready.front();
          ready.pop_front();
          try {
            scheduler.Submit([id, run = nodes_[id].run, completions] {
              Completion completion;
              completion.id = id;
              try {
                completion.status = run();
              } catch (...) { completion.exception = std::current_exception(); }
              {
                std::lock_guard<std::mutex> lock(completions->mutex);
                completions->slots[completions->written++] =
                    std::move(completion);
              }
              completions->ready.notify_one();
            });
            ++running;
          } catch (...) { exception_ = std::current_exception(); }
        }
        if (!status_ || exception_) {
          ready.clear();
        }
        if (running == 0) {
          break;
        }
        Completion completion;
        {
          std::unique_lock<std::mutex> lock(completions->mutex);
          completions->ready.wait(
              lock, [&] { return completions->read != completions->written; });
          completion = std::move(completions->slots[completions->read++]);
        }
        --running;
        nodes_[completion.id].run = {};
        if (completion.exception && !exception_) {
          exception_ = completion.exception;
        }
        if (!completion.status && status_) {
          status_ = Status(completion.status.error_code(),
                           "Pipeline [" + nodes_[completion.id].name +
                               "]: " + completion.status.error_message());
        }
        if (status_ && !exception_) {
          for (auto successor : successors[completion.id]) {
            if (--pending[successor] == 0) {
              ready.push_back(successor);
            }
          }
        }
      }
    } catch (...) {
      exception_ = std::current_exception();
      // Coordinator-side failures (including queue allocation) must also drain
      // submitted work before releasing operator states or graph references.
      while (running != 0) {
        std::unique_lock<std::mutex> lock(completions->mutex);
        completions->ready.wait(
            lock, [&] { return completions->read != completions->written; });
        ++completions->read;
        --running;
      }
    }
    // Release unscheduled states only after every submitted task has completed.
    for (auto& node : nodes_) {
      node.run = {};
    }
    if (exception_) {
      std::rethrow_exception(exception_);
    }
    return status_;
  }

 private:
  struct Node {
    std::string name;
    std::vector<NodeId> dependencies;
    std::function<Status()> run;
  };
  std::vector<Node> nodes_;
  bool executed_ = false;
  Status status_ = Status::OK();
  std::exception_ptr exception_;
};
}  // namespace neug::execution
