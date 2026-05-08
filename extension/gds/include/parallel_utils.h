/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <bthread/bthread.h>
#include <algorithm>
#include <atomic>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {
namespace gds {
struct ParallelUtils {
  template <typename FUNC>
  static void parallel_for(vid_t start, vid_t end, const FUNC& func,
                           int concurrency, int chunk_size = 1024) {
    if (concurrency <= 0) {
      concurrency = std::thread::hardware_concurrency();
    }
    if (concurrency <= 1) {
      for (vid_t v = start; v < end; ++v) {
        func(v);
      }
      return;
    }
    if (chunk_size <= 0) {
      chunk_size = 1;
    }

    struct WorkerArgs {
      std::atomic<vid_t>* current;
      vid_t end;
      int chunk_size;
      const FUNC* func;
    };
    std::atomic<vid_t> current(start);
    std::vector<bthread_t> threads(concurrency - 1);
    std::vector<WorkerArgs> args(concurrency - 1);
    for (int i = 0; i < concurrency - 1; ++i) {
      args[i] = WorkerArgs{&current, end, chunk_size, &func};
      bthread_start_urgent(
          &threads[i], nullptr,
          [](void* arg) -> void* {
            auto* params = static_cast<WorkerArgs*>(arg);
            while (true) {
              vid_t local_start =
                  params->current->fetch_add(params->chunk_size);
              if (local_start >= params->end) {
                break;
              }
              vid_t local_end = std::min<vid_t>(
                  local_start + params->chunk_size, params->end);
              for (vid_t v = local_start; v < local_end; ++v) {
                (*params->func)(v);
              }
            }
            return nullptr;
          },
          &args[i]);
    }
    // Main thread also participates in processing
    while (true) {
      vid_t local_start = current.fetch_add(chunk_size);
      if (local_start >= end)
        break;
      vid_t local_end = std::min<vid_t>(local_start + chunk_size, end);
      for (vid_t v = local_start; v < local_end; ++v) {
        func(v);
      }
    }
    for (auto& thread : threads) {
      bthread_join(thread, nullptr);
    }
  }

  template <typename FUNC>
  static void parallel_for(const vid_t* items, size_t size, const FUNC& func,
                           int concurrency, int chunk_size = 1024) {
    if (concurrency <= 0) {
      concurrency = std::thread::hardware_concurrency();
    }
    if (concurrency <= 1) {
      for (size_t i = 0; i < size; ++i) {
        func(items[i]);
      }
      return;
    }
    struct WorkerArgs {
      const vid_t* items;
      std::atomic<size_t>* current;
      size_t size;
      int chunk_size;
      const FUNC* func;
    };
    std::atomic<size_t> current(0);
    std::vector<bthread_t> threads(concurrency - 1);
    std::vector<WorkerArgs> args(concurrency - 1);
    for (int i = 0; i < concurrency - 1; ++i) {
      args[i] = WorkerArgs{items, &current, size, chunk_size, &func};
      bthread_start_urgent(
          &threads[i], nullptr,
          [](void* arg) -> void* {
            auto* params = static_cast<WorkerArgs*>(arg);
            while (true) {
              size_t local_start =
                  params->current->fetch_add(params->chunk_size);
              if (local_start >= params->size) {
                break;
              }
              size_t local_end =
                  std::min(local_start + params->chunk_size, params->size);
              for (size_t i = local_start; i < local_end; ++i) {
                (*params->func)(params->items[i]);
              }
            }
            return nullptr;
          },
          &args[i]);
    }
    // Main thread also participates in processing
    while (true) {
      size_t local_start = current.fetch_add(chunk_size);
      if (local_start >= size)
        break;
      size_t local_end = std::min(local_start + chunk_size, size);
      for (size_t i = local_start; i < local_end; ++i) {
        func(items[i]);
      }
    }
    for (auto& thread : threads) {
      bthread_join(thread, nullptr);
    }
  }
};
}  // namespace gds
}  // namespace neug