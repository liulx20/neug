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

#include <optional>
#include <utility>
#include <vector>

#include "neug/execution/common/context_chunk.h"

namespace neug::execution {
// Order-preserving accumulation for an associative concatenation operation.
// Level i holds 2^i consecutive input batches. Only equally sized batch groups
// merge during Add, avoiding a copy of the entire prefix for each new batch.
// Each row participates in O(log(batch count)) merges; empty typed batches are
// retained. This bounds copying, not the memory needed for materialization.
template <typename T, typename Merge>
class OrderedBatchAccumulator {
 public:
  explicit OrderedBatchAccumulator(Merge merge = {})
      : merge_(std::move(merge)) {}
  void Add(T batch) {
    size_t level = 0;
    while (level < levels_.size() && levels_[level]) {
      batch = merge_(*levels_[level], batch);
      levels_[level].reset();
      ++level;
    }
    if (level == levels_.size()) {
      levels_.emplace_back();
    }
    levels_[level] = std::move(batch);
  }
  std::optional<T> Finish() {
    std::optional<T> output;
    // Higher occupied levels precede lower ones in the input sequence.
    for (size_t i = levels_.size(); i > 0; --i) {
      auto& batch = levels_[i - 1];
      if (!batch) {
        continue;
      }
      output = output ? merge_(*output, *batch) : std::move(*batch);
      batch.reset();
    }
    levels_.clear();
    return output;
  }

 private:
  Merge merge_;
  std::vector<std::optional<T>> levels_;
};

struct MergeContextChunks {
  ContextChunk operator()(const ContextChunk& left,
                          const ContextChunk& right) const {
    return left.union_with(right);
  }
};
struct MergeContextColumns {
  std::shared_ptr<IContextColumn> operator()(
      const std::shared_ptr<IContextColumn>& left,
      const std::shared_ptr<IContextColumn>& right) const {
    return left->union_col(right);
  }
};
using ChunkAccumulator =
    OrderedBatchAccumulator<ContextChunk, MergeContextChunks>;
using ColumnAccumulator =
    OrderedBatchAccumulator<std::shared_ptr<IContextColumn>,
                            MergeContextColumns>;
}  // namespace neug::execution
