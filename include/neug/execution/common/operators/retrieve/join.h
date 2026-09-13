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

#include <memory>

#include "neug/common/types/graph_types.h"
#include "neug/utils/result.h"

namespace neug {
class IStorageInterface;
namespace execution {
class ContextChunk;
struct JoinParams;

// Immutable build-side data, reusable across all probe chunks in one execution.
class JoinTable {
 public:
  JoinTable(ContextChunk right, const JoinParams& params);
  ~JoinTable();
  struct Batch;
  JoinTable(const JoinParams& params, size_t partitions);
  // Partition calls may run concurrently. Append each batch to every bucket
  // in input order, with at most one active append per bucket. Finalize after
  // all appends complete; published tables support concurrent probes.
  std::shared_ptr<Batch> Partition(ContextChunk input) const;
  Status BuildPartition(size_t partition, const Batch& batch);
  Status Finalize();
  result<ContextChunk> Probe(ContextChunk left) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class Join {
 public:
  // Equi-joins build on the right input and probe in left-input order.
  // Duplicate right matches retain their right-input order. The Cartesian
  // product and primary-key lookup retain their separate implementations.
  static neug::result<ContextChunk> join(ContextChunk&& chunk,
                                         ContextChunk&& chunk2,
                                         const JoinParams& params);

  static neug::result<ContextChunk> pk_join(IStorageInterface&,
                                            ContextChunk&& chunk,
                                            const std::vector<label_t>& labels,
                                            int tag, int alias);
};
}  // namespace execution
}  // namespace neug
