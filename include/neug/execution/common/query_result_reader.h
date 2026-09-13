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

#include "neug/execution/common/context.h"

namespace neug::execution {
class QueueExecution;
class Pipeline;

// Single-consumer query handle. Only the caller waits: workers publish task
// completions and never read another pipeline. Destruction drains active work.
class QueryResultReader {
 public:
  using NextResult = result<std::optional<ContextChunk>>;
  QueryResultReader();
  ~QueryResultReader();
  QueryResultReader(QueryResultReader&&) noexcept;
  QueryResultReader& operator=(QueryResultReader&&) noexcept;
  QueryResultReader(const QueryResultReader&) = delete;
  QueryResultReader& operator=(const QueryResultReader&) = delete;
  NextResult Next();
  const std::vector<int>& output_columns() const { return output_columns_; }

 private:
  friend class Pipeline;
  QueryResultReader(std::unique_ptr<QueueExecution>, std::vector<int>);
  std::unique_ptr<QueueExecution> execution_;
  std::vector<int> output_columns_;
  std::optional<Status> error_;
};

inline result<Context> materialize(QueryResultReader reader) {
  Context output;
  output.tag_ids = reader.output_columns();
  while (true) {
    GS_AUTO(chunk, reader.Next());
    if (!chunk) {
      return output;
    }
    output.append_chunk(std::move(*chunk));
  }
}
}  // namespace neug::execution
