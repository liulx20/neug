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
#include "neug/execution/execute/pipeline.h"

namespace neug::execution {
inline Context context_from_batches(ChunkBatch chunks,
                                    std::vector<int> columns = {}) {
  Context input;
  input.tag_ids = std::move(columns);
  for (auto& chunk : chunks) {
    input.append_chunk(std::move(chunk));
  }
  return input;
}
inline result<ContextChunk> collect_chunk(Context input) {
  std::optional<ContextChunk> output;
  for (auto& chunk : input.chunks()) {
    output = output ? output->union_with(chunk) : std::move(chunk);
  }
  return output ? std::move(*output) : ContextChunk{};
}
inline result<ContextChunk> collect_chunk(QueryResultReader reader) {
  GS_AUTO(output, materialize(std::move(reader)));
  return collect_chunk(std::move(output));
}
inline result<ChunkBatch> collect_batches(QueryResultReader reader) {
  GS_AUTO(output, materialize(std::move(reader)));
  return std::move(output.chunks());
}
// Test input is a real source operator, not an alternate pipeline pull API.
inline Pipeline PrependInput(Pipeline pipeline,
                             ChunkMorselSource::ReadBatch read) {
  class Source final : public MorselSourceOperator {
   public:
    explicit Source(ChunkMorselSource::ReadBatch read)
        : read_(std::move(read)) {}
    bool consumes_input() const override { return false; }
    std::string get_operator_name() const override { return "TestInput"; }
    std::unique_ptr<MorselSource> CreateMorselSource(
        IStorageInterface&, const ParamsMap&) override {
      return std::make_unique<ChunkMorselSource>(read_);
    }

   private:
    ChunkMorselSource::ReadBatch read_;
  };
  class Child final : public IOperator {
   public:
    explicit Child(Pipeline pipeline) : pipeline_(std::move(pipeline)) {}
    std::string get_operator_name() const override { return "TestChild"; }
    SubPipelines sub_pipelines() override {
      return {SubPipelineMode::kStreaming, {&pipeline_}};
    }
    Kernel CreateState(IStorageInterface&, const ParamsMap&,
                       OprTimer*) override {
      return make_chunk_kernel(
          [](ContextChunk input) -> result<ContextChunk> { return input; });
    }

   private:
    Pipeline pipeline_;
  };
  std::vector<std::unique_ptr<IOperator>> ops;
  ops.push_back(std::make_unique<Source>(std::move(read)));
  ops.push_back(std::make_unique<Child>(std::move(pipeline)));
  return Pipeline(std::move(ops));
}
}  // namespace neug::execution
