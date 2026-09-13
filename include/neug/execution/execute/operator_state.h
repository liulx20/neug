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

#include <functional>
#include <memory>
#include <optional>
#include "neug/execution/common/context.h"

namespace neug::execution {
using ChunkBatch = std::vector<ContextChunk>;
using KernelResult = result<ChunkBatch>;

inline ChunkBatch one_chunk(ContextChunk chunk) {
  ChunkBatch output;
  output.push_back(std::move(chunk));
  return output;
}

// Execution-owned state. No upstream input, reader, scheduler or task handle.
// The driver supplies chunks and calls Finalize exactly once on input
// completion.
class OperatorState {
 public:
  virtual ~OperatorState() = default;
  virtual KernelResult Process(ContextChunk chunk) = 0;
  virtual KernelResult Finalize() { return ChunkBatch{}; }
  std::vector<int> input_columns;
  virtual bool Finished() const { return false; }
};
using Kernel = std::unique_ptr<OperatorState>;

template <typename F>
Kernel make_chunk_kernel(F function) {
  class State final : public OperatorState {
   public:
    explicit State(F function) : function_(std::move(function)) {}
    KernelResult Process(ContextChunk chunk) override {
      GS_AUTO(output, function_(std::move(chunk)));
      return one_chunk(std::move(output));
    }

   private:
    F function_;
  };
  return std::make_unique<State>(std::move(function));
}

template <typename F>
Kernel make_global_kernel(F function) {
  class State final : public OperatorState {
   public:
    explicit State(F function) : function_(std::move(function)) {}
    KernelResult Process(ContextChunk chunk) override {
      if (input_) {
        *input_ = input_->union_with(chunk);
      } else {
        input_ = std::move(chunk);
      }
      return ChunkBatch{};
    }
    KernelResult Finalize() override {
      GS_AUTO(output, function_(input_ ? std::move(*input_) : ContextChunk{}));
      input_.reset();
      return one_chunk(std::move(output));
    }

   private:
    F function_;
    std::optional<ContextChunk> input_;
  };
  return std::make_unique<State>(std::move(function));
}

// Explicit materialization for commands that must stabilize input before
// mutation.
template <typename F>
Kernel make_batch_kernel(F function) {
  class State final : public OperatorState {
   public:
    explicit State(F function) : function_(std::move(function)) {}
    KernelResult Process(ContextChunk chunk) override {
      input_.push_back(std::move(chunk));
      return ChunkBatch{};
    }
    KernelResult Finalize() override { return function_(std::move(input_)); }

   private:
    F function_;
    ChunkBatch input_;
  };
  return std::make_unique<State>(std::move(function));
}

// A command/source runs after its input dependencies complete, without
// retaining rows.
template <typename F>
Kernel make_once_kernel(F function) {
  class State final : public OperatorState {
   public:
    explicit State(F function) : function_(std::move(function)) {}
    KernelResult Process(ContextChunk) override { return ChunkBatch{}; }
    KernelResult Finalize() override { return function_(); }

   private:
    F function_;
  };
  return std::make_unique<State>(std::move(function));
}
template <typename F>
Kernel make_source_kernel(F function) {
  return make_once_kernel(
      [function = std::move(function)]() mutable -> KernelResult {
        GS_AUTO(output, function());
        return one_chunk(std::move(output));
      });
}

template <typename F>
Kernel make_context_kernel(F function) {
  class State final : public OperatorState {
   public:
    explicit State(F function) : function_(std::move(function)) {}
    KernelResult Process(ContextChunk chunk) override {
      input_.append_chunk(std::move(chunk));
      return ChunkBatch{};
    }
    KernelResult Finalize() override {
      input_.tag_ids = input_columns;
      GS_AUTO(output, function_(std::move(input_)));
      return std::move(output.chunks());
    }

   private:
    F function_;
    Context input_;
  };
  return std::make_unique<State>(std::move(function));
}
}  // namespace neug::execution
