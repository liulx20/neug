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
#include <optional>
#include <utility>

#include "neug/execution/common/context.h"

namespace neug::execution {

// Result layout, independent of batch contents and execution progress.
// The aliases select output columns in result order, including empty results.
struct StreamMetadata {
  std::vector<int> output_columns;
};

// Per-execution state, never shared by concurrent consumers. Operator objects
// describe the plan; state objects own cursors, inputs and intermediate data.
template <typename T>
class StreamState {
 public:
  virtual ~StreamState() = default;
  virtual result<std::optional<T>> Next() = 0;
};

using OperatorState = StreamState<ContextChunk>;

// A single-consumer, synchronous pull stream. Construction does not read rows.
// The stream yields T directly. Execution uses ContextChunk, which already
// owns the DataChunk and anonymous head. Metadata also describes empty streams.
// EOF and errors are terminal; an empty batch is NOT EOF.
template <typename T>
class Stream {
 public:
  using NextResult = result<std::optional<T>>;
  using Pull = std::function<NextResult()>;

  Stream() = default;
  explicit Stream(Pull pull, StreamMetadata metadata = {})
      : metadata_(std::move(metadata)) {
    if (pull) {
      state_ = std::make_shared<FunctionState>(std::move(pull));
    }
  }
  explicit Stream(std::shared_ptr<StreamState<T>> state,
                  StreamMetadata metadata = {})
      : metadata_(std::move(metadata)), state_(std::move(state)) {}
  Stream(Stream&&) = default;
  Stream& operator=(Stream&&) = default;
  Stream(const Stream&) = delete;
  Stream& operator=(const Stream&) = delete;

  NextResult Next() {
    if (error_) {
      return tl::unexpected(*error_);
    }
    if (!state_) {
      return std::optional<T>{};
    }
    auto output = PullOne();
    if (!output) {
      error_ = output.error();
      state_.reset();
    } else if (!*output) {
      state_.reset();
    }
    return output;
  }

  const StreamMetadata& metadata() const { return metadata_; }
  void set_metadata(StreamMetadata metadata) {
    metadata_ = std::move(metadata);
  }

 private:
  NextResult PullOne() {
    NextResult output = std::optional<T>{};
    TRY_HANDLE_ALL_WITH_EXCEPTION(
        NextResult, [&]() { return state_->Next(); },
        [&](const Status& status) { output = tl::unexpected(status); },
        [&](NextResult&& batch) { output = std::move(batch); });
    return output;
  }

  class FunctionState final : public StreamState<T> {
   public:
    explicit FunctionState(Pull pull) : pull_(std::move(pull)) {}
    NextResult Next() override { return pull_(); }

   private:
    Pull pull_;
  };
  StreamMetadata metadata_;
  std::shared_ptr<StreamState<T>> state_;
  std::optional<Status> error_;
};

// An execution error is observed through Next(), just like a read error.
template <typename T>
Stream<T> error_stream(Status error) {
  return Stream<T>(
      [error = std::move(error)]() ->
      typename Stream<T>::NextResult { return tl::unexpected(error); });
}

// Own execution state until first demand. Initialization and its exceptions
// run inside Stream::Next's error boundary, exactly once.
template <typename Initialize>
Stream<ContextChunk> defer_stream(Stream<ContextChunk> input,
                                  Initialize initialize) {
  auto metadata = input.metadata();
  class DeferredState final : public OperatorState {
   public:
    DeferredState(Stream<ContextChunk> input, Initialize initialize)
        : input_(std::move(input)), initialize_(std::move(initialize)) {}
    Stream<ContextChunk>::NextResult Next() override {
      if (!output_) {
        output_.emplace(initialize_(std::move(input_)));
      }
      return output_->Next();
    }

   private:
    Stream<ContextChunk> input_;
    Initialize initialize_;
    std::optional<Stream<ContextChunk>> output_;
  };
  return Stream<ContextChunk>(
      std::make_shared<DeferredState>(std::move(input), std::move(initialize)),
      std::move(metadata));
}

// Put a batch pulled for initialization back in front of its remaining input.
inline Stream<ContextChunk> prepend_chunk(std::optional<ContextChunk> first,
                                          Stream<ContextChunk> input) {
  if (!first) {
    return std::move(input);
  }
  auto metadata = input.metadata();
  auto pending =
      std::make_shared<std::optional<ContextChunk>>(std::move(first));
  auto upstream = std::make_shared<Stream<ContextChunk>>(std::move(input));
  return Stream<ContextChunk>(
      [pending, upstream]() -> Stream<ContextChunk>::NextResult {
        if (*pending) {
          auto chunk = std::move(*pending);
          pending->reset();
          return chunk;
        }
        return upstream->Next();
      },
      std::move(metadata));
}

// Exactly one upstream pull and one kernel invocation per downstream pull.
template <typename Transform>
Stream<ContextChunk> map_chunks(Stream<ContextChunk> input,
                                Transform transform) {
  auto metadata = input.metadata();
  class MapState final : public OperatorState {
   public:
    MapState(Stream<ContextChunk> input, Transform transform)
        : input_(std::move(input)), transform_(std::move(transform)) {}
    Stream<ContextChunk>::NextResult Next() override {
      GS_AUTO(next, input_.Next());
      if (!next) {
        return std::optional<ContextChunk>{};
      }
      GS_AUTO(output, transform_(std::move(*next)));
      return std::optional<ContextChunk>(std::move(output));
    }

   private:
    Stream<ContextChunk> input_;
    Transform transform_;
  };
  return Stream<ContextChunk>(
      std::make_shared<MapState>(std::move(input), std::move(transform)),
      std::move(metadata));
}

// Invoke a producer on first demand, without an intermediate Context.
template <typename Producer>
Stream<ContextChunk> generate_chunk(Producer producer,
                                    StreamMetadata metadata = {}) {
  class GenerateState final : public OperatorState {
   public:
    explicit GenerateState(Producer producer)
        : producer_(std::move(producer)) {}
    Stream<ContextChunk>::NextResult Next() override {
      if (done_) {
        return std::optional<ContextChunk>{};
      }
      done_ = true;
      GS_AUTO(chunk, producer_());
      return std::optional<ContextChunk>(std::move(chunk));
    }

   private:
    Producer producer_;
    bool done_ = false;
  };
  return Stream<ContextChunk>(
      std::make_shared<GenerateState>(std::move(producer)),
      std::move(metadata));
}

// Explicit global-input boundary. Row-local operators never collect input.
inline result<ContextChunk> collect_chunk(Stream<ContextChunk> input) {
  std::optional<ContextChunk> accumulated;
  while (true) {
    GS_AUTO(next, input.Next());
    if (!next) {
      return accumulated ? std::move(*accumulated) : ContextChunk{};
    }
    ContextChunk chunk = std::move(*next);
    if (accumulated) {
      *accumulated = accumulated->union_with(chunk);
    } else {
      accumulated = std::move(chunk);
    }
  }
}

template <typename Reduce>
Stream<ContextChunk> reduce_stream(Stream<ContextChunk> input, Reduce reduce) {
  auto metadata = input.metadata();
  auto upstream = std::make_shared<Stream<ContextChunk>>(std::move(input));
  return generate_chunk(
      [upstream, reduce = std::move(reduce)]() mutable -> result<ContextChunk> {
        GS_AUTO(chunk, collect_chunk(std::move(*upstream)));
        return reduce(std::move(chunk));
      },
      std::move(metadata));
}

// Buffer only when an operator must replay its input or stabilize it before
// mutations. Batches retain their boundaries and share column ownership.
inline result<std::vector<ContextChunk>> collect_batches(
    Stream<ContextChunk> input) {
  std::vector<ContextChunk> chunks;
  while (true) {
    GS_AUTO(next, input.Next());
    if (!next) {
      return chunks;
    }
    chunks.push_back(std::move(*next));
  }
}

inline Stream<ContextChunk> stream_from_batches(
    std::vector<ContextChunk> chunks, StreamMetadata metadata = {}) {
  class BatchState final : public OperatorState {
   public:
    explicit BatchState(std::vector<ContextChunk> chunks)
        : chunks_(std::move(chunks)) {}
    Stream<ContextChunk>::NextResult Next() override {
      if (index_ == chunks_.size()) {
        return std::optional<ContextChunk>{};
      }
      return std::optional<ContextChunk>(std::move(chunks_[index_++]));
    }

   private:
    std::vector<ContextChunk> chunks_;
    size_t index_ = 0;
  };
  return Stream<ContextChunk>(std::make_shared<BatchState>(std::move(chunks)),
                              std::move(metadata));
}

inline Stream<ContextChunk> stream_from_context(Context ctx) {
  return stream_from_batches(std::move(ctx.chunks()),
                             StreamMetadata{std::move(ctx.tag_ids)});
}

inline result<Context> materialize(Stream<ContextChunk> stream) {
  Context ctx;
  ctx.tag_ids = stream.metadata().output_columns;
  while (true) {
    auto next = stream.Next();
    if (!next) {
      return tl::unexpected(next.error());
    }
    if (!*next) {
      return ctx;
    }
    auto& batch = **next;
    ctx.append_chunk(std::move(batch));
  }
}

}  // namespace neug::execution
