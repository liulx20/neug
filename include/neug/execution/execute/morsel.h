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
#include "neug/execution/common/context_chunk.h"

namespace neug::execution {

// A work range, not a result batch. One morsel may yield several chunks.
struct Morsel {
  size_t partition = 0;
  size_t begin = 0;
  size_t end = 0;
  std::shared_ptr<const ContextChunk> chunk;
};

class MorselReader {
 public:
  virtual ~MorselReader() = default;
  virtual void Start(const Morsel& morsel) = 0;
  virtual result<std::optional<ContextChunk>> Next() = 0;
};

// Execution-owned shared source. The step serializes work allocation, while
// each worker lane owns a reader and processes its claimed ranges
// independently.
class MorselSource {
 public:
  virtual ~MorselSource() = default;
  virtual result<std::optional<Morsel>> Pick() = 0;
  virtual std::unique_ptr<MorselReader> CreateReader() = 0;
  virtual Status Finalize() { return Status::OK(); }
};

// Adapts a sequential supplier to range work. Decoding remains serialized;
// the returned ranges and their downstream transforms execute independently.
class ChunkMorselSource final : public MorselSource {
 public:
  using ReadBatch = std::function<result<std::optional<ContextChunk>>()>;
  explicit ChunkMorselSource(ReadBatch input) : input_(std::move(input)) {}

  result<std::optional<Morsel>> Pick() override {
    if (!chunk_) {
      GS_AUTO(next, input_());
      if (!next) {
        return std::optional<Morsel>{};
      }
      chunk_ = std::make_shared<ContextChunk>(std::move(*next));
      offset_ = 0;
    }
    auto end = std::min(offset_ + size_t{4096}, chunk_->row_num());
    Morsel work{0, offset_, end, chunk_};
    offset_ = end;
    if (end == chunk_->row_num()) {
      chunk_.reset();
    }
    return std::optional<Morsel>(std::move(work));
  }

  std::unique_ptr<MorselReader> CreateReader() override {
    class Reader final : public MorselReader {
     public:
      void Start(const Morsel& work) override {
        work_ = work;
        done_ = false;
      }
      result<std::optional<ContextChunk>> Next() override {
        if (done_) {
          return std::optional<ContextChunk>{};
        }
        auto end = std::min(work_.begin + size_t{1024}, work_.end);
        sel_vec_t rows;
        for (auto row = work_.begin; row < end; ++row) {
          rows.push_back(row);
        }
        auto output = *work_.chunk;
        output.reshuffle(rows);
        work_.begin = end;
        done_ = end == work_.end;
        return std::optional<ContextChunk>(std::move(output));
      }

     private:
      Morsel work_;
      bool done_ = true;
    };
    return std::make_unique<Reader>();
  }

 private:
  ReadBatch input_;
  std::shared_ptr<const ContextChunk> chunk_;
  size_t offset_ = 0;
};
}  // namespace neug::execution
