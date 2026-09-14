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

#include "neug/execution/execute/ops/retrieve/dedup.h"

#include <stddef.h>
#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include "neug/utils/encoder.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/dedup.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
using IntegerKey = std::optional<uint64_t>;

static bool IsInteger(DataTypeId type) {
  return type == DataTypeId::kInt32 || type == DataTypeId::kInt64 ||
         type == DataTypeId::kUInt32 || type == DataTypeId::kUInt64;
}

static IntegerKey ReadInteger(const Value& value) {
  if (value.IsNull()) {
    return std::nullopt;
  }
  switch (value.type().id()) {
  case DataTypeId::kInt32:
    return static_cast<uint64_t>(value.GetValue<int32_t>());
  case DataTypeId::kInt64:
    return static_cast<uint64_t>(value.GetValue<int64_t>());
  case DataTypeId::kUInt32:
    return value.GetValue<uint32_t>();
  case DataTypeId::kUInt64:
    return value.GetValue<uint64_t>();
  default:
    throw std::logic_error("Expected integer key");
  }
}

class DedupState final : public PartitionState {
 public:
  DedupState(std::vector<int32_t> keys, size_t partitions)
      : keys_(std::move(keys)), partitions_(partitions) {
    if (partitions == 0) {
      throw std::invalid_argument("Dedup requires a partition");
    }
  }
  struct Input final : Batch {
    ContextChunk chunk;
    std::vector<std::vector<std::pair<std::string, sel_t>>> buckets;
    std::vector<std::vector<std::pair<IntegerKey, sel_t>>> integer_buckets;
    bool integer = false;
    bool retain_all = false;
  };
  std::shared_ptr<Batch> PartitionBuild(ContextChunk chunk) const override {
    auto input = std::make_shared<Input>();
    input->buckets.resize(partitions_.size());
    input->retain_all = keys_.empty();
    if (keys_.size() == 1) {
      auto column = chunk.get(keys_[0]);
      auto type = column->elem_type().id();
      input->integer = IsInteger(type);
      // Typed single-column equality can differ from encoded equality, e.g.
      // floating signed zero or edge property identity. Preserve all candidates
      // for the established final helper in these cases.
      switch (type) {
      case DataTypeId::kInt32:
      case DataTypeId::kInt64:
      case DataTypeId::kUInt32:
      case DataTypeId::kUInt64:
      case DataTypeId::kBoolean:
      case DataTypeId::kVarchar:
      case DataTypeId::kDate:
      case DataTypeId::kTimestampMs: {
        // Hashing nearly unique scalar keys costs more than the native final
        // sort. Sampling only chooses whether to reduce this batch early;
        // the final helper still guarantees global uniqueness in either case.
        auto rows = std::min<size_t>(64, chunk.row_num());
        if (input->integer) {
          flat_hash_set<IntegerKey> sample;
          for (size_t row = 0; row < rows; ++row) {
            sample.insert(ReadInteger(column->get_elem(row)));
          }
          input->retain_all = sample.size() > rows / 2;
        } else {
          flat_hash_set<std::string> sample;
          vector_t<char> bytes;
          for (size_t row = 0; row < rows; ++row) {
            bytes.clear();
            Encoder encoder(bytes);
            encode_value(column->get_elem(row), encoder);
            sample.emplace(bytes.begin(), bytes.end());
          }
          input->retain_all = sample.size() > rows / 2;
        }
        break;
      }
      case DataTypeId::kVertex:
      case DataTypeId::kList:
      case DataTypeId::kArray:
      case DataTypeId::kStruct:
        break;
      default:
        input->retain_all = true;
      }
    }
    if (keys_.empty()) {
      input->chunk = chunk;
    } else {
      for (auto key : keys_) {
        input->chunk.set(key, chunk.get(key));
      }
    }
    if (input->retain_all) {
      return input;
    }
    if (input->integer) {
      input->integer_buckets.resize(partitions_.size());
      flat_hash_set<IntegerKey> local;
      sel_vec_t selected;
      auto column = chunk.get(keys_[0]);
      for (size_t row = 0; row < chunk.row_num(); ++row) {
        auto key = ReadInteger(column->get_elem(row));
        if (local.insert(key).second) {
          auto part = std::hash<IntegerKey>{}(key) % partitions_.size();
          input->integer_buckets[part].emplace_back(key, selected.size());
          selected.push_back(row);
        }
      }
      input->chunk.reshuffle(selected);
      return input;
    }
    flat_hash_set<std::string> local;
    sel_vec_t selected;
    vector_t<char> bytes;
    for (size_t row = 0; row < chunk.row_num(); ++row) {
      bytes.assign((keys_.size() + 7) / 8, 0);
      Encoder encoder(bytes);
      for (size_t key = 0; key < keys_.size(); ++key) {
        auto value = chunk.get(keys_[key])->get_elem(row);
        if (value.IsNull()) {
          bytes[key >> 3] |= static_cast<char>(1U << (key & 7));
        }
        encode_value(value, encoder);
        encoder.put_byte('#');
      }
      std::string encoded(bytes.begin(), bytes.end());
      if (local.insert(encoded).second) {
        auto partition = std::hash<std::string>{}(encoded) % partitions_.size();
        input->buckets[partition].emplace_back(std::move(encoded),
                                               selected.size());
        selected.push_back(row);
      }
    }
    input->chunk.reshuffle(selected);
    return input;
  }
  size_t BuildPartitions() const override { return partitions_.size(); }
  Status BuildPartition(size_t part, const Batch& batch) override {
    const auto& input = static_cast<const Input&>(batch);
    auto& partition = partitions_.at(part);
    Selected output;
    if (input.retain_all) {
      output.passthrough = true;
      if (part == 0) {
        output.chunk = input.chunk;
      }
      partition.output.push_back(std::move(output));
      return Status::OK();
    }
    if (input.integer) {
      for (const auto& entry : input.integer_buckets[part]) {
        if (partition.integer_seen.insert(entry.first).second) {
          output.rows.push_back(entry.second);
        }
      }
    } else {
      for (const auto& entry : input.buckets[part]) {
        if (partition.seen.insert(entry.first).second) {
          output.rows.push_back(entry.second);
        }
      }
    }
    // Release duplicate payloads as soon as this batch finishes appending.
    output.chunk = input.chunk;
    output.chunk.reshuffle(output.rows);
    partition.output.push_back(std::move(output));
    return Status::OK();
  }
  Status FinalizeBuild() override {
    ChunkAccumulator candidates;
    auto batches = partitions_.front().output.size();
    for (const auto& partition : partitions_) {
      if (partition.output.size() != batches) {
        return Status::InternalError("Dedup partition is unfinished");
      }
    }
    for (size_t batch = 0; batch < batches; ++batch) {
      if (partitions_.size() == 1 || partitions_[0].output[batch].passthrough) {
        candidates.Add(std::move(partitions_[0].output[batch].chunk));
        continue;
      }
      ChunkAccumulator merged;
      std::vector<std::pair<sel_t, sel_t>> positions;
      size_t offset = 0;
      for (auto& partition : partitions_) {
        auto& output = partition.output[batch];
        for (auto row : output.rows) {
          positions.emplace_back(row, offset++);
        }
        merged.Add(std::move(output.chunk));
        sel_vec_t{}.swap(output.rows);
      }
      std::sort(positions.begin(), positions.end());
      sel_vec_t order;
      order.reserve(positions.size());
      for (const auto& position : positions) {
        order.push_back(position.second);
      }
      auto chunk = merged.Finish();
      chunk->reshuffle(order);
      candidates.Add(std::move(*chunk));
    }
    for (auto& partition : partitions_) {
      partition.output.clear();
      partition.seen.clear();
      partition.integer_seen.clear();
    }
    auto chunk = candidates.Finish();
    if (keys_.size() > 1 && chunk) {
      // Composite keys always use encoded equality and never bypass reduction.
      // The partition sets already guarantee uniqueness; input order is
      // restored.
      chunk->head().reset();
      output_ = one_chunk(std::move(*chunk));
      return Status::OK();
    }
    // The single-column helpers may sort by value. Running the established
    // normalization on reduced candidates retains that order and null behavior.
    auto result =
        Dedup::dedup(chunk ? std::move(*chunk) : ContextChunk{}, keys_);
    if (!result) {
      return result.error();
    }
    output_ = one_chunk(std::move(*result));
    return Status::OK();
  }
  ChunkBatch TakeOutput() override { return std::exchange(output_, {}); }

 private:
  struct Selected {
    bool passthrough = false;
    ContextChunk chunk;
    sel_vec_t rows;
  };
  struct Partition {
    flat_hash_set<std::string> seen;
    flat_hash_set<IntegerKey> integer_seen;
    std::vector<Selected> output;
  };
  std::vector<int32_t> keys_;
  std::vector<Partition> partitions_;
  ChunkBatch output_;
};

class DedupOpr : public PartitionedOperator {
 public:
  explicit DedupOpr(const std::vector<int32_t>& tag_ids) : tag_ids_(tag_ids) {}
  std::string get_operator_name() const override { return "DedupOpr"; }
  std::shared_ptr<PartitionState> CreatePartitionState(
      size_t workers) override {
    return std::make_shared<DedupState>(tag_ids_, workers);
  }
  std::optional<std::vector<int>> output_columns() const override {
    if (tag_ids_.empty()) {
      return std::nullopt;
    }
    return std::vector<int>(tag_ids_.begin(), tag_ids_.end());
  }

 private:
  std::vector<int32_t> tag_ids_;
};

neug::result<OpBuildResultT> DedupOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& dedup_opr = plan.plan(op_idx).opr().dedup();
  int keys_num = dedup_opr.keys_size();
  std::vector<int32_t> keys;
  ContextMeta ret_meta;
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const auto& key = dedup_opr.keys(k_i);
    int tag = key.has_tag() ? key.tag().id() : -1;
    keys.emplace_back(tag);
    ret_meta.set(tag, ctx_meta.get(tag));
  }

  return std::make_pair(std::make_unique<DedupOpr>(keys), ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug