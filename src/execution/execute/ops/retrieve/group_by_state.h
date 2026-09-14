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

#include <bit>
#include <queue>
#include <tuple>
#include "neug/common/columns/columns_utils.h"
#include "neug/common/columns/value_columns.h"
#include "neug/execution/execute/operator.h"
#include "neug/utils/encoder.h"

namespace neug::execution::ops {

struct AggregateSpec {
  AggrKind kind;
  int input;
  int output;
  DataType type;
};

// A column of partial aggregate states, indexed by local group id.
class AggregateColumn {
 public:
  virtual ~AggregateColumn() = default;
  virtual void Resize(size_t size) = 0;
  virtual void Consume(const IContextColumn* input,
                       const sel_vec_t& groups) = 0;
  virtual void Merge(const AggregateColumn& input, size_t source,
                     size_t dest) = 0;
  virtual std::shared_ptr<IContextColumn> Finish() const = 0;
};

template <typename T>
class TypedAggregateColumn final : public AggregateColumn {
 public:
  explicit TypedAggregateColumn(AggrKind kind) : kind_(kind) {}
  void Resize(size_t size) override {
    if (kind_ == AggrKind::kAvg) {
      sums_.resize(size);
    } else if (kind_ != AggrKind::kCount) {
      values_.resize(size);
    }
    if (kind_ != AggrKind::kSum) {
      counts_.resize(size);
    }
  }
  void Consume(const IContextColumn* input, const sel_vec_t& groups) override {
    for (size_t row = 0; row < groups.size(); ++row) {
      if (input && !input->has_value(row)) {
        continue;
      }
      auto dest = groups[row];
      if (kind_ == AggrKind::kCount) {
        ++counts_[dest];
        continue;
      }
      auto value = input->get_elem(row).template GetValue<T>();
      if (kind_ == AggrKind::kSum) {
        values_[dest] = Add(values_[dest], value);
        continue;
      } else if (kind_ == AggrKind::kAvg) {
        if constexpr (std::is_arithmetic_v<T>) {
          sums_[dest] += static_cast<double>(value);
        }
      } else if (!counts_[dest] ||
                 (kind_ == AggrKind::kMin ? value < values_[dest]
                                          : values_[dest] < value)) {
        values_[dest] = value;
      }
      ++counts_[dest];
    }
  }
  void Merge(const AggregateColumn& input, size_t source,
             size_t dest) override {
    const auto& other = static_cast<const TypedAggregateColumn&>(input);
    if (kind_ == AggrKind::kSum) {
      values_[dest] = Add(values_[dest], other.values_[source]);
      return;
    }
    if (!other.counts_[source]) {
      return;
    }
    if (kind_ == AggrKind::kAvg) {
      sums_[dest] += other.sums_[source];
    } else if (kind_ != AggrKind::kCount &&
               (!counts_[dest] ||
                (kind_ == AggrKind::kMin
                     ? other.values_[source] < values_[dest]
                     : values_[dest] < other.values_[source]))) {
      values_[dest] = other.values_[source];
    }
    counts_[dest] += other.counts_[source];
  }
  std::shared_ptr<IContextColumn> Finish() const override {
    if (kind_ == AggrKind::kSum) {
      ValueColumnBuilder<T> output;
      output.reserve(values_.size());
      for (const auto& value : values_) {
        output.push_back_opt(value);
      }
      return output.finish();
    }
    if (kind_ == AggrKind::kCount) {
      ValueColumnBuilder<int64_t> output;
      output.reserve(counts_.size());
      for (auto count : counts_) {
        output.push_back_opt(count);
      }
      return output.finish();
    }
    if (kind_ == AggrKind::kAvg) {
      ValueColumnBuilder<double> output;
      output.reserve(counts_.size());
      for (size_t i = 0; i < counts_.size(); ++i) {
        if (counts_[i]) {
          output.push_back_opt(sums_[i] / counts_[i]);
        } else {
          output.push_back_null();
        }
      }
      return output.finish();
    }
    ValueColumnBuilder<T> output;
    output.reserve(counts_.size());
    for (size_t i = 0; i < counts_.size(); ++i) {
      if (counts_[i]) {
        output.push_back_opt(values_[i]);
      } else {
        output.push_back_null();
      }
    }
    return output.finish();
  }

 private:
  static T Add(T left, T right) {
    if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
      // Preserve fixed-width sums without introducing signed-overflow UB when
      // local partials are merged in a different association.
      using U = std::make_unsigned_t<T>;
      return std::bit_cast<T>(
          static_cast<U>(static_cast<U>(left) + static_cast<U>(right)));
    } else if constexpr (std::is_floating_point_v<T>) {
      return left + right;
    } else {
      throw std::logic_error("Unsupported partial SUM type");
    }
  }
  AggrKind kind_;
  std::vector<T> values_;
  std::vector<double> sums_;
  std::vector<uint64_t> counts_;
};

inline std::unique_ptr<AggregateColumn> MakeAggregate(
    const AggregateSpec& spec) {
  if (spec.kind == AggrKind::kCount) {
    return std::make_unique<TypedAggregateColumn<int64_t>>(spec.kind);
  }
  switch (spec.type.id()) {
#define AGGREGATE_TYPE(id, type) \
  case DataTypeId::id:           \
    return std::make_unique<TypedAggregateColumn<type>>(spec.kind);
    FOR_EACH_DATA_TYPE(AGGREGATE_TYPE)
#undef AGGREGATE_TYPE
  default:
    throw std::logic_error("Unsupported partial aggregate type");
  }
}

class GroupByState final : public PartitionState {
 public:
  GroupByState(std::vector<std::pair<int, int>> mappings,
               std::vector<DataType> key_types,
               std::vector<AggregateSpec> specs, size_t workers)
      : mappings_(std::move(mappings)),
        key_types_(std::move(key_types)),
        specs_(std::move(specs)),
        partitions_(workers) {
    if (!workers) {
      throw std::invalid_argument("GroupBy requires a partition");
    }
    for (auto& partition : partitions_) {
      for (const auto& type : key_types_) {
        partition.keys.push_back(ColumnsUtils::create_builder(type));
      }
      for (const auto& spec : specs_) {
        partition.aggregates.push_back(MakeAggregate(spec));
      }
    }
  }
  struct Input final : Batch {
    ContextChunk keys;
    sel_vec_t offsets;
    std::vector<std::string> signatures;
    std::vector<std::vector<size_t>> buckets;
    std::vector<std::unique_ptr<AggregateColumn>> aggregates;
  };
  std::shared_ptr<Batch> PartitionBuild(ContextChunk chunk) const override {
    auto input = std::make_shared<Input>();
    input->buckets.resize(partitions_.size());
    flat_hash_map<std::string, size_t> groups;
    sel_vec_t row_groups;
    row_groups.reserve(chunk.row_num());
    auto signature_at = [&](size_t row) {
      vector_t<char> bytes((mappings_.size() + 7) / 8, 0);
      Encoder encoder(bytes);
      for (size_t key = 0; key < mappings_.size(); ++key) {
        auto value = chunk.get(mappings_[key].first)->get_elem(row);
        if (value.IsNull()) {
          bytes[key >> 3] |= static_cast<char>(1U << (key & 7));
        }
        encode_value(value, encoder);
      }
      return std::string(bytes.begin(), bytes.end());
    };
    auto scalar =
        mappings_.size() == 1 ? chunk.get(mappings_[0].first) : nullptr;
    if (scalar && (scalar->elem_type().id() == DataTypeId::kInt64 ||
                   scalar->elem_type().id() == DataTypeId::kInt32)) {
      // Keep the existing integer-key specialization: encode only one key per
      // local group instead of allocating an encoded key for every input row.
      flat_hash_map<int64_t, size_t> typed_groups;
      std::optional<size_t> null_group;
      for (size_t row = 0; row < chunk.row_num(); ++row) {
        auto value = scalar->get_elem(row);
        size_t group;
        bool added;
        if (value.IsNull()) {
          added = !null_group;
          if (added) {
            null_group = input->signatures.size();
          }
          group = *null_group;
        } else {
          auto number = value.type().id() == DataTypeId::kInt64
                            ? value.GetValue<int64_t>()
                            : value.GetValue<int32_t>();
          auto entry = typed_groups.emplace(number, input->signatures.size());
          group = entry.first->second;
          added = entry.second;
        }
        row_groups.push_back(group);
        if (added) {
          input->offsets.push_back(row);
          input->signatures.push_back(signature_at(row));
        }
      }
    } else {
      for (size_t row = 0; row < chunk.row_num(); ++row) {
        auto signature = signature_at(row);
        auto [it, added] = groups.emplace(signature, groups.size());
        row_groups.push_back(it->second);
        if (added) {
          input->offsets.push_back(row);
          input->signatures.push_back(std::move(signature));
        }
      }
    }
    // Ungrouped empty input still owns one aggregate state (COUNT/SUM = 0).
    if (mappings_.empty() && input->signatures.empty()) {
      input->signatures.emplace_back();
      input->offsets.push_back(0);
    }
    for (auto [source, dest] : mappings_) {
      input->keys.set(dest, chunk.get(source)->shuffle(input->offsets));
    }
    for (size_t group = 0; group < input->signatures.size(); ++group) {
      auto part = std::hash<std::string>{}(input->signatures[group]) %
                  partitions_.size();
      input->buckets[part].push_back(group);
    }
    for (const auto& spec : specs_) {
      auto aggregate = MakeAggregate(spec);
      aggregate->Resize(input->signatures.size());
      aggregate->Consume(spec.input < 0 ? nullptr : chunk.get(spec.input).get(),
                         row_groups);
      input->aggregates.push_back(std::move(aggregate));
    }
    return input;
  }
  size_t BuildPartitions() const override { return partitions_.size(); }
  Status BuildPartition(size_t part, const Batch& batch) override {
    const auto& input = static_cast<const Input&>(batch);
    auto& partition = partitions_.at(part);
    auto sequence = partition.sequence++;
    for (auto group : input.buckets[part]) {
      size_t dest;
      bool added;
      if (key_types_.size() == 1 &&
          (key_types_[0].id() == DataTypeId::kInt64 ||
           key_types_[0].id() == DataTypeId::kInt32)) {
        auto value = input.keys.get(mappings_[0].second)->get_elem(group);
        if (value.IsNull()) {
          added = !partition.null_group;
          if (added) {
            partition.null_group = partition.order.size();
          }
          dest = *partition.null_group;
        } else {
          auto number = value.type().id() == DataTypeId::kInt64
                            ? value.GetValue<int64_t>()
                            : value.GetValue<int32_t>();
          auto entry =
              partition.typed_groups.emplace(number, partition.order.size());
          dest = entry.first->second;
          added = entry.second;
        }
      } else {
        auto entry = partition.groups.emplace(input.signatures[group],
                                              partition.order.size());
        dest = entry.first->second;
        added = entry.second;
      }
      if (added) {
        partition.order.emplace_back(sequence, input.offsets[group]);
        for (size_t key = 0; key < mappings_.size(); ++key) {
          partition.keys[key]->push_back_elem(
              input.keys.get(mappings_[key].second)->get_elem(group));
        }
        for (auto& aggregate : partition.aggregates) {
          aggregate->Resize(partition.order.size());
        }
      }
      for (size_t i = 0; i < specs_.size(); ++i) {
        partition.aggregates[i]->Merge(*input.aggregates[i], group, dest);
      }
    }
    return Status::OK();
  }
  Status FinalizeBuild() override {
    if (mappings_.empty() && partitions_[0].sequence == 0) {
      // An upstream producer may signal EOF without publishing a typed empty
      // batch. Ungrouped aggregation must still emit its empty-input row.
      partitions_[0].order.emplace_back(0, 0);
      for (auto& aggregate : partitions_[0].aggregates) {
        aggregate->Resize(1);
      }
    }
    ChunkAccumulator output;
    // Each partition already records first appearances in input order. Merge
    // these ordered runs instead of sorting all groups again.
    using Position = std::tuple<size_t, size_t, size_t, size_t>;
    std::priority_queue<Position, std::vector<Position>, std::greater<Position>>
        ready;
    std::vector<size_t> offsets;
    size_t offset = 0;
    for (auto& partition : partitions_) {
      ContextChunk chunk;
      for (size_t key = 0; key < mappings_.size(); ++key) {
        chunk.set(mappings_[key].second, partition.keys[key]->finish());
      }
      for (size_t i = 0; i < specs_.size(); ++i) {
        chunk.set(specs_[i].output, partition.aggregates[i]->Finish());
      }
      offsets.push_back(offset);
      offset += partition.order.size();
      if (!partition.order.empty()) {
        auto [sequence, row] = partition.order[0];
        ready.emplace(sequence, row, offsets.size() - 1, 0);
      }
      output.Add(std::move(chunk));
    }
    sel_vec_t rows;
    rows.reserve(offset);
    while (!ready.empty()) {
      auto [sequence, row, part, index] = ready.top();
      ready.pop();
      rows.push_back(offsets[part] + index);
      if (++index < partitions_[part].order.size()) {
        auto [next_sequence, next_row] = partitions_[part].order[index];
        ready.emplace(next_sequence, next_row, part, index);
      }
    }
    auto chunk = output.Finish();
    if (partitions_.size() > 1) {
      chunk->reshuffle(rows);
    }
    output_ = one_chunk(std::move(*chunk));
    partitions_.clear();
    return Status::OK();
  }
  ChunkBatch TakeOutput() override { return std::exchange(output_, {}); }

 private:
  struct Partition {
    size_t sequence = 0;
    flat_hash_map<std::string, size_t> groups;
    flat_hash_map<int64_t, size_t> typed_groups;
    std::optional<size_t> null_group;
    std::vector<std::pair<size_t, size_t>> order;
    std::vector<std::shared_ptr<IContextColumnBuilder>> keys;
    std::vector<std::unique_ptr<AggregateColumn>> aggregates;
  };
  std::vector<std::pair<int, int>> mappings_;
  std::vector<DataType> key_types_;
  std::vector<AggregateSpec> specs_;
  std::vector<Partition> partitions_;
  ChunkBatch output_;
};
}  // namespace neug::execution::ops
