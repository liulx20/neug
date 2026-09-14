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

#include <atomic>
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
  virtual void Consume(const IContextColumn* input, const sel_vec_t& groups,
                       const sel_vec_t* selection = nullptr) = 0;
  virtual void Merge(const AggregateColumn& input, size_t source,
                     size_t dest) = 0;
  virtual std::shared_ptr<IContextColumn> Finish() const = 0;
  virtual std::shared_ptr<IContextColumn> FinishMerged(
      const std::vector<const AggregateColumn*>& partitions,
      const std::vector<std::pair<size_t, size_t>>& rows) const = 0;
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
  void Consume(const IContextColumn* input, const sel_vec_t& groups,
               const sel_vec_t* selection = nullptr) override {
    for (size_t index = 0; index < groups.size(); ++index) {
      auto row = selection ? (*selection)[index] : index;
      if (input && !input->has_value(row)) {
        continue;
      }
      auto dest = groups[index];
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
    auto size = kind_ == AggrKind::kSum ? values_.size() : counts_.size();
    return FinishRows(size, [this](size_t row) {
      return std::pair{this, row};
    });
  }
  std::shared_ptr<IContextColumn> FinishMerged(
      const std::vector<const AggregateColumn*>& partitions,
      const std::vector<std::pair<size_t, size_t>>& rows) const override {
    std::vector<const TypedAggregateColumn*> inputs;
    for (auto* partition : partitions) {
      inputs.push_back(static_cast<const TypedAggregateColumn*>(partition));
    }
    return FinishRows(rows.size(), [&](size_t row) {
      auto [part, index] = rows[row];
      return std::pair{inputs[part], index};
    });
  }

 private:
  template <typename Select>
  std::shared_ptr<IContextColumn> FinishRows(size_t size, Select select) const {
    if (kind_ == AggrKind::kSum) {
      ValueColumnBuilder<T> output;
      output.reserve(size);
      for (size_t row = 0; row < size; ++row) {
        auto [input, index] = select(row);
        output.push_back_opt(input->values_[index]);
      }
      return output.finish();
    }
    if (kind_ == AggrKind::kCount) {
      ValueColumnBuilder<int64_t> output;
      output.reserve(size);
      for (size_t row = 0; row < size; ++row) {
        auto [input, index] = select(row);
        output.push_back_opt(input->counts_[index]);
      }
      return output.finish();
    }
    if (kind_ == AggrKind::kAvg) {
      ValueColumnBuilder<double> output;
      output.reserve(size);
      for (size_t row = 0; row < size; ++row) {
        auto [input, index] = select(row);
        if (input->counts_[index]) {
          output.push_back_opt(input->sums_[index] / input->counts_[index]);
        } else {
          output.push_back_null();
        }
      }
      return output.finish();
    }
    ValueColumnBuilder<T> output;
    output.reserve(size);
    for (size_t row = 0; row < size; ++row) {
      auto [input, index] = select(row);
      if (input->counts_[index]) {
        output.push_back_opt(input->values_[index]);
      } else {
        output.push_back_null();
      }
    }
    return output.finish();
  }

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
  enum class InputMode { kPartial, kRaw, kAdaptive };
  GroupByState(std::vector<std::pair<int, int>> mappings,
               std::vector<DataType> key_types,
               std::vector<AggregateSpec> specs, size_t workers,
               InputMode mode = InputMode::kAdaptive)
      : mappings_(std::move(mappings)),
        key_types_(std::move(key_types)),
        specs_(std::move(specs)),
        partitions_(workers),
        mode_(mode) {
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
    ContextChunk source;
    bool raw = false;
    sel_vec_t offsets;
    std::vector<std::string> signatures;
    std::vector<sel_vec_t> buckets;
    std::vector<std::unique_ptr<AggregateColumn>> aggregates;
  };
  std::shared_ptr<Batch> PartitionBuild(ContextChunk chunk) const override {
    auto input = std::make_shared<Input>();
    input->buckets.resize(partitions_.size());
    flat_hash_map<std::string, size_t> groups;
    sel_vec_t row_groups;
    row_groups.reserve(chunk.row_num());
    // Resolve native storage once per batch. Retain the generic encoder for
    // other representations and types, with exactly the same key bytes.
    struct KeyColumn {
      const IContextColumn* column;
      const ValueColumn<int64_t>* integers64;
      const ValueColumn<int32_t>* integers32;
      const ValueColumn<std::string>* strings;
    };
    std::vector<KeyColumn> columns;
    for (auto [source, dest] : mappings_) {
      auto* column = chunk.get(source).get();
      columns.push_back(
          {column, dynamic_cast<const ValueColumn<int64_t>*>(column),
           dynamic_cast<const ValueColumn<int32_t>*>(column),
           dynamic_cast<const ValueColumn<std::string>*>(column)});
    }
    vector_t<char> bytes;
    auto signature_at = [&](size_t row) {
      bytes.assign((mappings_.size() + 7) / 8, 0);
      Encoder encoder(bytes);
      for (size_t key = 0; key < columns.size(); ++key) {
        const auto& column = columns[key];
        if (!column.column->has_value(row)) {
          bytes[key >> 3] |= static_cast<char>(1U << (key & 7));
          encoder.put_int(-1);
        } else if (column.integers64) {
          encoder.put_long(column.integers64->data()[row]);
        } else if (column.integers32) {
          encoder.put_int(column.integers32->data()[row]);
        } else if (column.strings) {
          encoder.put_string_view(column.strings->data()[row]);
        } else {
          encode_value(column.column->get_elem(row), encoder);
        }
      }
      return std::string(bytes.begin(), bytes.end());
    };
    auto scalar =
        mappings_.size() == 1 ? chunk.get(mappings_[0].first) : nullptr;
    bool integer_key =
        scalar && (scalar->elem_type().id() == DataTypeId::kInt64 ||
                   scalar->elem_type().id() == DataTypeId::kInt32);
    auto partition_at = [&](size_t row, const std::string& signature) {
      if (integer_key) {
        auto value = scalar->get_elem(row);
        auto number = value.IsNull() ? int64_t{0}
                                     : (value.type().id() == DataTypeId::kInt64
                                            ? value.GetValue<int64_t>()
                                            : value.GetValue<int32_t>());
        return std::hash<int64_t>{}(number) % partitions_.size();
      }
      return std::hash<std::string>{}(signature) % partitions_.size();
    };
    bool raw = mode_ == InputMode::kRaw;
    if (mode_ == InputMode::kAdaptive && !mappings_.empty()) {
      // Observe complete batches, not a key prefix. Once local reduction is
      // unprofitable, periodically probe again so changed distributions
      // recover.
      auto batch = batches_.fetch_add(1, std::memory_order_relaxed);
      raw = prefer_raw_.load(std::memory_order_relaxed) && batch % 32 != 0;
    }
    if (raw && !mappings_.empty()) {
      input->raw = true;
      if (!integer_key) {
        input->signatures.reserve(chunk.row_num());
      }
      for (auto [source, dest] : mappings_) {
        input->keys.set(dest, chunk.get(source));
      }
      for (size_t row = 0; row < chunk.row_num(); ++row) {
        std::string signature;
        if (!integer_key) {
          signature = signature_at(row);
        }
        input->buckets[partition_at(row, signature)].push_back(row);
        if (!integer_key) {
          input->signatures.push_back(std::move(signature));
        }
      }
      input->source = std::move(chunk);
      return input;
    }
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
    if (mode_ == InputMode::kAdaptive && !mappings_.empty() &&
        chunk.row_num()) {
      // The mode comparison crosses over between 1/8 and 1/4 distinct groups
      // per 1024-row batch for COUNT/SUM. Keep this as a heuristic, not a cost
      // guarantee for every aggregate/type/distribution.
      prefer_raw_.store(input->signatures.size() >= (chunk.row_num() + 3) / 4,
                        std::memory_order_relaxed);
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
      auto part = partition_at(input->offsets[group], input->signatures[group]);
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
    const auto* scalar = mappings_.size() == 1
                             ? input.keys.get(mappings_[0].second).get()
                             : nullptr;
    // Reduced batches contain few rows; avoid RTTI dispatch for those batches.
    const auto* keys64 =
        input.raw ? dynamic_cast<const ValueColumn<int64_t>*>(scalar) : nullptr;
    const auto* keys32 = input.raw && !keys64
                             ? dynamic_cast<const ValueColumn<int32_t>*>(scalar)
                             : nullptr;
    auto* output64 = keys64 ? dynamic_cast<ValueColumnBuilder<int64_t>*>(
                                  partition.keys[0].get())
                            : nullptr;
    auto* output32 = keys32 ? dynamic_cast<ValueColumnBuilder<int32_t>*>(
                                  partition.keys[0].get())
                            : nullptr;
    sel_vec_t destinations;
    if (input.raw) {
      destinations.reserve(input.buckets[part].size());
    }
    for (auto group : input.buckets[part]) {
      size_t dest;
      bool added;
      if (key_types_.size() == 1 &&
          (key_types_[0].id() == DataTypeId::kInt64 ||
           key_types_[0].id() == DataTypeId::kInt32)) {
        // Native value columns expose typed storage. Avoid constructing a
        // generic Value for each hash lookup and each newly retained key.
        std::optional<int64_t> number;
        if (keys64) {
          if (keys64->has_value(group)) {
            number = keys64->get_value(group);
          }
        } else if (keys32) {
          if (keys32->has_value(group)) {
            number = keys32->get_value(group);
          }
        } else {
          auto value = scalar->get_elem(group);
          if (!value.IsNull()) {
            number = value.type().id() == DataTypeId::kInt64
                         ? value.GetValue<int64_t>()
                         : value.GetValue<int32_t>();
          }
        }
        if (!number) {
          added = !partition.null_group;
          if (added) {
            partition.null_group = partition.order.size();
          }
          dest = *partition.null_group;
        } else {
          auto entry =
              partition.typed_groups.emplace(*number, partition.order.size());
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
        partition.order.emplace_back(sequence,
                                     input.raw ? group : input.offsets[group]);
        if (output64) {
          if (keys64->has_value(group)) {
            output64->push_back_opt(keys64->get_value(group));
          } else {
            output64->push_back_null();
          }
        } else if (output32) {
          if (keys32->has_value(group)) {
            output32->push_back_opt(keys32->get_value(group));
          } else {
            output32->push_back_null();
          }
        } else {
          for (size_t key = 0; key < mappings_.size(); ++key) {
            partition.keys[key]->push_back_elem(
                input.keys.get(mappings_[key].second)->get_elem(group));
          }
        }
        if (!input.raw) {
          for (auto& aggregate : partition.aggregates) {
            aggregate->Resize(partition.order.size());
          }
        }
      }
      if (input.raw) {
        destinations.push_back(dest);
      } else {
        for (size_t i = 0; i < specs_.size(); ++i) {
          partition.aggregates[i]->Merge(*input.aggregates[i], group, dest);
        }
      }
    }
    if (input.raw) {
      for (size_t i = 0; i < specs_.size(); ++i) {
        auto& aggregate = partition.aggregates[i];
        aggregate->Resize(partition.order.size());
        aggregate->Consume(specs_[i].input < 0
                               ? nullptr
                               : input.source.get(specs_[i].input).get(),
                           destinations, &input.buckets[part]);
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
    ContextChunk output;
    if (partitions_.size() == 1) {
      // A single partition already has the final row order.
      auto& partition = partitions_[0];
      for (size_t key = 0; key < mappings_.size(); ++key) {
        output.set(mappings_[key].second, partition.keys[key]->finish());
      }
      for (size_t i = 0; i < specs_.size(); ++i) {
        output.set(specs_[i].output, partition.aggregates[i]->Finish());
      }
    } else {
      // Determine final first-appearance order once, then write each result
      // column directly. Do not concatenate and reshuffle intermediate columns.
      using Position = std::tuple<size_t, size_t, size_t, size_t>;
      std::priority_queue<Position, std::vector<Position>,
                          std::greater<Position>>
          ready;
      size_t size = 0;
      for (size_t part = 0; part < partitions_.size(); ++part) {
        const auto& order = partitions_[part].order;
        size += order.size();
        if (!order.empty()) {
          auto [sequence, row] = order[0];
          ready.emplace(sequence, row, part, 0);
        }
      }
      std::vector<std::pair<size_t, size_t>> rows;
      rows.reserve(size);
      while (!ready.empty()) {
        auto [sequence, row, part, index] = ready.top();
        ready.pop();
        rows.emplace_back(part, index);
        if (++index < partitions_[part].order.size()) {
          auto [next_sequence, next_row] = partitions_[part].order[index];
          ready.emplace(next_sequence, next_row, part, index);
        }
      }
      for (size_t key = 0; key < mappings_.size(); ++key) {
        std::vector<std::shared_ptr<IContextColumn>> inputs;
        for (auto& partition : partitions_) {
          inputs.push_back(partition.keys[key]->finish());
        }
        auto column = ColumnsUtils::create_builder(key_types_[key]);
        column->reserve(rows.size());
        for (auto [part, index] : rows) {
          column->push_back_elem(inputs[part]->get_elem(index));
        }
        output.set(mappings_[key].second, column->finish());
      }
      for (size_t i = 0; i < specs_.size(); ++i) {
        std::vector<const AggregateColumn*> inputs;
        for (auto& partition : partitions_) {
          inputs.push_back(partition.aggregates[i].get());
        }
        output.set(specs_[i].output, inputs[0]->FinishMerged(inputs, rows));
      }
    }
    output.head().reset();
    output_ = one_chunk(std::move(output));
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
  InputMode mode_;
  mutable std::atomic<size_t> batches_{0};
  mutable std::atomic<bool> prefer_raw_{false};
  ChunkBatch output_;
};
}  // namespace neug::execution::ops
