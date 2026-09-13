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

#include "neug/execution/common/operators/retrieve/join.h"

#include <atomic>
#include "neug/common/columns/vertex_columns.h"
#include "neug/common/types.h"
#include "neug/common/types/data_chunk.h"
#include "neug/execution/common/batch_accumulator.h"
#include "neug/execution/common/context_chunk.h"
#include "neug/execution/utils/params.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/encoder.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

using vertex_pair = std::pair<VertexRecord, VertexRecord>;

struct JoinTable::Batch {
  std::shared_ptr<const ContextChunk> chunk;
  size_t vertex_keys = 0;
  struct Bucket {
    sel_vec_t rows;
    std::vector<std::pair<std::string, sel_t>> keys;
  };
  std::vector<Bucket> buckets;
};

struct JoinTable::Impl {
  struct RowRef {
    size_t chunk;
    sel_t row;
  };
  using Matches = std::vector<RowRef>;
  JoinParams params;
  size_t vertex_keys = 0;
  struct Tables {
    flat_hash_map<VertexRecord, Matches> single;
    flat_hash_map<vertex_pair, Matches> dual;
    flat_hash_map<std::string, Matches> generic;
    std::vector<std::shared_ptr<const ContextChunk>> chunks;
  };
  std::vector<std::unique_ptr<Tables>> partitions;
  mutable std::atomic<size_t> prepared{0};
  bool finalized = false;

  template <typename Map, typename Key>
  size_t PartitionId(const Key& key) const {
    return typename Map::hasher{}(key) % partitions.size();
  }

  std::optional<std::string> Key(const ContextChunk& chunk, size_t row,
                                 const std::vector<int>& columns,
                                 bool skip_null) const {
    vector_t<char> bytes;
    Encoder encoder(bytes);
    for (auto alias : columns) {
      auto value = chunk.get(alias)->get_elem(row);
      if (skip_null && value.IsNull()) {
        return std::nullopt;
      }
      encode_value(value, encoder);
      encoder.put_byte('#');
    }
    return std::string(bytes.begin(), bytes.end());
  }

  VertexRecord Vertex(const ContextChunk& chunk, int alias, size_t row) const {
    return static_cast<const IVertexColumn&>(*chunk.get(alias)).get_vertex(row);
  }

  Impl(const JoinParams& config, size_t count) : params(config) {
    if (count == 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Join requires a build partition");
    }
    if (params.left_columns.size() != params.right_columns.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Join columns size mismatch");
    }
    for (size_t i = 0; i < count; ++i) {
      partitions.push_back(std::make_unique<Tables>());
    }
  }

  std::shared_ptr<Batch> Partition(ContextChunk input) const {
    auto batch = std::make_shared<Batch>();
    batch->chunk = std::make_shared<const ContextChunk>(std::move(input));
    batch->buckets.resize(partitions.size());
    const auto& right = *batch->chunk;
    auto count = params.right_columns.size();
    if (count == 1 || count == 2) {
      batch->vertex_keys = count;
      for (auto alias : params.right_columns) {
        if (!right.exist(alias) ||
            right.get(alias)->column_type() != ContextColumnType::kVertex) {
          batch->vertex_keys = 0;
        }
      }
    }
    if ((params.join_type == JoinKind::kSemiJoin ||
         params.join_type == JoinKind::kAntiJoin) &&
        batch->vertex_keys == 1) {
      batch->vertex_keys = 0;
    }
    if (params.join_type != JoinKind::kTimesJoin) {
      for (size_t row = 0; row < right.row_num(); ++row) {
        size_t partition;
        if (batch->vertex_keys == 1) {
          partition = PartitionId<decltype(Tables::single)>(
              Vertex(right, params.right_columns[0], row));
        } else if (batch->vertex_keys == 2) {
          partition = PartitionId<decltype(Tables::dual)>(
              vertex_pair{Vertex(right, params.right_columns[0], row),
                          Vertex(right, params.right_columns[1], row)});
        } else {
          auto key = Key(right, row, params.right_columns,
                         params.join_type == JoinKind::kInnerJoin);
          if (!key) {
            continue;
          }
          partition = PartitionId<decltype(Tables::generic)>(*key);
          batch->buckets[partition].keys.emplace_back(std::move(*key), row);
          continue;
        }
        batch->buckets[partition].rows.push_back(row);
      }
    }
    ++prepared;
    return batch;
  }

  void BuildPartition(size_t partition, const Batch& batch) {
    auto& target = *partitions.at(partition);
    auto chunk_id = target.chunks.size();
    target.chunks.push_back(batch.chunk);
    const auto& right = *batch.chunk;
    const auto& bucket = batch.buckets.at(partition);
    for (const auto& entry : bucket.keys) {
      target.generic[entry.first].push_back({chunk_id, entry.second});
    }
    for (auto row : bucket.rows) {
      if (batch.vertex_keys == 1) {
        target.single[Vertex(right, params.right_columns[0], row)].push_back(
            {chunk_id, row});
      } else {
        target
            .dual[{Vertex(right, params.right_columns[0], row),
                   Vertex(right, params.right_columns[1], row)}]
            .push_back({chunk_id, row});
      }
    }
  }

  // Gather only matched rows. Group by source chunk, then restore probe order.
  // This never concatenates the retained build input.
  ContextChunk Gather(const Matches& refs, bool outer) const {
    const auto& chunks = partitions.front()->chunks;
    if (chunks.empty()) {
      return {};
    }
    std::vector<sel_vec_t> rows(chunks.size()), positions(chunks.size());
    for (size_t i = 0; i < refs.size(); ++i) {
      rows[refs[i].chunk].push_back(refs[i].row);
      positions[refs[i].chunk].push_back(i);
    }
    sel_vec_t order(refs.size());
    size_t offset = 0;
    ChunkAccumulator result;
    for (size_t i = 0; i < chunks.size(); ++i) {
      if (rows[i].empty() && (!refs.empty() || i != 0)) {
        continue;
      }
      auto selected = *chunks[i];
      if (outer) {
        for (auto alias : params.right_columns) {
          selected.remove(alias);
        }
        selected.optional_reshuffle(rows[i]);
      } else {
        selected.reshuffle(rows[i]);
      }
      for (auto position : positions[i]) {
        order[position] = offset++;
      }
      result.Add(std::move(selected));
    }
    auto output = result.Finish();
    // Chunk grouping often already preserves probe order. Avoid copying every
    // selected column again when the restoring permutation is the identity.
    bool ordered = true;
    for (size_t i = 0; i < order.size(); ++i) {
      if (order[i] != i) {
        ordered = false;
        break;
      }
    }
    if (!ordered) {
      output->reshuffle(order);
    }
    return std::move(*output);
  }

  ContextChunk Probe(ContextChunk left) const {
    sel_vec_t left_rows;
    Matches right_rows;
    bool semi = params.join_type == JoinKind::kSemiJoin;
    bool anti = params.join_type == JoinKind::kAntiJoin;
    bool outer = params.join_type == JoinKind::kLeftOuterJoin;
    bool times = params.join_type == JoinKind::kTimesJoin;
    if (!semi && !anti && !outer && !times &&
        params.join_type != JoinKind::kInnerJoin) {
      THROW_NOT_SUPPORTED_EXCEPTION("Unsupported join type");
    }
    for (size_t row = 0; row < left.row_num(); ++row) {
      if (times) {
        const auto& chunks = partitions.front()->chunks;
        for (size_t i = 0; i < chunks.size(); ++i) {
          for (size_t index = 0; index < chunks[i]->row_num(); ++index) {
            left_rows.push_back(row);
            right_rows.push_back({i, static_cast<sel_t>(index)});
          }
        }
        continue;
      }
      const Matches* matches = nullptr;
      if (vertex_keys == 1) {
        auto key = Vertex(left, params.left_columns[0], row);
        const auto& table =
            *partitions[PartitionId<decltype(Tables::single)>(key)];
        auto found = table.single.find(key);
        if (found != table.single.end()) {
          matches = &found->second;
        }
      } else if (vertex_keys == 2) {
        vertex_pair key{Vertex(left, params.left_columns[0], row),
                        Vertex(left, params.left_columns[1], row)};
        const auto& table =
            *partitions[PartitionId<decltype(Tables::dual)>(key)];
        auto found = table.dual.find(key);
        if (found != table.dual.end()) {
          matches = &found->second;
        }
      } else {
        auto key = Key(left, row, params.left_columns, !outer);
        if (!key) {
          continue;
        }
        const auto& table =
            *partitions[PartitionId<decltype(Tables::generic)>(*key)];
        auto found = table.generic.find(*key);
        if (found != table.generic.end()) {
          matches = &found->second;
        }
      }
      if (semi || anti) {
        if (semi == (matches != nullptr)) {
          left_rows.push_back(row);
        }
      } else if (matches) {
        for (auto index : *matches) {
          left_rows.push_back(row);
          right_rows.push_back(index);
        }
      } else if (outer) {
        left_rows.push_back(row);
        right_rows.push_back({0, std::numeric_limits<sel_t>::max()});
      }
    }
    left.reshuffle(left_rows);
    if (!semi && !anti) {
      auto selected = Gather(right_rows, outer);
      for (size_t alias = 0; alias < selected.col_num(); ++alias) {
        auto column = selected.get(alias);
        if (column && (times || (outer && vertex_keys == 0) ||
                       alias >= left.col_num() || !left.get(alias))) {
          left.set(alias, column);
        }
      }
    }
    left.head().reset();
    return left;
  }
};

JoinTable::JoinTable(ContextChunk right, const JoinParams& params)
    : JoinTable(params, 1) {
  auto batch = Partition(std::move(right));
  BuildPartition(0, *batch);
  Finalize();
}
JoinTable::JoinTable(const JoinParams& params, size_t partitions)
    : impl_(std::make_unique<Impl>(params, partitions)) {}
JoinTable::~JoinTable() = default;
std::shared_ptr<JoinTable::Batch> JoinTable::Partition(
    ContextChunk input) const {
  return impl_->Partition(std::move(input));
}
Status JoinTable::BuildPartition(size_t partition, const Batch& batch) {
  impl_->BuildPartition(partition, batch);
  return Status::OK();
}
Status JoinTable::Finalize() {
  if (impl_->finalized) {
    return Status::OK();
  }
  for (const auto& partition : impl_->partitions) {
    if (partition->chunks.size() != impl_->prepared.load()) {
      return Status::InternalError("Join build partition is unfinished");
    }
  }
  const auto& chunks = impl_->partitions.front()->chunks;
  if (!chunks.empty()) {
    auto count = impl_->params.right_columns.size();
    if (count == 1 || count == 2) {
      impl_->vertex_keys = count;
      for (auto alias : impl_->params.right_columns) {
        if (!chunks.front()->exist(alias) ||
            chunks.front()->get(alias)->column_type() !=
                ContextColumnType::kVertex) {
          impl_->vertex_keys = 0;
        }
      }
    }
    if ((impl_->params.join_type == JoinKind::kSemiJoin ||
         impl_->params.join_type == JoinKind::kAntiJoin) &&
        impl_->vertex_keys == 1) {
      impl_->vertex_keys = 0;
    }
  }
  impl_->finalized = true;
  return Status::OK();
}
result<ContextChunk> JoinTable::Probe(ContextChunk left) const {
  if (!impl_->finalized) {
    return tl::unexpected(Status::InternalError("Join table is not ready"));
  }
  return impl_->Probe(std::move(left));
}

neug::result<ContextChunk> Join::join(ContextChunk&& left, ContextChunk&& right,
                                      const JoinParams& params) {
  if (params.left_columns.size() != params.right_columns.size()) {
    RETURN_INVALID_ARGUMENT_ERROR("Join columns size mismatch");
  }
  JoinTable table(std::move(right), params);
  return table.Probe(std::move(left));
}

neug::result<ContextChunk> Join::pk_join(IStorageInterface& graph,
                                         ContextChunk&& chunk,
                                         const std::vector<label_t>& labels,
                                         int tag, int alias) {
  size_t row_num = chunk.row_num();
  auto column = chunk.get(tag);
  MSVertexColumnBuilder builder(labels[0]);
  sel_vec_t offsets;
  for (label_t label : labels) {
    builder.start_label(label);
    for (size_t i = 0; i < row_num; ++i) {
      const auto& any = column->get_elem(i);
      vid_t index;
      if (graph.GetVertexIndex(label, any, index)) {
        builder.push_back_opt(index);
        offsets.push_back(i);
      }
    }
  }
  chunk.remove(alias);
  chunk.reshuffle(offsets);
  chunk.set(alias, builder.finish());
  chunk.head().reset();
  return chunk;
}

}  // namespace execution
}  // namespace neug
