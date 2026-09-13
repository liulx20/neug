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

#include "neug/common/columns/vertex_columns.h"
#include "neug/common/types.h"
#include "neug/common/types/data_chunk.h"
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

struct JoinTable::Impl {
  ContextChunk right;
  JoinParams params;
  size_t vertex_keys = 0;
  struct Tables {
    flat_hash_map<VertexRecord, sel_vec_t> single;
    flat_hash_map<vertex_pair, sel_vec_t> dual;
    flat_hash_map<std::string, sel_vec_t> generic;
    sel_vec_t rows;
    std::vector<std::pair<std::string, sel_t>> keys;
    bool built = false;
  };
  std::vector<std::unique_ptr<Tables>> partitions;
  bool finalized = false;

  template <typename Map, typename Key>
  size_t Partition(const Key& key) const {
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

  Impl(ContextChunk input, const JoinParams& config, size_t partition_count)
      : right(std::move(input)), params(config) {
    if (partition_count == 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Join requires a build partition");
    }
    for (size_t i = 0; i < partition_count; ++i) {
      partitions.push_back(std::make_unique<Tables>());
    }
    if (params.left_columns.size() != params.right_columns.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Join columns size mismatch");
    }
    if (params.join_type == JoinKind::kTimesJoin) {
      return;
    }
    auto count = params.right_columns.size();
    if (count == 1 || count == 2) {
      vertex_keys = count;
      for (auto alias : params.right_columns) {
        if (!right.exist(alias) ||
            right.get(alias)->column_type() != ContextColumnType::kVertex) {
          vertex_keys = 0;
        }
      }
    }
    // Retain the existing generic single-key semi/anti NULL behavior.
    if ((params.join_type == JoinKind::kSemiJoin ||
         params.join_type == JoinKind::kAntiJoin) &&
        vertex_keys == 1) {
      vertex_keys = 0;
    }
    if (partitions.size() == 1) {
      return;
    }
    // Route in input order so duplicate matches keep their original order.
    // Each build task owns one bucket and never mutates another bucket's table.
    for (size_t row = 0; row < right.row_num(); ++row) {
      size_t partition;
      if (vertex_keys == 1) {
        partition = Partition<decltype(Tables::single)>(
            Vertex(right, params.right_columns[0], row));
      } else if (vertex_keys == 2) {
        partition = Partition<decltype(Tables::dual)>(
            vertex_pair{Vertex(right, params.right_columns[0], row),
                        Vertex(right, params.right_columns[1], row)});
      } else {
        auto key = Key(right, row, params.right_columns,
                       params.join_type == JoinKind::kInnerJoin);
        if (!key) {
          continue;
        }
        partition = Partition<decltype(Tables::generic)>(*key);
        partitions[partition]->keys.emplace_back(std::move(*key), row);
        continue;
      }
      partitions[partition]->rows.push_back(row);
    }
  }

  void BuildPartition(size_t partition) {
    auto& target = *partitions.at(partition);
    if (target.built) {
      return;
    }
    if (params.join_type == JoinKind::kTimesJoin) {
      target.built = true;
      return;
    }
    for (auto& entry : target.keys) {
      target.generic[std::move(entry.first)].push_back(entry.second);
    }
    decltype(target.keys){}.swap(target.keys);
    auto count = partitions.size() == 1 ? right.row_num() : target.rows.size();
    for (size_t index = 0; index < count; ++index) {
      auto row = partitions.size() == 1 ? index : target.rows[index];
      if (vertex_keys == 1) {
        target.single[Vertex(right, params.right_columns[0], row)].push_back(
            row);
      } else if (vertex_keys == 2) {
        target
            .dual[{Vertex(right, params.right_columns[0], row),
                   Vertex(right, params.right_columns[1], row)}]
            .push_back(row);
      } else {
        auto key = Key(right, row, params.right_columns,
                       params.join_type == JoinKind::kInnerJoin);
        if (key) {
          target.generic[*key].push_back(row);
        }
      }
    }
    sel_vec_t{}.swap(target.rows);
    target.built = true;
  }

  ContextChunk Probe(ContextChunk left) const {
    sel_vec_t left_rows, right_rows;
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
        for (size_t index = 0; index < right.row_num(); ++index) {
          left_rows.push_back(row);
          right_rows.push_back(index);
        }
        continue;
      }
      const sel_vec_t* matches = nullptr;
      if (vertex_keys == 1) {
        auto key = Vertex(left, params.left_columns[0], row);
        const auto& table =
            *partitions[Partition<decltype(Tables::single)>(key)];
        auto found = table.single.find(key);
        if (found != table.single.end()) {
          matches = &found->second;
        }
      } else if (vertex_keys == 2) {
        vertex_pair key{Vertex(left, params.left_columns[0], row),
                        Vertex(left, params.left_columns[1], row)};
        const auto& table = *partitions[Partition<decltype(Tables::dual)>(key)];
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
            *partitions[Partition<decltype(Tables::generic)>(*key)];
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
        right_rows.push_back(std::numeric_limits<sel_t>::max());
      }
    }
    left.reshuffle(left_rows);
    if (!semi && !anti) {
      // Copy only the column handles; reshuffle creates result columns and
      // never mutates the reusable build-side data.
      auto selected = right;
      if (outer) {
        for (auto alias : params.right_columns) {
          selected.remove(alias);
        }
        selected.optional_reshuffle(right_rows);
      } else {
        selected.reshuffle(right_rows);
      }
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
    : impl_(std::make_unique<Impl>(std::move(right), params, 1)) {
  impl_->BuildPartition(0);
  Finalize();
}
JoinTable::JoinTable(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
JoinTable::~JoinTable() = default;
std::unique_ptr<JoinTable> JoinTable::Prepare(ContextChunk right,
                                              const JoinParams& params,
                                              size_t partitions) {
  return std::unique_ptr<JoinTable>(new JoinTable(
      std::make_unique<Impl>(std::move(right), params, partitions)));
}
Status JoinTable::BuildPartition(size_t partition) {
  impl_->BuildPartition(partition);
  return Status::OK();
}
Status JoinTable::Finalize() {
  if (impl_->finalized) {
    return Status::OK();
  }
  for (const auto& partition : impl_->partitions) {
    if (!partition->built) {
      return Status::InternalError("Join build partition is unfinished");
    }
  }
  // Publication barrier only: probe routes to the immutable hash buckets.
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
