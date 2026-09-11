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
  flat_hash_map<VertexRecord, sel_vec_t> single;
  flat_hash_map<vertex_pair, sel_vec_t> dual;
  flat_hash_map<std::string, sel_vec_t> generic;

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

  Impl(ContextChunk input, const JoinParams& config)
      : right(std::move(input)), params(config) {
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
    for (size_t row = 0; row < right.row_num(); ++row) {
      if (vertex_keys == 1) {
        single[Vertex(right, params.right_columns[0], row)].push_back(row);
      } else if (vertex_keys == 2) {
        dual[{Vertex(right, params.right_columns[0], row),
              Vertex(right, params.right_columns[1], row)}]
            .push_back(row);
      } else {
        auto key = Key(right, row, params.right_columns,
                       params.join_type == JoinKind::kInnerJoin);
        if (key) {
          generic[*key].push_back(row);
        }
      }
    }
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
        auto found = single.find(Vertex(left, params.left_columns[0], row));
        if (found != single.end()) {
          matches = &found->second;
        }
      } else if (vertex_keys == 2) {
        auto found = dual.find({Vertex(left, params.left_columns[0], row),
                                Vertex(left, params.left_columns[1], row)});
        if (found != dual.end()) {
          matches = &found->second;
        }
      } else {
        auto key = Key(left, row, params.left_columns, !outer);
        if (!key) {
          continue;
        }
        auto found = generic.find(*key);
        if (found != generic.end()) {
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
    : impl_(std::make_unique<Impl>(std::move(right), params)) {}
JoinTable::~JoinTable() = default;
result<ContextChunk> JoinTable::Probe(ContextChunk left) const {
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
