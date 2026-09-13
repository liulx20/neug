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
#include <gtest/gtest.h>

#include <future>
#include <optional>
#include <vector>

#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/execution/common/operators/retrieve/join.h"
#include "neug/execution/utils/params.h"
#include "query_test_utils.h"

namespace neug::execution {
namespace {
struct Row {
  VertexRecord first;
  VertexRecord second;
  int64_t payload;
};

std::vector<Row> Rows(size_t size, bool right) {
  const std::vector<VertexRecord> first = {{0, 7}, {1, 7}, {0, 7}, {0, 9}};
  const std::vector<VertexRecord> second = {{1, 2}, {1, 2}, {1, 3}, {0, 4}};
  std::vector<Row> rows;
  for (size_t i = 0; i < size; ++i) {
    auto index = right ? (i * 3 + 1) % first.size() : i % first.size();
    rows.push_back({first[index], second[index], static_cast<int64_t>(i)});
  }
  return rows;
}

ContextChunk VertexChunk(const std::vector<Row>& rows, bool right) {
  MLVertexColumnBuilder first(std::set<label_t>{0, 1});
  MLVertexColumnBuilder second(std::set<label_t>{0, 1});
  ValueColumnBuilder<int64_t> payload;
  for (const auto& row : rows) {
    first.push_back_opt(row.first);
    second.push_back_opt(row.second);
    payload.push_back_opt(row.payload);
  }
  ContextChunk chunk;
  int base = right ? 3 : 0;
  chunk.set(base, first.finish());
  chunk.set(base + 1, second.finish());
  chunk.set(base + 2, payload.finish());
  return chunk;
}

JoinParams Params(bool dual, JoinKind kind) {
  return {dual ? std::vector<int>{0, 1} : std::vector<int>{0},
          dual ? std::vector<int>{3, 4} : std::vector<int>{3}, kind};
}

using Match = std::pair<int64_t, std::optional<int64_t>>;
std::vector<Match> Expected(const std::vector<Row>& left,
                            const std::vector<Row>& right, bool dual,
                            JoinKind kind) {
  std::vector<Match> expected;
  for (const auto& l : left) {
    bool matched = false;
    for (const auto& r : right) {
      if (l.first == r.first && (!dual || l.second == r.second)) {
        matched = true;
        if (kind == JoinKind::kInnerJoin || kind == JoinKind::kLeftOuterJoin) {
          expected.emplace_back(l.payload, r.payload);
        }
      }
    }
    if ((kind == JoinKind::kLeftOuterJoin && !matched) ||
        (kind == JoinKind::kSemiJoin && matched) ||
        (kind == JoinKind::kAntiJoin && !matched)) {
      expected.emplace_back(l.payload, std::nullopt);
    }
  }
  return expected;
}

void Check(const ContextChunk& result, const std::vector<Match>& expected,
           JoinKind kind) {
  ASSERT_EQ(result.row_num(), expected.size());
  EXPECT_FALSE(result.head());
  for (size_t row = 0; row < expected.size(); ++row) {
    EXPECT_EQ(result.get(2)->get_elem(row).GetValue<int64_t>(),
              expected[row].first);
    if (kind == JoinKind::kSemiJoin || kind == JoinKind::kAntiJoin) {
      continue;
    }
    auto value = result.get(5)->get_elem(row);
    if (expected[row].second) {
      ASSERT_FALSE(value.IsNull());
      EXPECT_EQ(value.GetValue<int64_t>(), *expected[row].second);
    } else {
      EXPECT_TRUE(value.IsNull());
    }
  }
}

TEST(HashJoinTest, VertexKeysUseLeftProbeOrderForBothSizeRelationships) {
  for (bool dual : {false, true}) {
    for (auto kind : {JoinKind::kInnerJoin, JoinKind::kLeftOuterJoin,
                      JoinKind::kSemiJoin, JoinKind::kAntiJoin}) {
      for (size_t left_size : {0, 1, 4, 9}) {
        for (size_t right_size : {0, 1, 3, 7}) {
          SCOPED_TRACE(::testing::Message()
                       << "dual=" << dual << " kind=" << static_cast<int>(kind)
                       << " left=" << left_size << " right=" << right_size);
          auto left = Rows(left_size, false);
          auto right = Rows(right_size, true);
          auto result =
              Join::join(VertexChunk(left, false), VertexChunk(right, true),
                         Params(dual, kind));
          ASSERT_TRUE(result) << result.error().ToString();
          Check(*result, Expected(left, right, dual, kind), kind);
        }
      }
    }
  }
}

result<ContextChunk> Batched(const std::vector<Row>& rows, bool right,
                             size_t size) {
  std::vector<ContextChunk> chunks;
  for (size_t begin = 0; begin < rows.size(); begin += size) {
    auto end = std::min(begin + size, rows.size());
    chunks.push_back(VertexChunk(
        std::vector<Row>(rows.begin() + begin, rows.begin() + end), right));
  }
  return collect_chunk(context_from_batches(std::move(chunks)));
}

TEST(HashJoinTest, MatchesAcrossChunkBoundariesRetainAllDuplicates) {
  auto left_rows = Rows(9, false);
  auto right_rows = Rows(7, true);
  for (bool dual : {false, true}) {
    for (auto kind : {JoinKind::kInnerJoin, JoinKind::kLeftOuterJoin}) {
      auto left = Batched(left_rows, false, 2);
      auto right = Batched(right_rows, true, 3);
      ASSERT_TRUE(left);
      ASSERT_TRUE(right);
      auto result =
          Join::join(std::move(*left), std::move(*right), Params(dual, kind));
      ASSERT_TRUE(result);
      Check(*result, Expected(left_rows, right_rows, dual, kind), kind);
    }
  }
}

TEST(HashJoinTest, BuildTableIsReusableAcrossProbeChunks) {
  auto right = Rows(7, true);
  for (bool dual : {false, true}) {
    for (auto kind : {JoinKind::kInnerJoin, JoinKind::kLeftOuterJoin,
                      JoinKind::kSemiJoin, JoinKind::kAntiJoin}) {
      JoinTable table(VertexChunk(right, true), Params(dual, kind));
      for (size_t count : {0, 1, 9, 4, 9}) {
        auto left = Rows(count, false);
        auto output = table.Probe(VertexChunk(left, false));
        ASSERT_TRUE(output);
        Check(*output, Expected(left, right, dual, kind), kind);
      }
    }
  }
}

TEST(HashJoinTest, ParallelBuildFinalizesBeforeConcurrentProbe) {
  for (bool dual : {false, true}) {
    for (auto kind : {JoinKind::kInnerJoin, JoinKind::kLeftOuterJoin,
                      JoinKind::kSemiJoin, JoinKind::kAntiJoin}) {
      auto right = Rows(17, true);
      auto table =
          JoinTable::Prepare(VertexChunk(right, true), Params(dual, kind), 4);
      EXPECT_FALSE(table->Finalize());
      EXPECT_FALSE(table->Probe(VertexChunk(Rows(1, false), false)));
      std::vector<std::future<Status>> builds;
      for (size_t part = 0; part < 4; ++part) {
        builds.push_back(std::async(std::launch::async, [&, part] {
          return table->BuildPartition(part);
        }));
      }
      for (auto& build : builds) {
        ASSERT_TRUE(build.get());
      }
      ASSERT_TRUE(table->Finalize());
      ASSERT_TRUE(table->Finalize());
      std::vector<std::future<result<ContextChunk>>> probes;
      for (size_t count : {0, 1, 9, 17}) {
        probes.push_back(std::async(std::launch::async, [&, count] {
          return table->Probe(VertexChunk(Rows(count, false), false));
        }));
      }
      size_t counts[] = {0, 1, 9, 17};
      for (size_t i = 0; i < probes.size(); ++i) {
        auto result = probes[i].get();
        ASSERT_TRUE(result);
        Check(*result, Expected(Rows(counts[i], false), right, dual, kind),
              kind);
      }
    }
  }
}

TEST(HashJoinTest, GenericInnerJoinSkipsNullKeysAndKeepsDuplicateMatches) {
  auto make = [](bool right) {
    ValueColumnBuilder<int64_t> key;
    ValueColumnBuilder<int64_t> payload;
    for (int64_t i = 0; i < 4; ++i) {
      if (i == 1) {
        key.push_back_null();
      } else {
        key.push_back_opt(i == 3 ? 9 : 7);
      }
      payload.push_back_opt(i);
    }
    ContextChunk chunk;
    chunk.set(right ? 3 : 0, key.finish());
    chunk.set(right ? 5 : 2, payload.finish());
    return chunk;
  };
  for (size_t partitions : {1, 3, 8}) {
    auto table = JoinTable::Prepare(
        make(true), Params(false, JoinKind::kInnerJoin), partitions);
    std::vector<std::future<Status>> builds;
    for (size_t part = 0; part < partitions; ++part) {
      builds.push_back(std::async(std::launch::async, [&, part] {
        return table->BuildPartition(part);
      }));
    }
    for (auto& build : builds) {
      ASSERT_TRUE(build.get());
    }
    ASSERT_TRUE(table->Finalize());
    auto result = table->Probe(make(false));
    ASSERT_TRUE(result);
    Check(*result, {{0, 0}, {0, 2}, {2, 0}, {2, 2}, {3, 3}},
          JoinKind::kInnerJoin);
  }
}

TEST(HashJoinTest, HashPartitionsPreserveSkewedAndMissingKeyOrder) {
  for (size_t partitions : {1, 3, 8}) {
    for (bool dual : {false, true}) {
      for (auto kind : {JoinKind::kInnerJoin, JoinKind::kLeftOuterJoin,
                        JoinKind::kSemiJoin, JoinKind::kAntiJoin}) {
        for (size_t size : {0, 1, 127}) {
          auto right = Rows(size, true);
          for (auto& row : right) {
            row.first = {0, 7};
            row.second = {1, 2};
          }
          auto table = JoinTable::Prepare(VertexChunk(right, true),
                                          Params(dual, kind), partitions);
          std::vector<std::future<Status>> builds;
          for (size_t part = 0; part < partitions; ++part) {
            builds.push_back(std::async(std::launch::async, [&, part] {
              return table->BuildPartition(part);
            }));
          }
          for (auto& build : builds) {
            ASSERT_TRUE(build.get());
          }
          ASSERT_TRUE(table->Finalize());
          auto left = Rows(19, false);
          auto output = table->Probe(VertexChunk(left, false));
          ASSERT_TRUE(output);
          Check(*output, Expected(left, right, dual, kind), kind);
        }
      }
    }
  }
}
}  // namespace
}  // namespace neug::execution
