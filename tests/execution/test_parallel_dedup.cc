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
#include "neug/common/columns/edge_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/execution/common/operators/retrieve/dedup.h"
#include "neug/execution/execute/ops/retrieve/dedup.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/encoder.h"
#include "query_test_utils.h"

namespace neug::execution {
namespace {
ContextChunk Rows(const std::vector<std::optional<int64_t>>& values,
                  int64_t base = 0) {
  ValueColumnBuilder<int64_t> keys, payload;
  ValueColumnBuilder<std::string> text;
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i]) {
      keys.push_back_opt(*values[i]);
    } else {
      keys.push_back_null();
    }
    text.push_back_opt(std::to_string((base + i) % 3));
    payload.push_back_opt(base + i);
  }
  ContextChunk chunk;
  chunk.set(0, keys.finish());
  chunk.set(1, text.finish());
  chunk.set(2, payload.finish());
  return chunk;
}

std::unique_ptr<IOperator> MakeDedup(const std::vector<int32_t>& keys,
                                     const ContextChunk& sample) {
  ContextMeta meta;
  for (size_t i = 0; i < sample.col_num(); ++i) {
    if (sample.get(i)) {
      meta.set(i, sample.get(i)->elem_type());
    }
  }
  physical::PhysicalPlan plan;
  auto* dedup = plan.add_plan()->mutable_opr()->mutable_dedup();
  for (auto key : keys) {
    dedup->add_keys()->mutable_tag()->set_id(key);
  }
  auto built = ops::DedupOprBuilder().Build(Schema(), meta, plan, 0);
  EXPECT_TRUE(built);
  return built ? std::move(built->first) : nullptr;
}

void Equal(const ContextChunk& actual, const ContextChunk& expected) {
  ASSERT_EQ(actual.row_num(), expected.row_num());
  ASSERT_EQ(actual.col_num(), expected.col_num());
  EXPECT_EQ(bool(actual.head()), bool(expected.head()));
  for (size_t alias = 0; alias < actual.col_num(); ++alias) {
    ASSERT_EQ(bool(actual.get(alias)), bool(expected.get(alias)));
    if (!actual.get(alias)) {
      continue;
    }
    for (size_t row = 0; row < actual.row_num(); ++row) {
      vector_t<char> a, b;
      Encoder ae(a), be(b);
      encode_value(actual.get(alias)->get_elem(row), ae);
      encode_value(expected.get(alias)->get_elem(row), be);
      EXPECT_EQ(a, b) << "alias=" << alias << " row=" << row;
      if (actual.get(alias)->elem_type().id() == DataTypeId::kEdge) {
        EXPECT_EQ(actual.get(alias)->get_elem(row).GetValue<edge_t>().prop,
                  expected.get(alias)->get_elem(row).GetValue<edge_t>().prop);
      }
    }
  }
}

void Check(ChunkBatch input, std::vector<int32_t> keys) {
  ChunkAccumulator merged;
  for (auto& chunk : input) {
    merged.Add(chunk);
  }
  auto all = merged.Finish();
  ASSERT_TRUE(all);
  auto expected = Dedup::dedup(ContextChunk(*all), keys);
  ASSERT_TRUE(expected);
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(MakeDedup(keys, *all));
  Pipeline pipeline(std::move(operators));
  for (size_t workers : {1, 2, 4}) {
    auto output = collect_chunk(pipeline.ExecuteReader(
        storage, context_from_batches(input), {}, nullptr, workers));
    ASSERT_TRUE(output) << output.error().ToString();
    Equal(*output, *expected);
  }
}

TEST(ParallelDedupTest, MatchesExistingOrderAcrossNullableAndCompositeBatches) {
  for (auto keys : {std::vector<int32_t>{0}, std::vector<int32_t>{0, 1},
                    std::vector<int32_t>{}}) {
    Check({Rows({3, 1, 3}), Rows({}), Rows({2, 1, 4, 3}, 3)}, keys);
    Check({Rows({3, 1}), Rows({std::nullopt, 3, 1}, 2),
           Rows({2, std::nullopt, 4, 3}, 5)},
          keys);
    Check({Rows({}), Rows({})}, keys);
  }
}

TEST(ParallelDedupTest, MixedReducedAndUnreducedBatchesKeepGlobalUniqueness) {
  ChunkBatch input;
  for (int batch = 0; batch < 6; ++batch) {
    std::vector<std::optional<int64_t>> values;
    for (int row = 0; row < 128; ++row) {
      values.push_back(batch % 2 == 0 ? row : row % 5);
    }
    if (batch == 5) {
      values.push_back(std::nullopt);
    }
    input.push_back(Rows(values));
  }
  Check(std::move(input), {0});
}

TEST(ParallelDedupTest, ScalarEdgePropertyIdentityUsesEstablishedHelper) {
  int properties[2] = {1, 2};
  SDSLEdgeColumnBuilder edges(Direction::kOut, {0, 0, 0});
  edges.push_back_opt(1, 2, &properties[0]);
  edges.push_back_opt(1, 2, &properties[1]);
  edges.push_back_opt(1, 2, &properties[0]);
  ContextChunk chunk;
  chunk.set(0, edges.finish());
  Check({chunk}, {0});
  Check({chunk, chunk}, {0});
}

TEST(ParallelDedupTest, ScalarFloatNullabilityAndSignedZeroRemainCompatible) {
  for (bool optional : {false, true}) {
    ChunkBatch input;
    for (int batch = 0; batch < 3; ++batch) {
      ValueColumnBuilder<double> values;
      values.push_back_opt(batch == 0 ? -0.0 : 0.0);
      values.push_back_opt(2.0);
      values.push_back_opt(-1.0);
      if (optional && batch == 2) {
        values.push_back_null();
      }
      ContextChunk chunk;
      chunk.set(0, values.finish());
      input.push_back(std::move(chunk));
    }
    Check(std::move(input), {0});
  }
}

TEST(ParallelDedupTest, VertexLabelsAndRepeatedKeysAcrossChunks) {
  ChunkBatch input;
  for (int batch = 0; batch < 8; ++batch) {
    MLVertexColumnBuilder vertices;
    for (int row = 0; row < 100; ++row) {
      vertices.push_back_opt({static_cast<label_t>(row % 2),
                              static_cast<vid_t>((row + batch) % 7)});
    }
    ContextChunk chunk;
    chunk.set(0, vertices.finish());
    input.push_back(std::move(chunk));
  }
  Check(std::move(input), {0});
}

TEST(ParallelDedupTest,
     StateSeparatesConcurrentPartitioningFromOrderedAccumulation) {
  auto sample = Rows({3, 1, 2, 3});
  auto op = MakeDedup({0}, sample);
  auto state = op->CreatePartitionState(4);
  std::vector<std::future<std::shared_ptr<PartitionState::Batch>>> jobs;
  for (int i = 0; i < 12; ++i) {
    jobs.push_back(std::async(std::launch::async, [&, i] {
      std::vector<std::optional<int64_t>> rows;
      for (int row = 0; row < 128; ++row) {
        rows.push_back((row + i) % 5);
      }
      return state->PartitionBuild(Rows(rows));
    }));
  }
  for (auto& job : jobs) {
    auto batch = job.get();
    std::vector<std::future<Status>> work;
    for (size_t part = 0; part < state->BuildPartitions(); ++part) {
      work.push_back(std::async(std::launch::async, [&, part] {
        return state->BuildPartition(part, *batch);
      }));
    }
    for (auto& task : work) {
      ASSERT_TRUE(task.get());
    }
  }
  ASSERT_TRUE(state->FinalizeBuild());
  auto output = state->TakeOutput();
  ASSERT_EQ(output.size(), 1);
  ASSERT_EQ(output[0].row_num(), 5);
  for (size_t row = 0; row < 5; ++row) {
    EXPECT_EQ(output[0].get(0)->get_elem(row).GetValue<int64_t>(), row);
  }
  EXPECT_TRUE(state->TakeOutput().empty());
}
}  // namespace
}  // namespace neug::execution
