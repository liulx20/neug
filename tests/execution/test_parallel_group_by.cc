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
#include <limits>
#include "neug/common/columns/value_columns.h"
#include "neug/execution/execute/ops/retrieve/group_by.h"
#include "neug/execution/execute/ops/retrieve/group_by_utils.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/storages/graph/property_graph.h"
#include "query_test_utils.h"

namespace neug::execution {
namespace {
using Kind = physical::GroupBy_AggFunc;

physical::GroupBy Definition(bool grouped,
                             const std::vector<Kind::Aggregate>& kinds) {
  physical::GroupBy definition;
  if (grouped) {
    auto* mapping = definition.add_mappings();
    mapping->mutable_key()->mutable_tag()->set_id(0);
    mapping->mutable_alias()->set_value(3);
  }
  for (size_t i = 0; i < kinds.size(); ++i) {
    auto* func = definition.add_functions();
    func->set_aggregate(kinds[i]);
    if (i != 0 || kinds[i] != Kind::COUNT) {
      func->add_vars()->mutable_tag()->set_id(1);
    }
    func->mutable_alias()->set_value(4 + i);
  }
  return definition;
}

void SetType(common::IrDataType* output, const DataType& type) {
  auto* data = output->mutable_data_type();
  switch (type.id()) {
  case DataTypeId::kInt64:
    data->set_primitive_type(common::DT_SIGNED_INT64);
    break;
  case DataTypeId::kInt32:
    data->set_primitive_type(common::DT_SIGNED_INT32);
    break;
  case DataTypeId::kDouble:
    data->set_primitive_type(common::DT_DOUBLE);
    break;
  case DataTypeId::kVarchar:
    data->mutable_string()->mutable_long_text();
    break;
  default:
    FAIL() << "Unhandled test metadata type";
  }
}

std::unique_ptr<IOperator> MakeGroup(const physical::GroupBy& definition,
                                     const ContextChunk& input) {
  ContextMeta meta;
  for (size_t i = 0; i < input.col_num(); ++i) {
    if (input.get(i)) {
      meta.set(i, input.get(i)->elem_type());
    }
  }
  physical::PhysicalPlan plan;
  auto* op = plan.add_plan();
  *op->mutable_opr()->mutable_group_by() = definition;
  for (const auto& mapping : definition.mappings()) {
    auto* m = op->add_meta_data();
    m->set_alias(mapping.alias().value());
    SetType(m->mutable_type(), meta.get(mapping.key().tag().id()));
  }
  for (const auto& func : definition.functions()) {
    auto* m = op->add_meta_data();
    m->set_alias(func.alias().value());
    if (func.aggregate() == Kind::COUNT ||
        func.aggregate() == Kind::COUNT_DISTINCT) {
      SetType(m->mutable_type(), DataType(DataTypeId::kInt64));
    } else if (func.aggregate() == Kind::AVG) {
      SetType(m->mutable_type(), DataType(DataTypeId::kDouble));
    } else if (func.aggregate() == Kind::TO_LIST) {
      m->mutable_type()
          ->mutable_data_type()
          ->mutable_list()
          ->mutable_component_type()
          ->set_primitive_type(common::DT_SIGNED_INT64);
    } else {
      SetType(m->mutable_type(), meta.get(func.vars(0).tag().id()));
    }
  }
  auto built = ops::GroupByOprBuilder().Build(Schema(), meta, plan, 0);
  EXPECT_TRUE(built);
  return built ? std::move(built->first) : nullptr;
}

void Equal(const ContextChunk& actual, const ContextChunk& expected) {
  ASSERT_EQ(actual.row_num(), expected.row_num());
  ASSERT_EQ(actual.col_num(), expected.col_num());
  for (size_t col = 0; col < expected.col_num(); ++col) {
    ASSERT_EQ(bool(actual.get(col)), bool(expected.get(col)));
    if (!expected.get(col)) {
      continue;
    }
    EXPECT_EQ(actual.get(col)->elem_type(), expected.get(col)->elem_type());
    for (size_t row = 0; row < expected.row_num(); ++row) {
      auto a = actual.get(col)->get_elem(row);
      auto b = expected.get(col)->get_elem(row);
      ASSERT_EQ(a.IsNull(), b.IsNull());
      if (a.IsNull()) {
        continue;
      }
      if (a.type().id() == DataTypeId::kDouble) {
        EXPECT_NEAR(a.GetValue<double>(), b.GetValue<double>(),
                    1e-10 * std::max(1.0, std::abs(b.GetValue<double>())));
      } else {
        vector_t<char> ab, bb;
        Encoder ae(ab), be(bb);
        encode_value(a, ae);
        encode_value(b, be);
        EXPECT_EQ(ab, bb) << "row=" << row << " col=" << col;
      }
    }
  }
}

void Check(ChunkBatch input, const physical::GroupBy& definition,
           bool parallel = true) {
  ChunkAccumulator all;
  for (const auto& chunk : input) {
    all.Add(chunk);
  }
  auto merged = all.Finish();
  ASSERT_TRUE(merged);
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  std::vector<std::pair<int, int>> mappings;
  std::vector<physical::GroupBy_AggFunc> specs;
  ASSERT_TRUE(ops::BuildGroupByUtils(definition, mappings, specs));
  auto key = ops::create_key_func(mappings, storage, merged->chunk());
  std::vector<ReduceOp> reducers;
  for (const auto& spec : specs) {
    reducers.push_back(ops::create_reduce_op(spec, storage, merged->chunk()));
  }
  auto expected = GroupBy::group_by(ContextChunk(*merged), std::move(key),
                                    std::move(reducers));
  ASSERT_TRUE(expected);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(MakeGroup(definition, *merged));
  ASSERT_EQ(
      operators[0]->pipeline_behavior(),
      parallel ? PipelineBehavior::kPartitioned : PipelineBehavior::kGlobal);
  Pipeline pipeline(std::move(operators));
  for (size_t workers : {1, 2, 4}) {
    auto result = collect_chunk(pipeline.ExecuteReader(
        storage, context_from_batches(input), {}, nullptr, workers));
    ASSERT_TRUE(result) << result.error().ToString();
    Equal(*result, *expected);
  }
}

ContextChunk Integers(size_t batch, size_t rows, bool nullable) {
  ValueColumnBuilder<int64_t> keys, values;
  for (size_t row = 0; row < rows; ++row) {
    if (nullable && row % 11 == 0) {
      keys.push_back_null();
    } else {
      keys.push_back_opt((row + batch) % 7);
    }
    if (nullable && row % 3 == 0) {
      values.push_back_null();
    } else {
      values.push_back_opt(static_cast<int64_t>(row) - 70);
    }
  }
  ContextChunk chunk;
  chunk.set(0, keys.finish());
  chunk.set(1, values.finish());
  return chunk;
}

TEST(ParallelGroupByTest, GroupedAndUngroupedNullableEmptyAndRepeatedKeys) {
  for (bool grouped : {false, true}) {
    auto definition = Definition(grouped, {Kind::COUNT, Kind::COUNT, Kind::SUM,
                                           Kind::MIN, Kind::MAX, Kind::AVG});
    Check({Integers(0, 0, false), Integers(0, 0, false)}, definition);
    Check({Integers(0, 130, false), Integers(1, 0, false),
           Integers(2, 200, true), Integers(3, 30, true)},
          definition);
    ValueColumnBuilder<int64_t> keys, values;
    keys.push_back_opt(1);
    values.push_back_null();
    ContextChunk chunk;
    chunk.set(0, keys.finish());
    chunk.set(1, values.finish());
    Check({chunk, chunk}, definition);
  }
}

TEST(ParallelGroupByTest, FloatingSumAndAverageUsePartialCounts) {
  ChunkBatch input;
  for (size_t batch = 0; batch < 9; ++batch) {
    auto chunk = Integers(batch, 129, false);
    ValueColumnBuilder<double> values;
    for (size_t row = 0; row < 129; ++row) {
      if ((row + batch) % 5 == 0) {
        values.push_back_null();
      } else {
        values.push_back_opt((row + batch) * 0.125 - 40.0);
      }
    }
    chunk.remove(1);
    chunk.set(1, values.finish());
    input.push_back(std::move(chunk));
  }
  Check(input, Definition(true, {Kind::SUM, Kind::AVG, Kind::COUNT}));
  Check(input, Definition(false, {Kind::SUM, Kind::AVG, Kind::COUNT}));
  Check(input, Definition(true, {Kind::MIN, Kind::MAX}), false);
}

TEST(ParallelGroupByTest, CompositeKeysAndNullableStrings) {
  ChunkBatch input;
  for (size_t batch = 0; batch < 4; ++batch) {
    auto chunk = Integers(batch, 81, true);
    ValueColumnBuilder<int64_t> second;
    ValueColumnBuilder<std::string> values;
    for (size_t row = 0; row < 81; ++row) {
      second.push_back_opt(row % 3);
      if (row % 5 == 0) {
        values.push_back_null();
      } else {
        values.push_back_opt(std::to_string((row + batch) % 13));
      }
    }
    chunk.remove(1);
    chunk.set(1, values.finish());
    chunk.set(2, second.finish());
    input.push_back(std::move(chunk));
  }
  auto definition =
      Definition(true, {Kind::COUNT, Kind::COUNT, Kind::MIN, Kind::MAX});
  auto* mapping = definition.add_mappings();
  mapping->mutable_key()->mutable_tag()->set_id(2);
  mapping->mutable_alias()->set_value(2);
  Check(input, definition);
}

TEST(ParallelGroupByTest, Int32SumKeepsInputWidth) {
  auto chunk = Integers(0, 6, false);
  ValueColumnBuilder<int32_t> values;
  for (int32_t value : {-9, 3, 0, 5, 6, -2}) {
    values.push_back_opt(value);
  }
  chunk.remove(1);
  chunk.set(1, values.finish());
  Check({chunk, chunk},
        Definition(false, {Kind::SUM, Kind::MIN, Kind::MAX, Kind::AVG}));
}

TEST(ParallelGroupByTest, DistinctAndListKeepGlobalKernel) {
  auto input = ChunkBatch{Integers(0, 20, true), Integers(1, 50, true)};
  Check(input, Definition(true, {Kind::COUNT_DISTINCT, Kind::TO_LIST}), false);
}

TEST(ParallelGroupByTest, EofWithoutABatchProducesUngroupedEmptyValues) {
  auto op = MakeGroup(
      Definition(false, {Kind::COUNT, Kind::SUM, Kind::MIN, Kind::AVG}),
      Integers(0, 0, false));
  auto state = op->CreatePartitionState(4);
  ASSERT_TRUE(state->FinalizeBuild());
  auto output = state->TakeOutput();
  ASSERT_EQ(output.size(), 1);
  ASSERT_EQ(output[0].row_num(), 1);
  EXPECT_EQ(output[0].get(4)->get_elem(0).GetValue<int64_t>(), 0);
  EXPECT_EQ(output[0].get(5)->get_elem(0).GetValue<int64_t>(), 0);
  EXPECT_TRUE(output[0].get(6)->get_elem(0).IsNull());
  EXPECT_TRUE(output[0].get(7)->get_elem(0).IsNull());
}

TEST(ParallelGroupByTest, ConcurrentStateBuildAndFixedWidthSums) {
  auto sample = Integers(0, 1, false);
  auto op = MakeGroup(Definition(false, {Kind::SUM}), sample);
  auto state = op->CreatePartitionState(4);
  std::vector<std::future<std::shared_ptr<PartitionState::Batch>>> jobs;
  for (size_t batch = 0; batch < 8; ++batch) {
    jobs.push_back(std::async(std::launch::async, [&, batch] {
      ValueColumnBuilder<int64_t> values;
      values.push_back_opt(std::numeric_limits<int64_t>::max());
      values.push_back_opt(1);
      auto input = Integers(0, 2, false);
      input.remove(1);
      input.set(1, values.finish());
      return state->PartitionBuild(std::move(input));
    }));
  }
  for (auto& job : jobs) {
    auto input = job.get();
    std::vector<std::future<Status>> lanes;
    for (size_t part = 0; part < 4; ++part) {
      lanes.push_back(std::async(std::launch::async, [&, part] {
        return state->BuildPartition(part, *input);
      }));
    }
    for (auto& lane : lanes) {
      ASSERT_TRUE(lane.get());
    }
  }
  ASSERT_TRUE(state->FinalizeBuild());
  auto output = state->TakeOutput();
  ASSERT_EQ(output.size(), 1);
  ASSERT_EQ(output[0].row_num(), 1);
  EXPECT_EQ(output[0].get(4)->get_elem(0).GetValue<int64_t>(), 0);
}
}  // namespace
}  // namespace neug::execution
