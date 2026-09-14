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
#include <google/protobuf/arena.h>
#include <gtest/gtest.h>
#include <set>

#include "neug/common/columns/array_columns.h"
#include "neug/common/columns/list_columns.h"
#include "neug/common/columns/path_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/execution/common/operators/retrieve/edge_expand.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/execution/execute/ops/retrieve/limit.h"
#include "neug/execution/execute/ops/retrieve/sink.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/property_graph.h"
#include "query_test_utils.h"

namespace neug::execution {
namespace ops {
std::unique_ptr<IOperator> make_tc_opr(
    const physical::EdgeExpand&, const physical::EdgeExpand&,
    const physical::EdgeExpand&, const LabelTriplet&, const LabelTriplet&,
    const LabelTriplet&, const std::array<DataTypeId, 3>&, int, int);
}

namespace {

DataChunk chunk(int64_t value, int alias = 0) {
  ValueColumnBuilder<int64_t> builder;
  builder.push_back_opt(value);
  DataChunk out;
  out.set(alias, builder.finish());
  return out;
}

TEST(QueryResultTest, TriangleFusionPreservesSkippedRootOffsetsAcrossWorkers) {
  NeugDB db;
  ASSERT_TRUE(db.Open("", 4));
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query("CREATE NODE TABLE V(id INT64, PRIMARY KEY(id))"));
  ASSERT_TRUE(conn->Query("CREATE REL TABLE E(FROM V TO V, weight INT64)"));
  ASSERT_TRUE(
      conn->Query("CREATE (:V {id:0}), (:V {id:1}), (:V {id:2}), (:V {id:3})"));
  for (const auto& query : {"MATCH (a:V),(b:V) WHERE a.id=1 AND b.id=3 CREATE "
                            "(a)-[:E {weight:2}]->(b)",
                            "MATCH (a:V),(b:V) WHERE a.id=1 AND b.id=2 CREATE "
                            "(a)-[:E {weight:4}]->(b)",
                            "MATCH (a:V),(b:V) WHERE a.id=2 AND b.id=3 CREATE "
                            "(a)-[:E {weight:0}]->(b)"}) {
    ASSERT_TRUE(conn->Query(query));
  }
  // The fixture is no longer mutated; GraphView's API takes a mutable
  // reference.
  GraphView view(const_cast<PropertyGraph&>(db.graph()));
  StorageReadInterface storage(view, std::numeric_limits<timestamp_t>::max());
  auto label = db.schema().get_vertex_label_id("V");
  auto edge = db.schema().get_edge_label_id("E");
  EXPECT_EQ(storage.GetGenericOutgoingGraphView(label, label, edge).type(),
            CsrViewType::kMultipleMutable);
  EXPECT_TRUE(storage.GetEdgeDataAccessor(label, label, edge, 0).is_bundled());
  std::vector<vid_t> ids;
  for (int64_t id = 0; id < 4; ++id) {
    vid_t lid;
    ASSERT_TRUE(db.graph().get_lid(label, Value::INT64(id), lid,
                                   std::numeric_limits<timestamp_t>::max()));
    ids.push_back(lid);
  }
  for (bool less : {false, true}) {
    for (size_t workers : {1, 2, 4}) {
      physical::EdgeExpand edge0, edge1, edge2;
      for (auto* op : {&edge0, &edge1, &edge2}) {
        op->set_direction(physical::EdgeExpand_Direction_OUT);
      }
      edge0.mutable_v_tag()->set_value(0);
      auto* predicate = edge0.mutable_params()->mutable_predicate();
      predicate->add_operators()->mutable_var();
      predicate->add_operators()->set_logical(less ? ::common::Logical::LT
                                                   : ::common::Logical::GT);
      predicate->add_operators()->mutable_param()->set_name("threshold");
      auto op = ops::make_tc_opr(
          edge0, edge1, edge2, {label, label, edge}, {label, label, edge},
          {label, label, edge},
          {DataTypeId::kInt64, DataTypeId::kInt64, DataTypeId::kInt64}, 1, 2);
      ASSERT_TRUE(op);
      EXPECT_EQ(op->pipeline_behavior(), PipelineBehavior::kChunkLocal);
      std::vector<std::unique_ptr<IOperator>> operators;
      operators.push_back(std::move(op));
      Pipeline pipeline(std::move(operators));
      MSVertexColumnBuilder roots(label);
      ValueColumnBuilder<int64_t> ordinals;
      for (size_t row = 0; row < 6000; ++row) {
        roots.push_back_opt(ids[row % 4]);
        ordinals.push_back_opt(row);
      }
      ContextChunk chunk;
      chunk.set(0, roots.finish());
      chunk.set(9, ordinals.finish());
      Context input;
      input.append_chunk(std::move(chunk));
      auto result = collect_chunk(pipeline.ExecuteReader(
          storage, std::move(input),
          {{"threshold", Value::INT64(less ? 3 : 1)}}, nullptr, workers));
      ASSERT_TRUE(result);
      ASSERT_EQ(result->row_num(), 1500);
      std::set<int64_t> rows;
      for (size_t row = 0; row < result->row_num(); ++row) {
        auto ordinal = result->get(9)->get_elem(row).GetValue<int64_t>();
        EXPECT_EQ(ordinal % 4, 1);
        EXPECT_TRUE(rows.insert(ordinal).second);
        for (int alias : {0, 1, 2}) {
          auto col =
              std::dynamic_pointer_cast<IVertexColumn>(result->get(alias));
          EXPECT_EQ(col->get_vertex(row).vid_, ids[alias + 1]);
        }
      }
    }
  }
  conn->Close();
  db.Close();
}

TEST(QueryResultTest, PathColumnConcatenationPreservesNullAndZeroHopPaths) {
  Path zero(0, 12);
  auto one = zero.expand(0, 0, 13, Direction::kOut, nullptr);
  PathColumnBuilder left_builder, right_builder, empty_builder;
  left_builder.push_back_opt(zero);
  left_builder.push_back_null();
  right_builder.push_back_opt(one);
  right_builder.push_back_opt(zero);
  auto left = left_builder.finish();
  auto right = right_builder.finish();
  auto empty = empty_builder.finish();
  auto output = left->union_col(empty)->union_col(right);
  ASSERT_EQ(output->size(), 4);
  EXPECT_TRUE(output->is_optional());
  EXPECT_EQ(PathValue::Get(output->get_elem(0)), zero);
  EXPECT_TRUE(output->get_elem(1).IsNull());
  EXPECT_EQ(PathValue::Get(output->get_elem(2)), one);
  EXPECT_EQ(PathValue::Get(output->get_elem(3)).length(), 0);
  auto reversed = right->union_col(left);
  EXPECT_TRUE(reversed->get_elem(3).IsNull());
  EXPECT_EQ(left->size(), 2);
  EXPECT_EQ(right->size(), 2);
  EXPECT_EQ(empty->union_col(empty)->size(), 0);
}

TEST(QueryResultTest, BatchAccumulatorPreservesOrderAndBoundsCopies) {
  struct Merge {
    size_t* copied;
    std::vector<int> operator()(const std::vector<int>& left,
                                const std::vector<int>& right) const {
      *copied += left.size() + right.size();
      auto out = left;
      out.insert(out.end(), right.begin(), right.end());
      return out;
    }
  };
  size_t copied = 0;
  OrderedBatchAccumulator<std::vector<int>, Merge> accumulator(Merge{&copied});
  std::vector<int> expected;
  // Non-power-of-two count and uneven batches exercise carry and tail merges.
  for (size_t batch = 0; batch < 1025; ++batch) {
    std::vector<int> input;
    const size_t rows = batch == 0 ? 4096 : batch % 7;
    for (size_t row = 0; row < rows; ++row) {
      input.push_back(expected.size());
      expected.push_back(expected.size());
    }
    accumulator.Add(std::move(input));
  }
  auto output = accumulator.Finish();
  ASSERT_TRUE(output);
  EXPECT_EQ(*output, expected);
  EXPECT_LE(copied, expected.size() * 22);
  EXPECT_FALSE(accumulator.Finish());
  accumulator.Add({42});
  EXPECT_EQ(*accumulator.Finish(), (std::vector<int>{42}));
}

TEST(QueryResultTest, BalancedChunksPreserveNullsSparseAliasesAndHead) {
  ChunkAccumulator accumulator;
  size_t row = 0;
  std::vector<std::optional<int64_t>> expected;
  for (size_t batch = 0; batch < 37; ++batch) {
    ValueColumnBuilder<int64_t> builder;
    for (size_t i = 0; i < batch % 5; ++i, ++row) {
      if (row % 7 == 0) {
        builder.push_back_null();
        expected.push_back(std::nullopt);
      } else {
        builder.push_back_opt(row);
        expected.push_back(row);
      }
    }
    ContextChunk input;
    input.set(3, builder.finish());
    accumulator.Add(std::move(input));
  }
  auto output = accumulator.Finish();
  ASSERT_TRUE(output);
  EXPECT_EQ(output->head(), output->get(3));
  ASSERT_EQ(output->row_num(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    auto value = output->get(3)->get_elem(i);
    EXPECT_EQ(value.IsNull(), !expected[i]);
    if (expected[i]) {
      EXPECT_EQ(value.GetValue<int64_t>(), *expected[i]);
    }
  }
  ContextChunk single;
  single.set(3, chunk(9).get(0));
  auto identity = single.head();
  accumulator.Add(std::move(single));
  EXPECT_EQ(accumulator.Finish()->head(), identity);
}

TEST(QueryResultTest, NestedColumnsMergeAcrossBatchesAndSerialize) {
  std::function<void(const Value&, const Value&)> expect_value;
  expect_value = [&](const Value& actual, const Value& expected) {
    ASSERT_EQ(actual.type(), expected.type());
    ASSERT_EQ(actual.IsNull(), expected.IsNull());
    if (expected.IsNull()) {
      return;
    }
    if (expected.type().id() == DataTypeId::kList ||
        expected.type().id() == DataTypeId::kArray) {
      const auto& actual_children = expected.type().id() == DataTypeId::kList
                                        ? ListValue::GetChildren(actual)
                                        : ArrayValue::GetChildren(actual);
      const auto& expected_children = expected.type().id() == DataTypeId::kList
                                          ? ListValue::GetChildren(expected)
                                          : ArrayValue::GetChildren(expected);
      ASSERT_EQ(actual_children.size(), expected_children.size());
      for (size_t i = 0; i < actual_children.size(); ++i) {
        expect_value(actual_children[i], expected_children[i]);
      }
    } else {
      EXPECT_EQ(actual, expected);
    }
  };
  const auto list_type = DataType::List(DataType::INT32);
  const auto array_type = DataType::Array(DataType::INT32, 2);
  const auto nested_type = DataType::Array(array_type, 2);
  const auto pair =
      Value::ARRAY(array_type, {Value::INT32(1), Value(DataType::INT32)});
  for (const auto& pattern : std::vector<std::vector<Value>>{
           {Value::LIST(DataType::INT32,
                        {Value::INT32(1), Value(DataType::INT32)}),
            Value(list_type), Value::LIST(DataType::INT32, {}),
            Value::LIST(DataType::INT32, {Value::INT32(4)})},
           {pair, Value(array_type), pair, Value(array_type)},
           {Value::ARRAY(nested_type, {pair, pair}), Value(nested_type),
            Value::ARRAY(nested_type, {pair, pair}), Value(nested_type)}}) {
    std::vector<Value> values;
    for (size_t repeat = 0; repeat < 17; ++repeat) {
      values.insert(values.end(), pattern.begin(), pattern.end());
    }
    std::vector<ContextChunk> batches;
    for (size_t begin = 0; begin < values.size(); begin += 2) {
      auto builder = ColumnsUtils::create_builder(values.front().type());
      builder->push_back_elem(values[begin]);
      builder->push_back_elem(values[begin + 1]);
      ContextChunk batch;
      batch.set(0, builder->finish());
      batches.push_back(std::move(batch));
    }
    auto merged = collect_chunk(context_from_batches(batches));
    ASSERT_TRUE(merged);
    ASSERT_EQ(merged->row_num(), values.size());
    for (size_t row = 0; row < values.size(); ++row) {
      expect_value(merged->get(0)->get_elem(row), values[row]);
    }
    EXPECT_EQ(merged->head(), merged->get(0));
    PropertyGraph result_graph;
    GraphView result_view(result_graph);
    StorageReadInterface result_storage(result_view, 0);
    Pipeline result_pipeline;
    auto result = result_pipeline.Execute(
        result_storage, context_from_batches(std::move(batches), {0}), {},
        nullptr);
    ASSERT_TRUE(result);
    PropertyGraph graph;
    GraphView view(graph);
    StorageReadInterface storage(view, 0);
    QueryResponse response;
    Sink::sink_results(*result, storage, &response);
    EXPECT_EQ(response.row_count(), values.size());
    EXPECT_EQ(response.arrays_size(), 1);
    Context reference;
    reference.tag_ids = {0};
    reference.append_chunk(std::move(*merged));
    QueryResponse expected_response;
    Sink::sink_results(reference, storage, &expected_response);
    EXPECT_EQ(response.SerializeAsString(),
              expected_response.SerializeAsString());
  }
}

TEST(QueryResultTest, PrimitiveChunkSerializationMatchesCollectedColumns) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  auto check = [&]<typename T>() {
    for (int mode : {0, 1, 2}) {
      for (const auto& sizes :
           {std::vector<size_t>{3, 0, 7, 5}, std::vector<size_t>{0, 0}}) {
        Context input;
        input.tag_ids = {2, 0, 2};
        size_t offset = 0, batch = 0;
        for (auto size : sizes) {
          ValueColumnBuilder<T> values;
          ValueColumnBuilder<int64_t> ordinals;
          for (size_t row = 0; row < size; ++row) {
            ordinals.push_back_opt(offset + row);
            if (mode == 2 || (mode == 1 && batch % 2 == 0 && row % 3 == 1)) {
              values.push_back_null();
            } else if constexpr (std::is_same_v<T, std::string>) {
              values.push_back_opt(std::string("a\0b", 3) +
                                   std::to_string(offset + row));
            } else {
              values.push_back_opt(static_cast<T>(offset + row));
            }
          }
          ContextChunk chunk;
          chunk.set(0, ordinals.finish());
          chunk.set(2, values.finish());
          input.append_chunk(std::move(chunk));
          offset += size;
          ++batch;
        }
        Context reference = input;
        reference.flatten();
        QueryResponse actual, expected;
        Sink::sink_results(input, storage, &actual);
        Sink::sink_results(reference, storage, &expected);
        EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
        google::protobuf::Arena arena;
        auto* arena_response =
            google::protobuf::Arena::CreateMessage<QueryResponse>(&arena);
        Sink::sink_results(input, storage, arena_response);
        EXPECT_EQ(arena_response->SerializeAsString(),
                  expected.SerializeAsString());
      }
    }
  };
  check.template operator()<bool>();
  check.template operator()<int32_t>();
  check.template operator()<uint32_t>();
  check.template operator()<int64_t>();
  check.template operator()<uint64_t>();
  check.template operator()<float>();
  check.template operator()<double>();
  check.template operator()<std::string>();
}

TEST(QueryResultTest, ListChunkSerializationPreservesSlicedNestedValues) {
  const auto list_type = DataType::List(DataType::INT64);
  const auto nested_type = DataType::List(list_type);
  const auto array_type = DataType::Array(DataType::INT64, 2);
  const auto arrays_type = DataType::List(array_type);
  const auto pair =
      Value::LIST(DataType::INT64, {Value::INT64(1), Value(DataType::INT64)});
  const auto full =
      Value::LIST(DataType::INT64, {Value::INT64(7), Value::INT64(8)});
  const auto array =
      Value::ARRAY(array_type, {Value::INT64(1), Value(DataType::INT64)});
  const auto full_array =
      Value::ARRAY(array_type, {Value::INT64(7), Value::INT64(8)});
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  for (const auto& pattern : std::vector<std::vector<Value>>{
           {pair, Value(list_type), Value::LIST(DataType::INT64, {}), full,
            Value::LIST(DataType::INT64, {Value(DataType::INT64)}), full},
           {Value::LIST(list_type, {pair, Value(list_type)}),
            Value(nested_type), Value::LIST(list_type, {}),
            Value::LIST(list_type, {full}),
            Value::LIST(list_type, {Value(list_type)}),
            Value::LIST(list_type, {full})},
           {Value::LIST(array_type, {array}), Value(arrays_type),
            Value::LIST(array_type, {}), Value::LIST(array_type, {full_array}),
            Value::LIST(array_type, {Value(array_type)}),
            Value::LIST(array_type, {full_array})}}) {
    auto builder = ColumnsUtils::create_builder(pattern[0].type());
    ValueColumnBuilder<int64_t> keys;
    for (size_t row = 0; row < pattern.size(); ++row) {
      builder->push_back_elem(pattern[row]);
      keys.push_back_opt(row);
    }
    auto source = builder->finish();
    auto key_column = keys.finish();
    for (const auto& selections : std::vector<std::vector<sel_vec_t>>{
             {{3, 5, 3}, {}, {2, 5}, {3}},
             {{1, 3, 0}, {}, {4, 2, 5, 3}, {0, 1}},
             {{}, {}}}) {
      Context input;
      input.tag_ids = {2, 0, 2};
      for (const auto& selection : selections) {
        ContextChunk chunk;
        chunk.set(2, source->shuffle(selection));
        chunk.set(0, key_column->shuffle(selection));
        input.append_chunk(std::move(chunk));
      }
      Context reference = input;
      reference.flatten();
      QueryResponse actual, expected;
      Sink::sink_results(input, storage, &actual);
      Sink::sink_results(reference, storage, &expected);
      EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
      google::protobuf::Arena arena;
      auto* response =
          google::protobuf::Arena::CreateMessage<QueryResponse>(&arena);
      Sink::sink_results(input, storage, response);
      EXPECT_EQ(response->SerializeAsString(), expected.SerializeAsString());
    }
  }
}

TEST(QueryResultTest, IsLazyAndReleasesCursorOnCancellation) {
  int pulls = 0;
  std::weak_ptr<int> weak;
  {
    auto lifetime = std::make_shared<int>(0);
    weak = lifetime;
    PropertyGraph graph;
    GraphView view(graph);
    StorageReadInterface storage(view, 0);
    auto pipeline = PrependInput(
        Pipeline{}, [&, lifetime]() -> QueryResultReader::NextResult {
          ++pulls;
          return std::optional<ContextChunk>(std::in_place, chunk(pulls));
        });
    auto stream = pipeline.ExecuteReader(storage, {}, {}, nullptr, 1);
    EXPECT_EQ(pulls, 0);
    ASSERT_TRUE(stream.Next());
    EXPECT_EQ(pulls, 1);
  }
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(pulls, 1);
}

TEST(QueryResultTest, ErrorIsTerminalAndIsNotEndOfStream) {
  int pulls = 0;
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  auto pipeline =
      PrependInput(Pipeline{}, [&]() -> QueryResultReader::NextResult {
        ++pulls;
        if (pulls == 1) {
          return std::optional<ContextChunk>(std::in_place, chunk(1));
        }
        THROW_IO_EXCEPTION("late read failure");
      });
  auto stream = pipeline.ExecuteReader(storage, {}, {}, nullptr, 1);
  ASSERT_TRUE(stream.Next());
  auto failed = stream.Next();
  ASSERT_FALSE(failed);
  EXPECT_NE(failed.error().error_message().find("late read failure"),
            std::string::npos);
  EXPECT_FALSE(stream.Next());
  EXPECT_EQ(pulls, 2);
}

TEST(QueryResultTest, PreservesHeadsTagsSparseAliasesAndEmptyBatches) {
  auto first = chunk(7, 3);
  auto head = first.get(3);
  Context original;
  original.tag_ids = {-1, 3};
  original.append_chunk(std::move(first), head);
  ValueColumnBuilder<int64_t> empty_builder;
  DataChunk empty;
  empty.set(3, empty_builder.finish());
  original.append_chunk(std::move(empty));
  original.append_chunk(DataChunk(), head);
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Pipeline pipeline;
  auto restored = pipeline.Execute(storage, std::move(original), {}, nullptr);
  ASSERT_TRUE(restored);
  EXPECT_EQ(restored->tag_ids, (std::vector<int>{-1, 3}));
  ASSERT_EQ(restored->chunk_num(), 3);
  EXPECT_EQ(restored->chunk(0).get(-1), restored->chunk(0).get(3));
  EXPECT_EQ(restored->chunk(1).row_num(), 0);
  EXPECT_EQ(restored->chunk(1).get(3)->elem_type(), DataType::INT64);
  EXPECT_EQ(restored->chunk(2).row_num(), 1);
}

TEST(QueryResultTest, ChunkKernelPreservesColumnIdentity) {
  auto data = chunk(42, 3);
  auto column = data.get(3);
  auto state = make_chunk_kernel(
      [](ContextChunk input) -> result<ContextChunk> { return input; });
  auto output = state->Process(ContextChunk(std::move(data), column));
  ASSERT_TRUE(output);
  ASSERT_EQ(output->size(), 1);
  EXPECT_EQ(output->front().get(3), column);
  EXPECT_EQ(output->front().head(), column);
  EXPECT_TRUE(state->Finalize()->empty());
}

TEST(QueryResultTest, StorageBridgeMapsProvidedBatchesAndStopsAtEnd) {
  auto input = chunk(5, 2);
  input.set(0, chunk(9).get(0));
  ops::BatchChunkSupplier supplier(one_chunk(ContextChunk(std::move(input))),
                                   {{2, "a"}, {0, "b"}});
  EXPECT_EQ(supplier.RowNum(), -1);
  auto first = supplier.GetNextChunk();
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->get(0)->get_elem(0).GetValue<int64_t>(), 5);
  EXPECT_EQ(first->get(1)->get_elem(0).GetValue<int64_t>(), 9);
  EXPECT_EQ(supplier.GetNextChunk(), nullptr);
  EXPECT_EQ(supplier.rows_read(), 1);
}

TEST(QueryResultTest, CopyResultPreservesCardinalityWithoutInputColumns) {
  auto output = ops::batch_insert_result(1000000);
  EXPECT_EQ(output.row_num(), 1000000);
  EXPECT_EQ(output.col_num(), 0);
}

TEST(QueryResultTest, DeepPipelineRunsKernelsIterativelyAndFinalizesOnce) {
  class State final : public OperatorState {
   public:
    State(size_t index, size_t width, size_t& processed, size_t& finalized)
        : index_(index),
          width_(width),
          processed_(processed),
          finalized_(finalized) {}
    KernelResult Process(ContextChunk input) override {
      EXPECT_EQ(processed_++ % width_, index_);
      return one_chunk(std::move(input));
    }
    KernelResult Finalize() override {
      EXPECT_EQ(finalized_++, index_);
      return ChunkBatch{};
    }

   private:
    size_t index_, width_;
    size_t& processed_;
    size_t& finalized_;
  };
  class Forward final : public IOperator {
   public:
    Forward(size_t index, size_t width, size_t& processed, size_t& finalized)
        : index_(index),
          width_(width),
          processed_(processed),
          finalized_(finalized) {}
    std::string get_operator_name() const override { return "Forward"; }
    Kernel CreateState(IStorageInterface&, const ParamsMap&,
                       OprTimer*) override {
      return std::make_unique<State>(index_, width_, processed_, finalized_);
    }

   private:
    size_t index_, width_;
    size_t& processed_;
    size_t& finalized_;
  };
  size_t processed = 0, finalized = 0;
  constexpr size_t width = 4096;
  std::vector<std::unique_ptr<IOperator>> operators;
  for (size_t i = 0; i < width; ++i) {
    operators.push_back(
        std::make_unique<Forward>(i, width, processed, finalized));
  }
  Pipeline pipeline(std::move(operators));
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Context input;
  input.append_chunk(chunk(1));
  input.append_chunk(chunk(2));
  auto output = pipeline.Execute(storage, std::move(input), {}, nullptr);
  ASSERT_TRUE(output);
  EXPECT_EQ(output->row_num(), 2);
  EXPECT_EQ(processed, width * 2);
  EXPECT_EQ(finalized, width);
}

TEST(QueryResultTest, GlobalFinalizeStopsAtDownstreamLimit) {
  class Buffer final : public IOperator {
   public:
    std::string get_operator_name() const override { return "Buffer"; }
    Kernel CreateState(IStorageInterface&, const ParamsMap&,
                       OprTimer*) override {
      return make_batch_kernel(
          [](ChunkBatch input) -> KernelResult { return input; });
    }
  };
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  physical::PhysicalPlan plan;
  auto* range =
      plan.add_plan()->mutable_opr()->mutable_limit()->mutable_range();
  range->set_lower(0);
  range->set_upper(1);
  ops::LimitOprBuilder builder;
  auto built = builder.Build(Schema(), ContextMeta(), plan, 0);
  ASSERT_TRUE(built);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<Buffer>());
  operators.push_back(std::move(built->first));
  Pipeline pipeline(std::move(operators));
  Context input;
  input.append_chunk(chunk(1));
  input.append_chunk(chunk(2));
  OprTimer timer;
  auto output = pipeline.Execute(storage, std::move(input), {}, &timer);
  ASSERT_TRUE(output);
  EXPECT_EQ(output->chunk_num(), 1);
  EXPECT_EQ(output->row_num(), 1);
  EXPECT_EQ(output->chunk(0).get(0)->get_elem(0).GetValue<int64_t>(), 1);
}

struct Counts {
  int produced = 0;
  int consumed = 0;
};
class CountingSource final : public MorselSourceOperator {
 public:
  explicit CountingSource(Counts& counts) : counts_(counts) {}
  std::string get_operator_name() const override { return "CountingSource"; }
  std::unique_ptr<MorselSource> CreateMorselSource(IStorageInterface&,
                                                   const ParamsMap&) override {
    return std::make_unique<ChunkMorselSource>(
        [this]() -> result<std::optional<ContextChunk>> {
          EXPECT_EQ(counts_.produced, counts_.consumed);
          if (counts_.produced == 3) {
            return std::optional<ContextChunk>{};
          }
          return std::optional<ContextChunk>(std::in_place,
                                             chunk(++counts_.produced));
        });
  }

 private:
  Counts& counts_;
};
class CountingProject final : public IOperator {
 public:
  explicit CountingProject(Counts& counts) : counts_(counts) {}
  std::string get_operator_name() const override { return "CountingProject"; }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) override {
    return make_chunk_kernel(
        [this](ContextChunk&& chunk) -> result<ContextChunk> {
          ++counts_.consumed;
          EXPECT_EQ(chunk.row_num(), 1);
          return std::move(chunk);
        });
  }

 private:
  Counts& counts_;
};

TEST(QueryResultTest, GlobalKernelFinalizesAfterAllInput) {
  int calls = 0;
  auto state =
      make_global_kernel([&](ContextChunk input) -> result<ContextChunk> {
        ++calls;
        EXPECT_EQ(input.row_num(), 2);
        return input;
      });
  EXPECT_TRUE(state->Process(ContextChunk(chunk(1)))->empty());
  EXPECT_TRUE(state->Process(ContextChunk(chunk(2)))->empty());
  EXPECT_EQ(calls, 0);
  auto output = state->Finalize();
  ASSERT_TRUE(output);
  ASSERT_EQ(output->size(), 1);
  EXPECT_EQ(output->front().row_num(), 2);
  EXPECT_EQ(calls, 1);
}

TEST(QueryResultTest, SinkMetadataPreservesOutputOrderAndEmptyResults) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  physical::PhysicalPlan plan;
  auto* sink = plan.add_plan()->mutable_opr()->mutable_sink();
  for (int alias : {3, 0, 3}) {
    sink->add_tags()->mutable_tag()->set_value(alias);
  }
  ops::SinkOprBuilder builder;
  auto built = builder.Build(Schema(), ContextMeta(), plan, 0);
  ASSERT_TRUE(built);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::move(built->first));
  Pipeline pipeline(std::move(operators));
  for (bool empty : {false, true}) {
    Context input;
    if (!empty) {
      auto data = chunk(7, 3);
      data.set(0, chunk(9).get(0));
      input.append_chunk(std::move(data));
    }
    auto result = pipeline.Execute(storage, std::move(input), {}, nullptr);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->tag_ids, (std::vector<int>{3, 0, 3}));
    EXPECT_EQ(result->row_num(), empty ? 0 : 1);
  }
}

TEST(QueryResultTest, PipelineReportsInitializationFailureOnlyWhenPulled) {
  class FailingSource final : public IOperator {
   public:
    explicit FailingSource(int& calls) : calls_(calls) {}
    std::string get_operator_name() const override { return "FailingSource"; }
    Kernel CreateState(IStorageInterface&, const ParamsMap&,
                       OprTimer*) override {
      return make_once_kernel([this]() -> KernelResult {
        ++calls_;
        THROW_IO_EXCEPTION("source initialization failed");
      });
    }

   private:
    int& calls_;
  };
  int calls = 0;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<FailingSource>(calls));
  Pipeline pipeline(std::move(operators));
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  auto output = pipeline.ExecuteReader(storage, Context{}, {}, nullptr);
  EXPECT_EQ(calls, 0);
  auto error = output.Next();
  ASSERT_FALSE(error);
  EXPECT_NE(error.error().ToString().find("FailingSource"), std::string::npos);
  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(output.Next());
  EXPECT_EQ(calls, 1);
}

TEST(QueryResultTest, PipelinePullsThroughChunkwiseOperatorsAndProfilesRows) {
  Counts counts;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<CountingSource>(counts));
  operators.push_back(std::make_unique<CountingProject>(counts));
  Pipeline pipeline(std::move(operators));
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  OprTimer timer;
  auto result = pipeline.Execute(storage, Context(), {}, &timer);
  ASSERT_TRUE(result) << result.error().ToString();
  EXPECT_EQ(result->row_num(), 3);
  EXPECT_EQ(counts.produced, 3);
  EXPECT_EQ(counts.consumed, 3);
  auto profile = OprTimer::ToProfileResult(&timer);
  ASSERT_EQ(profile.operators_size(), 2);
  EXPECT_EQ(profile.operators(0).output_rows(), 3);
  EXPECT_EQ(profile.operators(1).output_rows(), 3);
}
}  // namespace
}  // namespace neug::execution
