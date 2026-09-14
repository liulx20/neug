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
#include "query_test_utils.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>

#include <future>
#include "neug/common/columns/value_columns.h"
#include "neug/execution/execute/ops/retrieve/join.h"
#include "neug/execution/execute/ops/retrieve/limit.h"
#include "neug/execution/execute/ops/retrieve/sink.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/storages/graph/property_graph.h"

namespace neug::execution {
namespace {
ContextChunk MakeChunk(int64_t value) {
  ValueColumnBuilder<int64_t> builder;
  builder.push_back_opt(value);
  ContextChunk chunk;
  chunk.set(0, builder.finish());
  return chunk;
}

TEST(TaskSchedulerTest, ScheduledPullIsLazyOrderedAndStopsOnDestruction) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Pipeline pipeline;
  std::atomic<int> calls{0};
  const auto caller = std::this_thread::get_id();
  {
    auto sourced = PrependInput(
        std::move(pipeline), [&]() -> QueryResultReader::NextResult {
          EXPECT_NE(caller, std::this_thread::get_id());
          return std::optional<ContextChunk>(MakeChunk(++calls));
        });
    auto stream = sourced.ExecuteReader(storage, Context{}, {}, nullptr, 2);
    EXPECT_EQ(calls.load(), 0);

    for (int64_t i = 1; i <= 3; ++i) {
      auto next = stream.Next();
      ASSERT_TRUE(next);
      ASSERT_TRUE(*next);
      EXPECT_EQ((**next).get(0)->get_elem(0).GetValue<int64_t>(), i);
      EXPECT_GE(calls.load(), i);
      EXPECT_LE(calls.load(), i + 2);
    }
  }
  EXPECT_LE(calls.load(), 5);
}

TEST(TaskSchedulerTest, ErrorsAreTerminalAndQueueCanBeDestroyed) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Pipeline pipeline;
  int calls = 0;
  auto sourced =
      PrependInput(std::move(pipeline), [&]() -> QueryResultReader::NextResult {
        ++calls;
        THROW_IO_EXCEPTION("scheduled source error");
      });
  auto stream = sourced.ExecuteReader(storage, Context{}, {}, nullptr, 2);
  auto first = stream.Next();
  ASSERT_FALSE(first);
  EXPECT_NE(first.error().ToString().find("scheduled source error"),
            std::string::npos);
  EXPECT_FALSE(stream.Next());
  EXPECT_EQ(calls, 1);
  auto invalid = sourced.ExecuteReader(storage, {}, {}, nullptr, 0);
  EXPECT_FALSE(invalid.Next());
}

class CallbackSource final : public IOperator {
 public:
  explicit CallbackSource(std::function<result<ContextChunk>()> produce,
                          int* initialized = nullptr)
      : produce_(std::move(produce)), initialized_(initialized) {}
  bool consumes_input() const override { return false; }
  std::string get_operator_name() const override { return "CallbackSource"; }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) override {
    if (initialized_) {
      ++*initialized_;
    }
    return make_source_kernel(produce_);
  }

 private:
  std::function<result<ContextChunk>()> produce_;
  int* initialized_;
};

Pipeline OneOperator(std::unique_ptr<IOperator> op) {
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::move(op));
  return Pipeline(std::move(operators));
}

class TestFork final : public IOperator {
 public:
  TestFork(SubPipelineMode mode, Pipeline left, Pipeline right)
      : mode_(mode), left_(std::move(left)), right_(std::move(right)) {}
  std::string get_operator_name() const override { return "TestFork"; }
  SubPipelines sub_pipelines() override { return {mode_, {&left_, &right_}}; }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) override {
    return make_chunk_kernel(
        [](ContextChunk chunk) -> result<ContextChunk> { return chunk; });
  }

 private:
  SubPipelineMode mode_;
  Pipeline left_;
  Pipeline right_;
};

TEST(TaskSchedulerTest, BuilderSplitsForkWithoutOperatorScheduling) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  std::mutex mutex;
  std::condition_variable ready;
  int started = 0;
  int input_pulls = 0;
  auto produce = [&]() -> result<ContextChunk> {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_EQ(input_pulls,
              4);  // three chunks and EOF before either branch starts
    ++started;
    ready.notify_all();
    EXPECT_TRUE(ready.wait_for(lock, std::chrono::seconds(5),
                               [&] { return started == 2; }));
    return MakeChunk(7);
  };
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kMaterialized,
      OneOperator(std::make_unique<CallbackSource>(produce)),
      OneOperator(std::make_unique<CallbackSource>(produce))));
  auto sourced =
      PrependInput(std::move(pipeline), [&]() -> QueryResultReader::NextResult {
        if (++input_pulls == 4) {
          return std::optional<ContextChunk>{};
        }
        return std::optional<ContextChunk>(MakeChunk(input_pulls));
      });
  auto output = sourced.ExecuteReader(storage, Context{}, {}, nullptr, 2);
  EXPECT_EQ(input_pulls, 0);
  auto result = collect_chunk(std::move(output));
  ASSERT_TRUE(result);
  EXPECT_EQ(started, 2);
  EXPECT_EQ(input_pulls, 4);
  EXPECT_EQ(result->get(0)->get_elem(0).GetValue<int64_t>(), 7);
}

TEST(TaskSchedulerTest, WritableExecutionUsesOneWorkerAndPreservesTaskOrder) {
  class WritableStorage final : public StorageReadInterface {
   public:
    explicit WritableStorage(GraphView& view) : StorageReadInterface(view, 0) {}
    bool writable() const override { return true; }
  };
  PropertyGraph graph;
  GraphView view(graph);
  WritableStorage storage(view);
  std::mutex mutex;
  std::vector<int> order;
  std::vector<std::thread::id> threads;
  auto record = [&](int step) {
    std::lock_guard<std::mutex> lock(mutex);
    order.push_back(step);
    threads.push_back(std::this_thread::get_id());
    return MakeChunk(step);
  };
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kMaterialized,
      OneOperator(std::make_unique<CallbackSource>([&] { return record(1); })),
      OneOperator(
          std::make_unique<CallbackSource>([&] { return record(2); }))));
  auto sourced = PrependInput(
      std::move(pipeline),
      [&, done = false]() mutable -> QueryResultReader::NextResult {
        if (done) {
          return std::optional<ContextChunk>{};
        }
        done = true;
        return std::optional<ContextChunk>(record(0));
      });
  auto stream = sourced.ExecuteReader(storage, Context{}, {}, nullptr, 4);
  EXPECT_TRUE(order.empty());
  auto result = collect_chunk(std::move(stream));
  ASSERT_TRUE(result) << result.error().ToString();
  EXPECT_EQ(order, (std::vector<int>{0, 1, 2}));
  ASSERT_EQ(threads.size(), 3);
  EXPECT_NE(threads[0], std::this_thread::get_id());
  EXPECT_EQ(threads[0], threads[1]);
  EXPECT_EQ(threads[1], threads[2]);
}

TEST(TaskSchedulerTest, SequentialGroupDoesNotInitializeUnusedBranch) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  int left_init = 0;
  int right_init = 0;
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kSequential,
      OneOperator(std::make_unique<CallbackSource>([] { return MakeChunk(1); },
                                                   &left_init)),
      OneOperator(std::make_unique<CallbackSource>(
          []() -> result<ContextChunk> {
            ADD_FAILURE() << "An unconsumed sequential branch must not execute";
            return tl::unexpected(Status::InternalError("unused"));
          },
          &right_init))));
  {
    auto output = pipeline.ExecuteReader(storage, {}, {}, nullptr, 2);
    EXPECT_EQ(left_init, 0);
    EXPECT_EQ(right_init, 0);
    auto next = output.Next();
    ASSERT_TRUE(next);
    ASSERT_TRUE(*next);
    EXPECT_EQ(left_init, 1);
    EXPECT_EQ(right_init, 0);
  }
  EXPECT_EQ(right_init, 0);
}

TEST(TaskSchedulerTest, FailedCommonInputPreventsBranchesFromRunning) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  auto produce = []() -> result<ContextChunk> {
    ADD_FAILURE() << "Failed common input must block both branches";
    return MakeChunk(1);
  };
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kMaterialized,
      OneOperator(std::make_unique<CallbackSource>(produce)),
      OneOperator(std::make_unique<CallbackSource>(produce))));
  auto sourced =
      PrependInput(std::move(pipeline), []() -> QueryResultReader::NextResult {
        return tl::unexpected(Status::InternalError("bad common input"));
      });
  auto output = sourced.ExecuteReader(storage, {}, {}, nullptr, 2);
  auto next = output.Next();
  ASSERT_FALSE(next);
  EXPECT_NE(next.error().ToString().find("bad common input"),
            std::string::npos);
}

TEST(TaskSchedulerTest, ReplacementSourcePrunesUnusedTasks) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  auto unused = []() -> result<ContextChunk> {
    ADD_FAILURE() << "Replaced upstream data flow must not execute";
    return tl::unexpected(Status::InternalError("unused input"));
  };
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<TestFork>(
      SubPipelineMode::kMaterialized,
      OneOperator(std::make_unique<CallbackSource>(unused)),
      OneOperator(std::make_unique<CallbackSource>(unused))));
  operators.push_back(
      std::make_unique<CallbackSource>([] { return MakeChunk(9); }));
  Pipeline pipeline(std::move(operators));
  auto output =
      collect_chunk(pipeline.ExecuteReader(storage, {}, {}, nullptr, 2));
  ASSERT_TRUE(output);
  EXPECT_EQ(output->get(0)->get_elem(0).GetValue<int64_t>(), 9);
}

void AddJoin(physical::PhysicalPlan& plan, int depth) {
  auto* join = plan.add_plan()->mutable_opr()->mutable_join();
  join->set_join_kind(physical::Join_JoinKind_INNER);
  join->add_left_keys()->mutable_tag()->set_id(0);
  join->add_right_keys()->mutable_tag()->set_id(0);
  if (depth > 1) {
    AddJoin(*join->mutable_left_plan(), depth - 1);
    AddJoin(*join->mutable_right_plan(), depth - 1);
  }
}

TEST(TaskSchedulerTest, JoinInsideSequentialGroupUsesSameGraphWithoutPoolWait) {
  PlanParser::get().init();
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  physical::PhysicalPlan plan;
  AddJoin(plan, 2);
  ContextMeta meta;
  meta.set(0, DataType::INT64);
  ops::JoinOprBuilder builder;
  auto built = builder.Build(graph.schema(), meta, plan, 0);
  ASSERT_TRUE(built);
  int unused = 0;
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kSequential, OneOperator(std::move(built->first)),
      OneOperator(std::make_unique<CallbackSource>(
          []() -> result<ContextChunk> {
            return tl::unexpected(Status::InternalError("unused branch"));
          },
          &unused))));
  std::vector<ContextChunk> chunks;
  chunks.push_back(MakeChunk(1));
  chunks.push_back(MakeChunk(1));
  auto output = pipeline.ExecuteReader(
      storage, context_from_batches(std::move(chunks)), {}, nullptr, 1);
  for (int batch = 0; batch < 2; ++batch) {
    auto next = output.Next();
    ASSERT_TRUE(next) << next.error().ToString();
    ASSERT_TRUE(*next);
    EXPECT_EQ((**next).row_num(), 8);
    EXPECT_EQ(unused, 0);
  }
  // Destroy before the second branch is demanded.
}

TEST(TaskSchedulerTest, JoinStateReceivesBuildDataAndReusesPublishedTable) {
  PlanParser::get().init();
  PropertyGraph graph;
  physical::PhysicalPlan plan;
  AddJoin(plan, 1);
  ContextMeta meta;
  meta.set(0, DataType::INT64);
  ops::JoinOprBuilder builder;
  auto built = builder.Build(graph.schema(), meta, plan, 0);
  ASSERT_TRUE(built);
  auto state = built->first->CreateBuildState(2);
  auto empty = MakeChunk(0);
  empty.reshuffle({});
  auto metadata = state->PartitionBuild(empty);
  ASSERT_TRUE(metadata);
  EXPECT_FALSE(state->PartitionBuild(empty));
  for (size_t i = 0; i < state->BuildPartitions(); ++i) {
    ASSERT_TRUE(state->BuildPartition(i, *metadata));
  }
  auto input = MakeChunk(1).union_with(MakeChunk(1));
  auto batch = state->PartitionBuild(std::move(input));
  ASSERT_TRUE(batch);
  ASSERT_FALSE(state->FinalizeBuild());
  for (size_t i = 0; i < state->BuildPartitions(); ++i) {
    ASSERT_TRUE(state->BuildPartition(i, *batch));
  }
  ASSERT_TRUE(state->FinalizeBuild());
  for (int i = 0; i < 3; ++i) {
    auto output = state->ProbeChunk(MakeChunk(1));
    ASSERT_TRUE(output);
    EXPECT_EQ(output->row_num(), 2);
  }
}

TEST(TaskSchedulerTest, RealNestedJoinReplaysMultipleChunksAndProfiles) {
  PlanParser::get().init();
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  physical::PhysicalPlan plan;
  AddJoin(plan, 2);
  ContextMeta meta;
  meta.set(0, DataType::INT64);
  ops::JoinOprBuilder builder;
  auto built = builder.Build(graph.schema(), meta, plan, 0);
  ASSERT_TRUE(built);
  ASSERT_TRUE(built->first);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::move(built->first));
  Pipeline pipeline(std::move(operators));
  auto input = [] {
    std::vector<ContextChunk> chunks;
    chunks.push_back(MakeChunk(1));
    chunks.push_back(MakeChunk(2));
    chunks.push_back(MakeChunk(1));
    return context_from_batches(std::move(chunks));
  };
  auto expected =
      collect_chunk(pipeline.ExecuteReader(storage, input(), {}, nullptr));
  ASSERT_TRUE(expected);
  EXPECT_EQ(expected->row_num(), 17);
  for (size_t workers : {1, 2, 4}) {
    OprTimer timer;
    auto stream = pipeline.ExecuteReader(storage, input(), {}, &timer, workers);
    auto chunks = collect_batches(std::move(stream));
    ASSERT_TRUE(chunks) << chunks.error().ToString();
    ASSERT_EQ(chunks->size(), 3);
    EXPECT_EQ((*chunks)[0].row_num(), 8);
    EXPECT_EQ((*chunks)[1].row_num(), 1);
    EXPECT_EQ((*chunks)[2].row_num(), 8);
    auto actual = collect_chunk(context_from_batches(std::move(*chunks)));
    ASSERT_TRUE(actual) << actual.error().ToString();
    ASSERT_EQ(actual->row_num(), expected->row_num());
    for (size_t row = 0; row < actual->row_num(); ++row) {
      EXPECT_EQ(actual->get(0)->get_elem(row), expected->get(0)->get_elem(row));
    }
    auto profile = OprTimer::ToProfileResult(&timer);
    EXPECT_GE(profile.operators_size(), 3);
    EXPECT_EQ(profile.operators(0).output_rows(), 17);
  }
  auto execute = [&](int64_t value) {
    std::vector<ContextChunk> chunks;
    chunks.push_back(MakeChunk(value));
    auto output = pipeline.ExecuteReader(
        storage, context_from_batches(std::move(chunks)), {}, nullptr, 2);
    return std::async(std::launch::async,
                      [stream = std::move(output)]() mutable {
                        return collect_chunk(std::move(stream));
                      });
  };
  auto first = execute(11);
  auto second = execute(22);
  auto first_result = first.get();
  auto second_result = second.get();
  ASSERT_TRUE(first_result);
  ASSERT_TRUE(second_result);
  EXPECT_EQ(first_result->get(0)->get_elem(0).GetValue<int64_t>(), 11);
  EXPECT_EQ(second_result->get(0)->get_elem(0).GetValue<int64_t>(), 22);
}

TEST(TaskSchedulerTest, ScheduledSinkPreservesEmptyOutputSchema) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  physical::PhysicalPlan plan;
  auto* sink = plan.add_plan()->mutable_opr()->mutable_sink();
  for (int alias : {3, 0, 3}) {
    sink->add_tags()->mutable_tag()->set_value(alias);
  }
  ops::SinkOprBuilder builder;
  auto built = builder.Build(graph.schema(), ContextMeta(), plan, 0);
  ASSERT_TRUE(built);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::move(built->first));
  Pipeline pipeline(std::move(operators));
  auto output =
      materialize(pipeline.ExecuteReader(storage, {}, {}, nullptr, 2));
  ASSERT_TRUE(output);
  EXPECT_EQ(output->row_num(), 0);
  EXPECT_EQ(output->tag_ids, (std::vector<int>{3, 0, 3}));
}

TEST(TaskSchedulerTest, PausedConsumerDoesNotRefillCompletedSlots) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  for (size_t workers : {1, 4}) {
    std::atomic<int> read{0};
    std::mutex mutex;
    std::condition_variable ready;
    auto pipeline =
        PrependInput(Pipeline{}, [&]() -> QueryResultReader::NextResult {
          std::lock_guard<std::mutex> lock(mutex);
          auto value = ++read;
          ready.notify_all();
          return std::optional<ContextChunk>(MakeChunk(value));
        });
    {
      auto reader = pipeline.ExecuteReader(storage, {}, {}, nullptr, workers);
      EXPECT_EQ(read, 0);
      auto first = reader.Next();
      ASSERT_TRUE(first);
      ASSERT_TRUE(*first);
      const int paused = workers == 1 ? 1 : workers + 1;
      {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(ready.wait_for(lock, std::chrono::seconds(1),
                                   [&] { return read == paused; }));
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      EXPECT_EQ(read, paused);
    }
    auto cancelled = read.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(read, cancelled);
  }
}

TEST(TaskSchedulerTest, FailureWaitsForOtherSubmittedBranch) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  std::mutex mutex;
  std::condition_variable ready;
  size_t started = 0;
  bool release = false, completed = false;
  auto source = [&](bool fail) -> result<ContextChunk> {
    std::unique_lock<std::mutex> lock(mutex);
    ++started;
    ready.notify_all();
    if (!ready.wait_for(lock, std::chrono::seconds(5),
                        [&] { return started == 2; })) {
      return tl::unexpected(Status::InternalError("branch start timeout"));
    }
    if (fail) {
      return tl::unexpected(Status::InternalError("branch failed"));
    }
    ready.wait(lock, [&] { return release; });
    completed = true;
    return MakeChunk(7);
  };
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kMaterialized,
      OneOperator(
          std::make_unique<CallbackSource>([&] { return source(true); })),
      OneOperator(
          std::make_unique<CallbackSource>([&] { return source(false); }))));
  auto reader = pipeline.ExecuteReader(storage, {}, {}, nullptr, 2);
  auto result = std::async(
      std::launch::async,
      [reader = std::move(reader)]() mutable { return reader.Next(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(ready.wait_for(lock, std::chrono::seconds(5),
                               [&] { return started == 2; }));
  }
  EXPECT_EQ(result.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    ready.notify_all();
  }
  auto failure = result.get();
  EXPECT_FALSE(failure);
  EXPECT_TRUE(completed);
  EXPECT_NE(failure.error().ToString().find("branch failed"),
            std::string::npos);
}

TEST(TaskSchedulerTest, UnusedUnionJoinDoesNotCreateBuildState) {
  class UnusedJoin final : public BuildProbeOperator {
   public:
    explicit UnusedJoin(int& calls) : calls_(calls) {}
    std::string get_operator_name() const override { return "UnusedJoin"; }
    SubPipelines sub_pipelines() override {
      return {SubPipelineMode::kBuildProbe, {&left_, &right_}};
    }
    std::shared_ptr<BuildProbeState> CreateBuildState(size_t) override {
      ++calls_;
      THROW_IO_EXCEPTION("unused Join initialized");
    }

   private:
    int& calls_;
    Pipeline left_, right_;
  };
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  int calls = 0;
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kSequential,
      OneOperator(
          std::make_unique<CallbackSource>([] { return MakeChunk(1); })),
      OneOperator(std::make_unique<UnusedJoin>(calls))));
  for (size_t workers : {1, 4}) {
    auto reader = pipeline.ExecuteReader(storage, {}, {}, nullptr, workers);
    EXPECT_EQ(calls, 0);
    auto first = reader.Next();
    ASSERT_TRUE(first);
    ASSERT_TRUE(*first);
    EXPECT_EQ(calls, 0);
  }
}

TEST(TaskSchedulerTest, LimitDoesNotDemandNextUnionBranch) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  int unused = 0;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<TestFork>(
      SubPipelineMode::kSequential,
      OneOperator(
          std::make_unique<CallbackSource>([] { return MakeChunk(1); })),
      OneOperator(std::make_unique<CallbackSource>([] { return MakeChunk(2); },
                                                   &unused))));
  physical::PhysicalPlan plan;
  auto* range =
      plan.add_plan()->mutable_opr()->mutable_limit()->mutable_range();
  range->set_lower(0);
  range->set_upper(1);
  ops::LimitOprBuilder builder;
  auto limit = builder.Build(Schema(), ContextMeta(), plan, 0);
  ASSERT_TRUE(limit);
  operators.push_back(std::move(limit->first));
  Pipeline pipeline(std::move(operators));
  for (size_t workers : {1, 4}) {
    auto result =
        materialize(pipeline.ExecuteReader(storage, {}, {}, nullptr, workers));
    ASSERT_TRUE(result);
    EXPECT_EQ(result->row_num(), 1);
    EXPECT_EQ(unused, 0);
  }
}
struct IncrementalBuildObservation {
  std::mutex mutex;
  std::condition_variable ready;
  bool release = false;
  bool blocked = false;
  bool second_partitioned = false;
  bool parallel_finalize = false;
  bool release_finalizers = false;
  int finalize_failure = 0;
  size_t finalizers_entered = 0;
  size_t finalizers_exited = 0;
  bool fail = false;
  bool finalized = false;
  bool probed = false;
  std::atomic<int> reads{0};
  std::vector<int> built[2];
};

class ObservedBuildState final : public BuildProbeState {
 public:
  explicit ObservedBuildState(IncrementalBuildObservation& observation)
      : observation_(observation) {}
  struct Input final : Batch {
    int value;
  };
  std::shared_ptr<Batch> PartitionBuild(ContextChunk chunk) const override {
    auto input = std::make_shared<Input>();
    input->value = chunk.get(0)->get_elem(0).GetValue<int64_t>();
    if (input->value == 2) {
      return nullptr;
    }
    std::unique_lock<std::mutex> lock(observation_.mutex);
    if (input->value == 0) {
      // The second partition task must run while the first is still active.
      EXPECT_TRUE(observation_.ready.wait_for(
          lock, std::chrono::seconds(5),
          [&] { return observation_.second_partitioned; }));
    } else if (input->value == 1) {
      observation_.second_partitioned = true;
      observation_.ready.notify_all();
    }
    return input;
  }
  size_t BuildPartitions() const override { return 2; }
  Status BuildPartition(size_t part, const Batch& batch) override {
    int value = static_cast<const Input&>(batch).value;
    std::unique_lock<std::mutex> lock(observation_.mutex);
    if (part == 0 && value == 0) {
      observation_.blocked = true;
      observation_.ready.notify_all();
      if (!observation_.ready.wait_for(lock, std::chrono::seconds(5),
                                       [&] { return observation_.release; })) {
        return Status::InternalError("build release timeout");
      }
    }
    observation_.built[part].push_back(value);
    observation_.ready.notify_all();
    if (part == 1 && value == 0 && observation_.fail) {
      return Status::InternalError("incremental build failed");
    }
    return Status::OK();
  }
  size_t FinalizePartitions() const override {
    return observation_.parallel_finalize ? 2 : 0;
  }
  Status FinalizePartition(size_t part) override {
    std::unique_lock<std::mutex> lock(observation_.mutex);
    EXPECT_EQ(observation_.built[0].size(), 19);
    EXPECT_EQ(observation_.built[1].size(), 19);
    ++observation_.finalizers_entered;
    observation_.ready.notify_all();
    if (part == 0) {
      EXPECT_TRUE(observation_.ready.wait_for(
          lock, std::chrono::seconds(5),
          [&] { return observation_.release_finalizers; }));
    }
    ++observation_.finalizers_exited;
    observation_.ready.notify_all();
    if (part == 1 && observation_.finalize_failure == 1) {
      return Status::InternalError("finalizer failed");
    }
    if (part == 1 && observation_.finalize_failure == 2) {
      throw std::runtime_error("finalizer threw");
    }
    return Status::OK();
  }
  Status FinalizeBuild() override {
    if (observation_.parallel_finalize) {
      EXPECT_EQ(observation_.finalizers_exited, 2);
    }
    observation_.finalized = true;
    EXPECT_EQ(observation_.built[0].size(), 19);
    EXPECT_EQ(observation_.built[1].size(), 19);
    return Status::OK();
  }
  result<ContextChunk> ProbeChunk(ContextChunk chunk) const override {
    EXPECT_TRUE(observation_.finalized);
    observation_.probed = true;
    return chunk;
  }

 private:
  IncrementalBuildObservation& observation_;
};

class ObservedBuildJoin final : public BuildProbeOperator {
 public:
  explicit ObservedBuildJoin(IncrementalBuildObservation& observation)
      : observation_(observation),
        left_(OneOperator(
            std::make_unique<CallbackSource>([] { return MakeChunk(1); }))),
        right_(PrependInput(
            Pipeline{}, [&observation]() -> QueryResultReader::NextResult {
              auto index = observation.reads++;
              if (index == 20) {
                return std::optional<ContextChunk>{};
              }
              return std::optional<ContextChunk>{MakeChunk(index)};
            })) {}
  std::string get_operator_name() const override { return "ObservedBuildJoin"; }
  SubPipelines sub_pipelines() override {
    return {SubPipelineMode::kBuildProbe, {&left_, &right_}};
  }
  std::shared_ptr<BuildProbeState> CreateBuildState(size_t) override {
    return std::make_shared<ObservedBuildState>(observation_);
  }

 private:
  IncrementalBuildObservation& observation_;
  Pipeline left_, right_;
};

TEST(TaskSchedulerTest, IncrementalBuildBoundsPendingInputAndDrainsFailure) {
  for (bool fail : {false, true}) {
    PropertyGraph graph;
    GraphView view(graph);
    StorageReadInterface storage(view, 0);
    IncrementalBuildObservation observation;
    observation.fail = fail;
    auto pipeline =
        OneOperator(std::make_unique<ObservedBuildJoin>(observation));
    auto reader = pipeline.ExecuteReader(storage, {}, {}, nullptr, 2);
    auto output = std::async(std::launch::async, [&] { return reader.Next(); });
    {
      std::unique_lock<std::mutex> lock(observation.mutex);
      EXPECT_TRUE(
          observation.ready.wait_for(lock, std::chrono::seconds(5), [&] {
            return observation.blocked && !observation.built[1].empty();
          }));
    }
    EXPECT_EQ(output.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    // Two build slots plus bounded upstream read-ahead, not all 20 chunks.
    EXPECT_LE(observation.reads.load(), 6);
    {
      std::lock_guard<std::mutex> lock(observation.mutex);
      observation.release = true;
      observation.ready.notify_all();
    }
    auto result = output.get();
    if (fail) {
      EXPECT_FALSE(result);
      EXPECT_FALSE(observation.finalized);
      EXPECT_FALSE(observation.probed);
      EXPECT_FALSE(reader.Next());
    } else {
      ASSERT_TRUE(result);
      EXPECT_TRUE(observation.finalized);
      EXPECT_TRUE(observation.probed);
      for (const auto& rows : observation.built) {
        ASSERT_EQ(rows.size(), 19);
        for (size_t i = 0; i < rows.size(); ++i) {
          EXPECT_EQ(rows[i], i < 2 ? i : i + 1);
        }
      }
    }
  }
}
TEST(TaskSchedulerTest, ParallelFinalizersWaitForBuildAndDrainErrors) {
  for (int failure : {0, 1, 2}) {
    PropertyGraph graph;
    GraphView view(graph);
    StorageReadInterface storage(view, 0);
    IncrementalBuildObservation observation;
    observation.release = true;
    observation.parallel_finalize = true;
    observation.finalize_failure = failure;
    auto pipeline =
        OneOperator(std::make_unique<ObservedBuildJoin>(observation));
    auto reader = pipeline.ExecuteReader(storage, {}, {}, nullptr, 2);
    auto output = std::async(std::launch::async, [&] { return reader.Next(); });
    {
      std::unique_lock<std::mutex> lock(observation.mutex);
      EXPECT_TRUE(
          observation.ready.wait_for(lock, std::chrono::seconds(5), [&] {
            return observation.finalizers_entered == 2 &&
                   observation.finalizers_exited == 1;
          }));
      EXPECT_FALSE(observation.finalized);
      EXPECT_FALSE(observation.probed);
    }
    EXPECT_EQ(output.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    {
      std::lock_guard<std::mutex> lock(observation.mutex);
      observation.release_finalizers = true;
      observation.ready.notify_all();
    }
    auto result = output.get();
    EXPECT_EQ(observation.finalizers_exited, 2);
    EXPECT_EQ(bool(result), failure == 0);
    EXPECT_EQ(observation.finalized, failure == 0);
    EXPECT_EQ(observation.probed, failure == 0);
    if (failure) {
      EXPECT_FALSE(reader.Next());
    }
  }
}

ContextChunk RangeChunk(int64_t begin, int64_t end) {
  ValueColumnBuilder<int64_t> values;
  for (auto i = begin; i < end; ++i) {
    values.push_back_opt(i);
  }
  ContextChunk chunk;
  chunk.set(0, values.finish());
  return chunk;
}

struct RangeObservation {
  std::mutex mutex;
  std::condition_variable ready;
  size_t entered = 0;
  std::set<std::thread::id> threads;
};

class ObservedRangeMap final : public IOperator {
 public:
  ObservedRangeMap(RangeObservation& observation, int64_t bias)
      : observation_(observation), bias_(bias) {}
  std::string get_operator_name() const override { return "ObservedRangeMap"; }
  PipelineBehavior pipeline_behavior() const override {
    return PipelineBehavior::kChunkLocal;
  }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) override {
    return make_chunk_kernel(
        [this](ContextChunk chunk) -> result<ContextChunk> {
          {
            std::unique_lock<std::mutex> lock(observation_.mutex);
            observation_.threads.insert(std::this_thread::get_id());
            ++observation_.entered;
            observation_.ready.notify_all();
            // The first worker cannot finish until a second range is executing.
            EXPECT_TRUE(observation_.ready.wait_for(
                lock, std::chrono::seconds(5),
                [&] { return observation_.entered >= 2; }));
          }
          ValueColumnBuilder<int64_t> values;
          for (size_t row = 0; row < chunk.row_num(); ++row) {
            values.push_back_opt(
                chunk.get(0)->get_elem(row).GetValue<int64_t>() + bias_);
          }
          chunk.remove(0);
          chunk.set(0, values.finish());
          return chunk;
        });
  }

 private:
  RangeObservation& observation_;
  int64_t bias_;
};

class GlobalRangeBarrier final : public IOperator {
 public:
  explicit GlobalRangeBarrier(bool collect = true) : collect_(collect) {}
  std::string get_operator_name() const override {
    return "GlobalRangeBarrier";
  }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) override {
    auto identity = [](ContextChunk chunk) -> result<ContextChunk> {
      return chunk;
    };
    return collect_ ? make_global_kernel(identity)
                    : make_chunk_kernel(identity);
  }

 private:
  bool collect_;
};

TEST(TaskSchedulerTest, MaterializedGlobalOutputRunsOrderedParallelRanges) {
  for (bool collect : {false, true}) {
    PropertyGraph graph;
    GraphView view(graph);
    StorageReadInterface storage(view, 0);
    RangeObservation observation;
    std::vector<std::unique_ptr<IOperator>> operators;
    operators.push_back(std::make_unique<GlobalRangeBarrier>(collect));
    operators.push_back(std::make_unique<ObservedRangeMap>(observation, 7));
    Pipeline pipeline(std::move(operators));
    ChunkBatch input;
    input.push_back(RangeChunk(0, 8192));
    input.push_back(RangeChunk(0, 0));
    input.push_back(RangeChunk(8192, 16384));
    auto result = collect_chunk(pipeline.ExecuteReader(
        storage, context_from_batches(std::move(input)), {}, nullptr, 2));
    ASSERT_TRUE(result);
    ASSERT_EQ(result->row_num(), 16384);
    EXPECT_GE(observation.threads.size(), 2);
    for (size_t row = 0; row < result->row_num(); ++row) {
      EXPECT_EQ(result->get(0)->get_elem(row).GetValue<int64_t>(), row + 7);
    }
  }
}

TEST(TaskSchedulerTest, SharedReplayBranchesOwnParallelCursors) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  RangeObservation left, right;
  auto pipeline = OneOperator(std::make_unique<TestFork>(
      SubPipelineMode::kSequential,
      OneOperator(std::make_unique<ObservedRangeMap>(left, 1)),
      OneOperator(std::make_unique<ObservedRangeMap>(right, 10000))));
  ChunkBatch input;
  for (int i = 0; i < 16; ++i) {
    input.push_back(RangeChunk(i * 128, (i + 1) * 128));
  }
  auto result = collect_chunk(pipeline.ExecuteReader(
      storage, context_from_batches(std::move(input)), {}, nullptr, 2));
  ASSERT_TRUE(result);
  ASSERT_EQ(result->row_num(), 4096);
  EXPECT_GE(left.threads.size(), 2);
  EXPECT_GE(right.threads.size(), 2);
  for (size_t row = 0; row < 2048; ++row) {
    EXPECT_EQ(result->get(0)->get_elem(row).GetValue<int64_t>(), row + 1);
    EXPECT_EQ(result->get(0)->get_elem(row + 2048).GetValue<int64_t>(),
              row + 10000);
  }
}

TEST(TaskSchedulerTest,
     JoinProbesMaterializedInputThenKeepsParallelTransforms) {
  PlanParser::get().init();
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  physical::PhysicalPlan plan;
  AddJoin(plan, 1);
  ContextMeta meta;
  meta.set(0, DataType::INT64);
  auto join = ops::JoinOprBuilder().Build(graph.schema(), meta, plan, 0);
  ASSERT_TRUE(join);
  RangeObservation observation;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<GlobalRangeBarrier>());
  operators.push_back(std::move(join->first));
  operators.push_back(std::make_unique<ObservedRangeMap>(observation, 0));
  Pipeline pipeline(std::move(operators));
  auto result = collect_chunk(pipeline.ExecuteReader(
      storage, context_from_batches(one_chunk(RangeChunk(0, 16384))), {},
      nullptr, 2));
  ASSERT_TRUE(result);
  ASSERT_EQ(result->row_num(), 16384);
  EXPECT_GE(observation.threads.size(), 2);
  for (size_t row = 0; row < result->row_num(); ++row) {
    EXPECT_EQ(result->get(0)->get_elem(row).GetValue<int64_t>(), row);
  }
}
TEST(TaskSchedulerTest, ParallelIntermediateLimitLeavesNextBranchUnused) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  RangeObservation observation;
  int unused = 0;
  std::vector<std::unique_ptr<IOperator>> first;
  first.push_back(
      std::make_unique<CallbackSource>([] { return RangeChunk(0, 16384); }));
  first.push_back(std::make_unique<ObservedRangeMap>(observation, 0));
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<TestFork>(
      SubPipelineMode::kSequential, Pipeline(std::move(first)),
      OneOperator(std::make_unique<CallbackSource>([] { return MakeChunk(99); },
                                                   &unused))));
  physical::PhysicalPlan plan;
  auto* range =
      plan.add_plan()->mutable_opr()->mutable_limit()->mutable_range();
  range->set_lower(0);
  range->set_upper(1);
  auto limit = ops::LimitOprBuilder().Build(Schema(), ContextMeta(), plan, 0);
  ASSERT_TRUE(limit);
  operators.push_back(std::move(limit->first));
  Pipeline pipeline(std::move(operators));
  auto result =
      collect_chunk(pipeline.ExecuteReader(storage, {}, {}, nullptr, 2));
  ASSERT_TRUE(result);
  ASSERT_EQ(result->row_num(), 1);
  EXPECT_EQ(result->get(0)->get_elem(0).GetValue<int64_t>(), 0);
  EXPECT_GE(observation.threads.size(), 2);
  EXPECT_EQ(unused, 0);
}
}  // namespace
}  // namespace neug::execution
