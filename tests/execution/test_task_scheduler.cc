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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "neug/common/columns/value_columns.h"
#include "neug/execution/execute/ops/retrieve/join.h"
#include "neug/execution/execute/ops/retrieve/sink.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/pipeline_graph.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/execute/task_scheduler.h"
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

TEST(TaskSchedulerTest, BranchesActuallyOverlap) {
  TaskScheduler scheduler(2);
  PipelineGraph graph;
  std::mutex mutex;
  std::condition_variable ready;
  int started = 0;
  std::thread::id ids[2];
  auto seed = graph.Add("seed", {}, [] { return Status::OK(); });
  std::vector<PipelineGraph::NodeId> dependencies;
  for (int i = 0; i < 2; ++i) {
    dependencies.push_back(graph.Add("branch", {seed}, [&, i] {
      std::unique_lock<std::mutex> lock(mutex);
      ids[i] = std::this_thread::get_id();
      ++started;
      ready.notify_all();
      EXPECT_TRUE(ready.wait_for(lock, std::chrono::seconds(5),
                                 [&] { return started == 2; }));
      return Status::OK();
    }));
  }
  int joined = 0;
  auto join = graph.Add("join", dependencies, [&] {
    EXPECT_EQ(started, 2);
    ++joined;
    return Status::OK();
  });
  EXPECT_EQ(graph.dependencies(join), dependencies);
  ASSERT_TRUE(graph.Execute(scheduler));
  EXPECT_NE(ids[0], ids[1]);
  EXPECT_NE(ids[0], std::this_thread::get_id());
  EXPECT_NE(ids[1], std::this_thread::get_id());
  EXPECT_EQ(joined, 1);
  EXPECT_TRUE(graph.Execute(scheduler));
  EXPECT_EQ(joined, 1);
}

TEST(TaskSchedulerTest, NestedDependenciesWithOneWorker) {
  TaskScheduler scheduler(1);
  PipelineGraph graph;
  std::vector<int> values(63, 0);
  std::function<PipelineGraph::NodeId(int)> build = [&](int depth) {
    std::vector<PipelineGraph::NodeId> inputs;
    if (depth != 0) {
      inputs = {build(depth - 1), build(depth - 1)};
    }
    auto id = graph.size();
    return graph.Add("reduce", inputs, [&, id, inputs] {
      values[id] = inputs.empty() ? 1 : values[inputs[0]] + values[inputs[1]];
      return Status::OK();
    });
  };
  auto output = build(5);
  ASSERT_TRUE(graph.Execute(scheduler));
  EXPECT_EQ(values[output], 32);
}

TEST(TaskSchedulerTest, FailureDrainsSubmittedTasksAndSkipsConsumers) {
  for (bool throws : {false, true}) {
    TaskScheduler scheduler(2);
    PipelineGraph graph;
    std::atomic<int> completed{0};
    auto left = graph.Add("left", {}, [&]() -> Status {
      ++completed;
      if (throws) {
        throw std::runtime_error("left failed");
      }
      return Status::InternalError("left failed");
    });
    auto right = graph.Add("right", {}, [&] {
      ++completed;
      return Status::OK();
    });
    graph.Add("join", {left, right}, [&] {
      ADD_FAILURE() << "Failed dependency must not unlock its consumer";
      return Status::OK();
    });
    if (throws) {
      EXPECT_THROW(graph.Execute(scheduler), std::runtime_error);
    } else {
      auto result = graph.Execute(scheduler);
      EXPECT_FALSE(result);
      EXPECT_NE(result.ToString().find("left failed"), std::string::npos);
    }
    EXPECT_EQ(completed.load(), 2);
  }
}

TEST(TaskSchedulerTest, ScheduledPullIsLazyOrderedAndStopsOnDestruction) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Pipeline pipeline;
  std::atomic<int> calls{0};
  const auto caller = std::this_thread::get_id();
  {
    auto input = Stream<ContextChunk>(
        [&]() -> Stream<ContextChunk>::NextResult {
          EXPECT_NE(caller, std::this_thread::get_id());
          return std::optional<ContextChunk>(MakeChunk(++calls));
        },
        StreamMetadata{{0}});
    auto stream =
        pipeline.ExecuteStream(storage, std::move(input), {}, nullptr, 2);
    EXPECT_EQ(calls.load(), 0);
    EXPECT_EQ(stream.metadata().output_columns, (std::vector<int>{0}));
    for (int64_t i = 1; i <= 3; ++i) {
      auto next = stream.Next();
      ASSERT_TRUE(next);
      ASSERT_TRUE(*next);
      EXPECT_EQ((**next).get(0)->get_elem(0).GetValue<int64_t>(), i);
      EXPECT_EQ(calls.load(), i);
    }
  }
  EXPECT_EQ(calls.load(), 3);
}

TEST(TaskSchedulerTest, ErrorsAreTerminalAndQueueCanBeDestroyed) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Pipeline pipeline;
  int calls = 0;
  auto input = Stream<ContextChunk>([&]() -> Stream<ContextChunk>::NextResult {
    ++calls;
    THROW_IO_EXCEPTION("scheduled source error");
  });
  auto stream =
      pipeline.ExecuteStream(storage, std::move(input), {}, nullptr, 2);
  auto first = stream.Next();
  ASSERT_FALSE(first);
  EXPECT_NE(first.error().ToString().find("scheduled source error"),
            std::string::npos);
  EXPECT_FALSE(stream.Next());
  EXPECT_EQ(calls, 1);
  auto invalid = pipeline.ExecuteStream(storage, {}, {}, nullptr, 0);
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
  auto input = Stream<ContextChunk>([&]() -> Stream<ContextChunk>::NextResult {
    if (++input_pulls == 4) {
      return std::optional<ContextChunk>{};
    }
    return std::optional<ContextChunk>(MakeChunk(input_pulls));
  });
  auto output =
      pipeline.ExecuteStream(storage, std::move(input), {}, nullptr, 2);
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
  auto input = Stream<ContextChunk>(
      [&, done = false]() mutable -> Stream<ContextChunk>::NextResult {
        if (done) {
          return std::optional<ContextChunk>{};
        }
        done = true;
        return std::optional<ContextChunk>(record(0));
      });
  auto stream =
      pipeline.ExecuteStream(storage, std::move(input), {}, nullptr, 4);
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
    auto output = pipeline.ExecuteStream(storage, {}, {}, nullptr, 2);
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
  auto output = pipeline.ExecuteStream(
      storage,
      error_stream<ContextChunk>(Status::InternalError("bad common input")), {},
      nullptr, 2);
  auto next = output.Next();
  ASSERT_FALSE(next);
  EXPECT_NE(next.error().ToString().find("bad common input"),
            std::string::npos);
}

TEST(TaskSchedulerTest, ReplacementSourcePrunesUnusedPipelineGraph) {
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
      collect_chunk(pipeline.ExecuteStream(storage, {}, {}, nullptr, 2));
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
  auto output = pipeline.ExecuteStream(
      storage, stream_from_batches(std::move(chunks)), {}, nullptr, 1);
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
  auto input = MakeChunk(1).union_with(MakeChunk(1));
  ASSERT_TRUE(state->PrepareBuild(std::move(input)));
  ASSERT_FALSE(state->FinalizeBuild());
  for (size_t i = 0; i < state->BuildPartitions(); ++i) {
    ASSERT_TRUE(state->BuildPartition(i));
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
    return stream_from_batches(std::move(chunks));
  };
  auto expected =
      collect_chunk(pipeline.ExecuteStream(storage, input(), {}, nullptr));
  ASSERT_TRUE(expected);
  EXPECT_EQ(expected->row_num(), 17);
  for (size_t workers : {1, 2, 4}) {
    OprTimer timer;
    auto stream = pipeline.ExecuteStream(storage, input(), {}, &timer, workers);
    auto chunks = collect_batches(std::move(stream));
    ASSERT_TRUE(chunks) << chunks.error().ToString();
    ASSERT_EQ(chunks->size(), 3);
    EXPECT_EQ((*chunks)[0].row_num(), 8);
    EXPECT_EQ((*chunks)[1].row_num(), 1);
    EXPECT_EQ((*chunks)[2].row_num(), 8);
    auto actual = collect_chunk(stream_from_batches(std::move(*chunks)));
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
    auto output = pipeline.ExecuteStream(
        storage, stream_from_batches(std::move(chunks)), {}, nullptr, 2);
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
      materialize(pipeline.ExecuteStream(storage, {}, {}, nullptr, 2));
  ASSERT_TRUE(output);
  EXPECT_EQ(output->row_num(), 0);
  EXPECT_EQ(output->tag_ids, (std::vector<int>{3, 0, 3}));
}
}  // namespace
}  // namespace neug::execution
