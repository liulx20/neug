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
  std::mutex mutex;
  std::condition_variable ready;
  int started = 0;
  auto branch = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    ++started;
    ready.notify_all();
    EXPECT_TRUE(ready.wait_for(lock, std::chrono::seconds(5),
                               [&] { return started == 2; }));
    return std::this_thread::get_id();
  };
  auto task =
      scheduler.Submit([&] { return scheduler.RunPair(branch, branch); });
  auto ids = scheduler.Wait(task);
  EXPECT_NE(ids.first, ids.second);
  EXPECT_NE(ids.first, std::this_thread::get_id());
  EXPECT_NE(ids.second, std::this_thread::get_id());
}

TEST(TaskSchedulerTest, NestedDependenciesWithOneWorker) {
  TaskScheduler scheduler(1);
  std::function<int(int)> run = [&](int depth) {
    if (depth == 0) {
      return 1;
    }
    auto pair = scheduler.RunPair([&] { return run(depth - 1); },
                                  [&] { return run(depth - 1); });
    return pair.first + pair.second;
  };
  auto task = scheduler.Submit([&] { return run(5); });
  EXPECT_EQ(scheduler.Wait(task), 32);
}

TEST(TaskSchedulerTest, ExceptionsDoNotAbandonSiblingTasks) {
  TaskScheduler scheduler(1);
  std::atomic<int> completed{0};
  for (bool fail_left : {false, true}) {
    auto task = scheduler.Submit([&] {
      return scheduler.RunPair(
          [&] {
            ++completed;
            if (fail_left) {
              throw std::runtime_error("left failed");
            }
            return 1;
          },
          [&] {
            ++completed;
            if (!fail_left) {
              throw std::runtime_error("right failed");
            }
            return 2;
          });
    });
    EXPECT_THROW(scheduler.Wait(task), std::runtime_error);
  }
  EXPECT_EQ(completed.load(), 4);
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
    auto stream = pipeline.ExecuteScheduled(storage, std::move(input), {}, 2);
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
  auto stream = pipeline.ExecuteScheduled(storage, std::move(input), {}, 2);
  auto first = stream.Next();
  ASSERT_FALSE(first);
  EXPECT_NE(first.error().ToString().find("scheduled source error"),
            std::string::npos);
  EXPECT_FALSE(stream.Next());
  EXPECT_EQ(calls, 1);
  auto invalid = pipeline.ExecuteScheduled(storage, {}, {}, 0);
  EXPECT_FALSE(invalid.Next());
}

TEST(TaskSchedulerTest, UnsupportedOperatorsAreRejectedBeforeEval) {
  class UnsafeOperator final : public IOperator {
   public:
    std::string get_operator_name() const override { return "UnsafeOperator"; }
    Stream<ContextChunk> Eval(IStorageInterface&, const ParamsMap&,
                              Stream<ContextChunk>&&, OprTimer*,
                              TaskScheduler*) override {
      ADD_FAILURE() << "Unsupported operator must not be initialized";
      return {};
    }
  };
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<UnsafeOperator>());
  Pipeline pipeline(std::move(operators));
  auto output = pipeline.ExecuteScheduled(storage, {}, {}, 2);
  auto next = output.Next();
  ASSERT_FALSE(next);
  EXPECT_NE(next.error().ToString().find("does not support"),
            std::string::npos);
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
    auto stream =
        pipeline.ExecuteScheduled(storage, input(), {}, workers, &timer);
    auto actual = collect_chunk(std::move(stream));
    ASSERT_TRUE(actual) << actual.error().ToString();
    ASSERT_EQ(actual->row_num(), expected->row_num());
    for (size_t row = 0; row < actual->row_num(); ++row) {
      EXPECT_EQ(actual->get(0)->get_elem(row), expected->get(0)->get_elem(row));
    }
    auto profile = OprTimer::ToProfileResult(&timer);
    EXPECT_GE(profile.operators_size(), 3);
    EXPECT_EQ(profile.operators(0).output_rows(), 17);
  }
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
  auto output = materialize(pipeline.ExecuteScheduled(storage, {}, {}, 2));
  ASSERT_TRUE(output);
  EXPECT_EQ(output->row_num(), 0);
  EXPECT_EQ(output->tag_ids, (std::vector<int>{3, 0, 3}));
}
}  // namespace
}  // namespace neug::execution
