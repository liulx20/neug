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
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>
#include "query_test_utils.h"

#include "neug/common/columns/value_columns.h"
#include "neug/execution/execute/ops/retrieve/join.h"
#include "neug/execution/execute/ops/retrieve/limit.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/storages/graph/property_graph.h"

namespace neug::execution {
namespace {
struct Observation {
  std::mutex mutex;
  std::condition_variable ready;
  std::set<std::thread::id> threads;
  size_t started = 0;
  size_t finished = 0;
  size_t finalized = 0;
  bool require_overlap = false;
  int64_t fail_at = -1;
};

class RangeSource final : public MorselSource {
 public:
  RangeSource(size_t rows, Observation& observed)
      : rows_(rows), observed_(observed) {}
  result<std::optional<Morsel>> Pick() override {
    if (ended_) {
      return std::optional<Morsel>{};
    }
    auto end = std::min(offset_ + size_t{11}, rows_);
    Morsel work{0, offset_, end, nullptr};
    offset_ = end;
    ended_ = offset_ == rows_;
    return std::optional<Morsel>(work);
  }
  std::unique_ptr<MorselReader> CreateReader() override {
    class Reader final : public MorselReader {
     public:
      explicit Reader(Observation& observed) : observed_(observed) {}
      void Start(const Morsel& work) override {
        work_ = work;
        done_ = false;
        std::unique_lock<std::mutex> lock(observed_.mutex);
        observed_.threads.insert(std::this_thread::get_id());
        ++observed_.started;
        observed_.ready.notify_all();
        if (observed_.require_overlap) {
          EXPECT_TRUE(
              observed_.ready.wait_for(lock, std::chrono::seconds(3),
                                       [&] { return observed_.started >= 2; }));
        }
      }
      result<std::optional<ContextChunk>> Next() override {
        if (done_) {
          return std::optional<ContextChunk>{};
        }
        ValueColumnBuilder<int64_t> builder;
        auto end = std::min(work_.begin + size_t{3}, work_.end);
        if (observed_.fail_at >= 0 &&
            work_.begin == size_t(observed_.fail_at)) {
          return tl::unexpected(Status::InternalError("range read failed"));
        }
        for (size_t row = work_.begin; row < end; ++row) {
          builder.push_back_opt(row);
        }
        work_.begin = end;
        done_ = end == work_.end;
        if (done_) {
          std::lock_guard<std::mutex> lock(observed_.mutex);
          ++observed_.finished;
        }
        ContextChunk chunk;
        chunk.set(0, builder.finish());
        return std::optional<ContextChunk>(std::move(chunk));
      }

     private:
      Observation& observed_;
      Morsel work_;
      bool done_ = true;
    };
    return std::make_unique<Reader>(observed_);
  }
  Status Finalize() override {
    std::lock_guard<std::mutex> lock(observed_.mutex);
    EXPECT_EQ(observed_.started, observed_.finished);
    ++observed_.finalized;
    return Status::OK();
  }

 private:
  size_t rows_;
  Observation& observed_;
  size_t offset_ = 0;
  bool ended_ = false;
};

class RangeSourceOpr final : public MorselSourceOperator {
 public:
  RangeSourceOpr(size_t rows, Observation& observed)
      : rows_(rows), observed_(observed) {}
  bool consumes_input() const override { return false; }
  std::string get_operator_name() const override { return "RangeSource"; }
  std::unique_ptr<MorselSource> CreateMorselSource(IStorageInterface&,
                                                   const ParamsMap&) override {
    return std::make_unique<RangeSource>(rows_, observed_);
  }

 private:
  size_t rows_;
  Observation& observed_;
};

class EvenProject final : public IOperator {
 public:
  PipelineBehavior pipeline_behavior() const override {
    return PipelineBehavior::kChunkLocal;
  }
  std::string get_operator_name() const override { return "EvenProject"; }
  Kernel CreateState(IStorageInterface&, const ParamsMap&, OprTimer*) override {
    return make_chunk_kernel([](ContextChunk&& input) -> result<ContextChunk> {
      ValueColumnBuilder<int64_t> column;
      for (size_t row = 0; row < input.row_num(); ++row) {
        auto value = input.get(0)->get_elem(row).GetValue<int64_t>();
        if (value % 2 == 0) {
          column.push_back_opt(value * 2);
        }
      }
      ContextChunk output;
      output.set(0, column.finish());
      return output;
    });
  }
};

class JoinWithSources final : public BuildProbeOperator {
 public:
  JoinWithSources(std::unique_ptr<IOperator> kernel, Pipeline left,
                  Pipeline right)
      : kernel_(std::move(kernel)),
        left_(std::move(left)),
        right_(std::move(right)) {}
  std::string get_operator_name() const override { return "JoinWithSources"; }
  SubPipelines sub_pipelines() override {
    return {SubPipelineMode::kBuildProbe, {&left_, &right_}};
  }
  std::shared_ptr<BuildProbeState> CreateBuildState(size_t workers) override {
    return kernel_->CreateBuildState(workers);
  }

 private:
  std::unique_ptr<IOperator> kernel_;
  Pipeline left_, right_;
};

TEST(MorselExecutionTest, JoinBuildAndProbeUsePartitionedSources) {
  PlanParser::get().init();
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  for (size_t workers : {1, 4}) {
    Observation left, right;
    left.require_overlap = right.require_overlap = workers > 1;
    auto source = [](size_t size, Observation& observed) {
      std::vector<std::unique_ptr<IOperator>> operators;
      operators.push_back(std::make_unique<RangeSourceOpr>(size, observed));
      return Pipeline(std::move(operators));
    };
    physical::PhysicalPlan plan;
    auto* join = plan.add_plan()->mutable_opr()->mutable_join();
    join->set_join_kind(physical::Join_JoinKind_INNER);
    join->add_left_keys()->mutable_tag()->set_id(0);
    join->add_right_keys()->mutable_tag()->set_id(0);
    ContextMeta meta;
    meta.set(0, DataType::INT64);
    ops::JoinOprBuilder builder;
    auto kernel = builder.Build(graph.schema(), meta, plan, 0);
    ASSERT_TRUE(kernel);
    std::vector<std::unique_ptr<IOperator>> operators;
    operators.push_back(std::make_unique<JoinWithSources>(
        std::move(kernel->first), source(157, left), source(123, right)));
    operators.push_back(std::make_unique<EvenProject>());
    Pipeline pipeline(std::move(operators));
    OprTimer timer;
    auto stream = pipeline.ExecuteReader(storage, {}, {}, &timer, workers);
    auto result = collect_chunk(std::move(stream));
    ASSERT_TRUE(result) << result.error().ToString();
    ASSERT_EQ(result->row_num(), 62);
    for (size_t row = 0; row < result->row_num(); ++row) {
      EXPECT_EQ(result->get(0)->get_elem(row).GetValue<int64_t>(), row * 4);
    }
    EXPECT_EQ(left.finalized, 1);
    EXPECT_EQ(right.finalized, 1);
    auto profile = OprTimer::ToProfileResult(&timer);
    EXPECT_EQ(profile.operators(0).output_rows(), 123);
    if (workers > 1) {
      EXPECT_GT(left.threads.size(), 1);
      EXPECT_GT(right.threads.size(), 1);
    }
  }
}

TEST(MorselExecutionTest, ClaimsRangesDynamicallyAndFusesLocalTransforms) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  for (size_t workers : {1, 4}) {
    for (size_t rows : {0, 157}) {
      Observation observed;
      observed.require_overlap = workers > 1 && rows > 0;
      std::vector<std::unique_ptr<IOperator>> operators;
      operators.push_back(std::make_unique<RangeSourceOpr>(rows, observed));
      operators.push_back(std::make_unique<EvenProject>());
      Pipeline pipeline(std::move(operators));
      OprTimer timer;
      auto output = pipeline.ExecuteReader(storage, {}, {}, &timer, workers);
      EXPECT_EQ(observed.started, 0);
      auto chunks = collect_batches(std::move(output));
      ASSERT_TRUE(chunks) << chunks.error().ToString();
      std::vector<int64_t> actual;
      for (const auto& chunk : *chunks) {
        for (size_t row = 0; row < chunk.row_num(); ++row) {
          actual.push_back(chunk.get(0)->get_elem(row).GetValue<int64_t>());
        }
      }
      std::vector<int64_t> expected;
      for (size_t row = 0; row < rows; row += 2) {
        expected.push_back(row * 2);
      }
      EXPECT_EQ(actual, expected);
      EXPECT_EQ(observed.finalized, 1);
      EXPECT_EQ(observed.started, observed.finished);
      if (observed.require_overlap) {
        EXPECT_GT(observed.threads.size(), 1);
      }
      auto profile = OprTimer::ToProfileResult(&timer);
      ASSERT_EQ(profile.operators_size(), 2);
      EXPECT_EQ(profile.operators(0).output_rows(), rows);
      EXPECT_EQ(profile.operators(1).output_rows(), expected.size());
    }
  }
}

TEST(MorselExecutionTest, GlobalLimitIsNotDuplicatedAcrossWorkers) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Observation observed;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<RangeSourceOpr>(10000, observed));
  physical::PhysicalPlan plan;
  auto* range =
      plan.add_plan()->mutable_opr()->mutable_limit()->mutable_range();
  range->set_lower(5);
  range->set_upper(17);
  ops::LimitOprBuilder builder;
  auto limit = builder.Build(graph.schema(), ContextMeta(), plan, 0);
  ASSERT_TRUE(limit);
  operators.push_back(std::move(limit->first));
  Pipeline pipeline(std::move(operators));
  auto output =
      collect_chunk(pipeline.ExecuteReader(storage, {}, {}, nullptr, 4));
  ASSERT_TRUE(output);
  ASSERT_EQ(output->row_num(), 12);
  for (size_t row = 0; row < output->row_num(); ++row) {
    EXPECT_EQ(output->get(0)->get_elem(row).GetValue<int64_t>(), row + 5);
  }
  EXPECT_LT(observed.started * 11, 10000);
  EXPECT_EQ(observed.finalized, 0);
}

TEST(MorselExecutionTest, DeferredReadErrorDrainsWorkAndRemainsTerminal) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  Observation observed;
  observed.fail_at = 11;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<RangeSourceOpr>(1000, observed));
  Pipeline pipeline(std::move(operators));
  auto output = pipeline.ExecuteReader(storage, {}, {}, nullptr, 4);
  for (int chunk = 0; chunk < 4; ++chunk) {
    auto next = output.Next();
    ASSERT_TRUE(next);
    ASSERT_TRUE(*next);
  }
  auto failure = output.Next();
  ASSERT_FALSE(failure);
  EXPECT_NE(failure.error().ToString().find("range read failed"),
            std::string::npos);
  auto started = observed.started;
  EXPECT_FALSE(output.Next());
  EXPECT_EQ(observed.started, started);
}
}  // namespace
}  // namespace neug::execution
