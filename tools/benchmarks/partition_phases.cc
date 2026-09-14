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

// Serial state-kernel diagnostic; excludes source execution and task
// scheduling.
#include <sys/resource.h>
#include <chrono>
#include <iostream>
#include "../../src/execution/execute/ops/retrieve/group_by_state.h"
#include "neug/execution/execute/ops/retrieve/dedup.h"

using namespace neug;
using namespace neug::execution;

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr
        << "usage: partition_phases "
           "dedup|composite|group|group-raw|group-partial rows cardinality\n";
    return 1;
  }
  std::string mode = argv[1];
  size_t rows = std::stoull(argv[2]), cardinality = std::stoull(argv[3]);
  if (!rows || !cardinality ||
      (mode != "dedup" && mode != "composite" && mode != "group" &&
       mode != "group-raw" && mode != "group-partial")) {
    return 1;
  }
  ChunkBatch chunks;
  for (size_t start = 0; start < rows; start += 1024) {
    ValueColumnBuilder<int64_t> keys, values;
    for (size_t row = start; row < std::min(rows, start + 1024); ++row) {
      keys.push_back_opt(row % cardinality);
      values.push_back_opt(row % 101);
    }
    ContextChunk chunk;
    chunk.set(0, keys.finish());
    chunk.set(1, values.finish());
    chunks.push_back(std::move(chunk));
  }
  std::shared_ptr<PartitionState> state;
  if (mode == "group" || mode == "group-raw" || mode == "group-partial") {
    state = std::make_shared<ops::GroupByState>(
        std::vector<std::pair<int, int>>{{0, 0}},
        std::vector<DataType>{DataType(DataTypeId::kInt64)},
        std::vector<ops::AggregateSpec>{
            {AggrKind::kCount, -1, 1, DataType(DataTypeId::kInt64)},
            {AggrKind::kSum, 1, 2, DataType(DataTypeId::kInt64)}},
        1,
        mode == "group-partial"
            ? ops::GroupByState::InputMode::kPartial
            : mode == "group-raw" ? ops::GroupByState::InputMode::kRaw
                                  : ops::GroupByState::InputMode::kAdaptive);
  } else {
    ContextMeta meta;
    meta.set(0, DataType(DataTypeId::kInt64));
    meta.set(1, DataType(DataTypeId::kInt64));
    physical::PhysicalPlan plan;
    auto* dedup = plan.add_plan()->mutable_opr()->mutable_dedup();
    dedup->add_keys()->mutable_tag()->set_id(0);
    if (mode == "composite") {
      dedup->add_keys()->mutable_tag()->set_id(1);
    }
    auto op = ops::DedupOprBuilder().Build(Schema(), meta, plan, 0);
    if (!op) {
      return 2;
    }
    state = op->first->CreatePartitionState(1);
  }
  auto timed = [](auto work) {
    auto start = std::chrono::steady_clock::now();
    work();
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - start)
        .count();
  };
  double local = 0, merge = 0;
  for (auto& chunk : chunks) {
    std::shared_ptr<PartitionState::Batch> batch;
    local += timed([&] { batch = state->PartitionBuild(std::move(chunk)); });
    if (batch) {
      merge += timed([&] {
        auto status = state->BuildPartition(0, *batch);
        if (!status) {
          throw std::runtime_error(status.ToString());
        }
      });
    }
  }
  double finalize = timed([&] {
    auto status = state->FinalizeBuild();
    if (!status) {
      throw std::runtime_error(status.ToString());
    }
  });
  auto output = state->TakeOutput();
  size_t result_rows = 0;
  for (const auto& chunk : output) {
    result_rows += chunk.row_num();
  }
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  auto peak = usage.ru_maxrss;
#ifndef __APPLE__
  peak *= 1024;
#endif
  std::cout << "{\"mode\":\"" << mode << "\",\"rows\":" << rows
            << ",\"cardinality\":" << cardinality << ",\"local_ms\":" << local
            << ",\"merge_ms\":" << merge << ",\"finalize_ms\":" << finalize
            << ",\"result_rows\":" << result_rows
            << ",\"process_peak_bytes\":" << peak << "}\n";
}
