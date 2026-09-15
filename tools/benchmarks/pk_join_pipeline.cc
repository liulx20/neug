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

// Physical PK Join benchmark: real vertex index, prebuilt input, full
// collection.
#include <chrono>
#include <iostream>
#include "neug/common/columns/value_columns.h"
#include "neug/execution/execute/ops/retrieve/join.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/execute/task_scheduler.h"
#include "neug/main/neug_db.h"

using namespace neug;
using namespace neug::execution;

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: pk_join_pipeline database rows\n";
    return 1;
  }
  size_t rows = std::stoull(argv[2]);
  NeugDB db;
  if (!db.Open(argv[1], 4, DBMode::READ_ONLY)) {
    return 1;
  }
  // The view only reads the opened, immutable database.
  GraphView view(const_cast<PropertyGraph&>(db.graph()));
  StorageReadInterface storage(view, MAX_TIMESTAMP);
  PlanParser::get().init();
  physical::PhysicalPlan plan;
  auto* join = plan.add_plan()->mutable_opr()->mutable_join();
  join->set_join_kind(physical::Join_JoinKind_INNER);
  auto* key = join->add_left_keys();
  key->mutable_tag()->set_id(2);
  key->mutable_property()->mutable_key()->set_name("id");
  join->add_right_keys()->mutable_tag()->set_id(1);
  auto* scan =
      join->mutable_left_plan()->add_plan()->mutable_opr()->mutable_scan();
  scan->mutable_alias()->set_value(2);
  scan->mutable_params()->add_tables()->set_id(
      db.graph().schema().get_vertex_label_id("item"));
  auto* child = join->mutable_right_plan()->add_plan();
  auto* metadata = child->add_meta_data();
  metadata->set_alias(1);
  metadata->mutable_type()->mutable_data_type()->set_primitive_type(
      ::common::DT_SIGNED_INT64);
  auto* mapping = child->mutable_opr()->mutable_project()->add_mappings();
  mapping->mutable_alias()->set_value(1);
  auto* variable = mapping->mutable_expr()->add_operators()->mutable_var();
  variable->mutable_tag()->set_id(0);
  variable->mutable_node_type()->mutable_data_type()->set_primitive_type(
      ::common::DT_SIGNED_INT64);
  ContextMeta meta;
  meta.set(0, DataType::INT64);
  auto built =
      ops::PrimaryKeyJoinOprBuilder().Build(db.graph().schema(), meta, plan, 0);
  if (!built || !built->first ||
      built->first->get_operator_name() != "PrimaryJoinOpr") {
    return 2;
  }
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::move(built->first));
  Pipeline pipeline(std::move(operators));
  auto pool = std::make_shared<TaskPool>(4);
  std::cout << "BENCH_JSON [";
  bool first = true;
  for (size_t workers : {1, 2, 4}) {
    for (int repeat = -1; repeat < 5; ++repeat) {
      ChunkBatch input;
      for (size_t start = 0; start < rows; start += 4096) {
        ValueColumnBuilder<int64_t> values;
        for (size_t row = start; row < std::min(rows, start + 4096); ++row) {
          values.push_back_opt(row);
        }
        ContextChunk chunk;
        chunk.set(0, values.finish());
        input.push_back(std::move(chunk));
      }
      Context context;
      for (auto& chunk : input) {
        context.append_chunk(std::move(chunk));
      }
      auto start = std::chrono::steady_clock::now();
      auto result = materialize(pipeline.ExecuteReader(
          storage, std::move(context), {}, nullptr, workers, pool));
      auto ms = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - start)
                    .count();
      if (!result) {
        return 3;
      }
      size_t position = 0;
      for (const auto& chunk : result->chunks()) {
        for (size_t row = 0; row < chunk.row_num(); ++row) {
          auto vertex = chunk.get(2)->get_elem(row).GetValue<vertex_t>();
          if (chunk.get(1)->get_elem(row).GetValue<int64_t>() != position ||
              view.GetOid(vertex.label(), vertex.vid(), MAX_TIMESTAMP)
                      .GetValue<int64_t>() != position) {
            return 4;
          }
          ++position;
        }
      }
      if (position != rows) {
        return 5;
      }
      if (repeat >= 0) {
        if (!first) {
          std::cout << ',';
        }
        first = false;
        std::cout << "{\"workers\":" << workers << ",\"ms\":" << ms << '}';
      }
    }
  }
  std::cout << "]\n";
  return 0;
}
