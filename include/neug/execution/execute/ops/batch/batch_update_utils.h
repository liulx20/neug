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
#pragma once

#include <vector>

#include "neug/common/types/graph_types.h"
#include "neug/execution/execute/operator_state.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/property/types.h"

namespace physical {
class PropertyMapping;
}
namespace google {
namespace protobuf {
template <typename T>
class RepeatedPtrField;
}
}  // namespace google

namespace neug {
class IDataChunkSupplier;
class Schema;
class StorageReadInterface;
namespace execution {

namespace ops {

std::string vertex_to_json_string(label_t label, vid_t vid,
                                  const StorageReadInterface& graph);

std::string edge_to_json_string(const EdgeRecord& edge,
                                const StorageReadInterface& graph);

std::string path_to_json_string(Path& path, const StorageReadInterface& graph);

class BatchChunkSupplier final : public IDataChunkSupplier {
 public:
  BatchChunkSupplier(ChunkBatch chunks,
                     std::vector<std::pair<int32_t, std::string>> mappings);
  std::shared_ptr<DataChunk> GetNextChunk() override;
  int64_t RowNum() const override { return -1; }
  size_t rows_read() const { return rows_read_; }

 private:
  ChunkBatch chunks_;
  size_t index_ = 0;
  std::vector<std::pair<int32_t, std::string>> mappings_;
  size_t rows_read_ = 0;
};

// Preserve COPY result cardinality without retaining its input payload.
ContextChunk batch_insert_result(size_t rows);

template <typename F>
Kernel make_insert_kernel(F insert) {
  class State final : public OperatorState {
   public:
    explicit State(F insert) : insert_(std::move(insert)) {}
    KernelResult Process(ContextChunk chunk) override {
      GS_AUTO(rows, insert_(std::move(chunk)));
      rows_ += rows;
      return ChunkBatch{};
    }
    KernelResult Finalize() override {
      return one_chunk(batch_insert_result(rows_));
    }

   private:
    F insert_;
    size_t rows_ = 0;
  };
  return std::make_unique<State>(std::move(insert));
}

std::vector<std::string> match_files_with_pattern(const std::string& file_path);

std::vector<std::shared_ptr<IDataChunkSupplier>> create_csv_chunk_suppliers(
    const std::string& file_path, const std::vector<DataType>& column_types,
    const std::unordered_map<std::string, std::string> csv_options);

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings);

}  // namespace ops

}  // namespace execution

}  // namespace neug
