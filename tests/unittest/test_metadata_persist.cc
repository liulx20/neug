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
#include <filesystem>

#include "neug/compiler/extension/extension_api.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/main/neug_db.h"

namespace {

class PersistProbeFunction : public neug::function::NeugCallFunction {
 public:
  PersistProbeFunction()
      : NeugCallFunction(
            "PERSIST_PROBE_FN", neug::function::call_input_types{},
            neug::function::call_output_columns{
                {"ok", ::neug::DataType(::neug::DataTypeId::kBoolean)}}) {}
};

struct PersistProbeFunctionSet {
  static constexpr const char* name = "PERSIST_PROBE_FN";
  static neug::function::function_set getFunctionSet() {
    neug::function::function_set funcSet;
    funcSet.emplace_back(std::make_unique<PersistProbeFunction>());
    return funcSet;
  }
};

}  // namespace

TEST(NeugDBMetadataPersist, ExtensionCatalogSurvivesCloseOpen) {
  auto test_dir =
      std::filesystem::temp_directory_path() / "neug_metadata_persist_test";
  if (std::filesystem::exists(test_dir)) {
    std::filesystem::remove_all(test_dir);
  }
  std::filesystem::create_directories(test_dir);

  neug::NeugDB db;
  db.Open((test_dir / "graph").string(), 2);

  auto* metadata_before = neug::main::MetadataRegistry::getMetadata();
  neug::extension::ExtensionAPI::registerExtension(
      {"persist_probe", "survives close/open"});
  neug::extension::ExtensionAPI::registerFunction<PersistProbeFunctionSet>(
      neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

  ASSERT_TRUE(neug::main::MetadataRegistry::getCatalog()->containsFunction(
      &neug::transaction::DUMMY_TRANSACTION, PersistProbeFunctionSet::name,
      false));

  // Same sequence as serve(): dump/reload graph stack without dropping
  // MetadataManager.
  auto config = db.config();
  db.Close();
  ASSERT_TRUE(db.Open(config));

  EXPECT_EQ(neug::main::MetadataRegistry::getMetadata(), metadata_before);
  EXPECT_TRUE(neug::main::MetadataRegistry::getCatalog()->containsFunction(
      &neug::transaction::DUMMY_TRANSACTION, PersistProbeFunctionSet::name,
      false));
  EXPECT_TRUE(neug::extension::ExtensionAPI::getLoadedExtensions().count(
      "persist_probe"));

  db.Close();
  std::filesystem::remove_all(test_dir);
}
