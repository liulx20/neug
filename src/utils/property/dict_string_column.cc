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

#include "neug/utils/property/column.h"

#include <cstring>

#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"

namespace neug {

void DictStringColumn::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                            MemoryLevel level) {
  dict_.clear();
  auto dict_yaml = desc.get("dictionary");
  if (dict_yaml.has_value() && !dict_yaml->empty()) {
    auto node = YAML::Load(dict_yaml.value());
    if (node.IsSequence()) {
      dict_.reserve(node.size());
      for (const auto& item : node) {
        dict_.push_back(item.as<std::string>());
      }
    }
  }
  rebuildIndex();

  codes_.clear();
  auto codes_path = desc.get_path(ModuleDescriptor::kDataPath);
  if (codes_path.has_value() && !codes_path->empty()) {
    auto buffer = ckp.OpenFile(codes_path.value(), level);
    size_t n = buffer->GetDataSize();
    auto* data = reinterpret_cast<const uint8_t*>(buffer->GetData());
    codes_.assign(data, data + n);
  }
}

void DictStringColumn::rebuildIndex() {
  str_to_id_.clear();
  str_to_id_.reserve(dict_.size());
  for (size_t i = 0; i < dict_.size(); ++i) {
    str_to_id_.emplace(dict_[i], static_cast<uint8_t>(i));
  }
}

void DictStringColumn::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                            const std::string& key) {
  if (key.empty()) {
    THROW_RUNTIME_ERROR("DictStringColumn::Dump: module key must not be empty");
  }
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();

  YAML::Node dict_node(YAML::NodeType::Sequence);
  for (const auto& s : dict_) {
    dict_node.push_back(s);
  }
  desc.set("dictionary", YAML::Dump(dict_node));

  auto buffer =
      ckp.CreateRuntimeContainer(codes_.size(), MemoryLevel::kInMemory);
  if (!codes_.empty()) {
    std::memcpy(buffer->GetData(), codes_.data(), codes_.size());
  }
  desc.set_path(ModuleDescriptor::kDataPath, ckp.Commit(*buffer));
  meta.set_module(key, std::move(desc));
}

void DictStringColumn::resize(size_t size) { codes_.resize(size, 0); }

void DictStringColumn::resize(size_t size, const Value& default_value) {
  size_t old_size = codes_.size();
  if (size <= old_size) {
    codes_.resize(size);
    return;
  }
  codes_.resize(size, 0);
  for (size_t i = old_size; i < size; ++i) {
    set_any(i, default_value, true);
  }
}

uint8_t DictStringColumn::intern(std::string_view value) {
  std::string key(value);
  auto it = str_to_id_.find(key);
  if (it != str_to_id_.end()) {
    return it->second;
  }
  if (dict_.size() >= kMaxDictSize) {
    THROW_STORAGE_EXCEPTION(
        "DictStringColumn dictionary exceeds 256 distinct values");
  }
  auto id = static_cast<uint8_t>(dict_.size());
  dict_.push_back(key);
  str_to_id_.emplace(std::move(key), id);
  return id;
}

void DictStringColumn::set_any(size_t index, const Value& value,
                               bool /*insert_safe*/) {
  if (index >= codes_.size()) {
    THROW_RUNTIME_ERROR("DictStringColumn::set_any: index out of range");
  }
  if (value.IsNull()) {
    codes_[index] = intern(std::string_view());
    return;
  }
  codes_[index] = intern(value.GetValue<std::string>());
}

Value DictStringColumn::get_any(size_t index) const {
  return Value::STRING(std::string(get_view(index)));
}

void DictStringColumn::ingest(uint32_t index, OutArchive& arc) {
  std::string_view val;
  arc >> val;
  set_any(index, Value::STRING(std::string(val)), true);
}

std::unique_ptr<Module> DictStringColumn::Clone() const {
  auto new_col = std::make_unique<DictStringColumn>();
  new_col->codes_ = codes_;
  new_col->dict_ = dict_;
  new_col->str_to_id_ = str_to_id_;
  return new_col;
}

void DictStringColumn::Detach(Checkpoint& /*ckp*/, MemoryLevel /*level*/) {
  // vectors are already owned; nothing to fork
}

NEUG_REGISTER_MODULE(DictStringColumn);

}  // namespace neug
