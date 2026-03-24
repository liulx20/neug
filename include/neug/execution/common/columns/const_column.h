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

#include "neug/execution/common/columns/i_context_column.h"

namespace neug {
namespace execution {
class ConstColumnBuilder;

class ConstColumn : public IContextColumn {
 public:
  explicit ConstColumn(const Value& val) : val_(val) {}
  ~ConstColumn() = default;

  size_t size() const override { return 1; }

  std::string column_info() const override {
    return "ConstColumn(" + val_.to_string() + ")";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kConst;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const select_vector_t& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const select_vector_t& offsets) const override;

  const DataType& elem_type() const override { return val_.type(); }

  Value get_elem(size_t idx) const override { return val_; }

  bool generate_dedup_offset(select_vector_t& offsets) const override {
    offsets.push_back(0);
    return true;
  }

  bool is_optional() const override { return val_.IsNull(); }

  bool has_value(size_t idx) const override { return !val_.IsNull(); }

  template <typename T>
  T get_value() const {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return val_.GetValue<std::string>();
    } else {
      return val_.GetValue<T>();
    }
  }

  bool is_null() const { return val_.IsNull(); }

  bool is_constant() const override { return true; }

 private:
  Value val_;
};

}  // namespace execution
}  // namespace neug