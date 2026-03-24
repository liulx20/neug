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

#include "neug/execution/common/columns/const_column.h"
#include "neug/execution/common/columns/value_columns.h"

namespace neug {
namespace execution {

std::shared_ptr<IContextColumn> ConstColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  return std::make_shared<ConstColumn>(val_, offsets.size());
}

std::shared_ptr<IContextColumn> ConstColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  LOG(FATAL) << "ConstColumn does not support optional shuffle";
  return nullptr;
}

template <typename T>
std::shared_ptr<IContextColumn> union_impl(const Value& val, size_t size,
                                           const ValueColumn<T>& other_col) {
  ValueColumnBuilder<T> builder;
  builder.reserve(size + other_col.size());
  for (size_t i = 0; i < size; ++i) {
    if (val.IsNull()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(val.GetValue<T>());
    }
  }
  if (other_col.is_optional()) {
    for (size_t i = 0; i < other_col.size(); ++i) {
      if (other_col.has_value(i)) {
        builder.push_back_opt(other_col.get_value(i));
      } else {
        builder.push_back_null();
      }
    }
  } else {
    for (size_t i = 0; i < other_col.size(); ++i) {
      builder.push_back_opt(other_col.get_value(i));
    }
  }
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> union_const(const Value& val, size_t size,
                                            const Value& val2, size_t size2) {
  ValueColumnBuilder<T> builder;
  builder.reserve(size + size2);
  T value = val.GetValue<T>();
  for (size_t i = 0; i < size; ++i) {
    if (val.IsNull()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(value);
    }
  }
  T value2 = val2.GetValue<T>();
  for (size_t i = 0; i < size2; ++i) {
    if (val2.IsNull()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(value2);
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> ConstColumn::union_col(
    std::shared_ptr<IContextColumn> other) const {
  if (other->column_type() == ContextColumnType::kConst) {
    auto other_const_col = std::dynamic_pointer_cast<ConstColumn>(other);
    if (val_ == other_const_col->val_) {
      return std::make_shared<ConstColumn>(val_, size() + other->size());
    } else {
      if (elem_type() != other->elem_type()) {
        LOG(FATAL) << "Cannot union ConstColumns with different types: "
                   << elem_type().ToString() << " vs "
                   << other->elem_type().ToString();
        return nullptr;
      }
      switch (elem_type().id()) {
#define TYPE_DISPATCHER(enum_val, cpp_type)                               \
  case DataTypeId::enum_val: {                                            \
    auto other_const_col = std::dynamic_pointer_cast<ConstColumn>(other); \
    return union_const<cpp_type>(val_, size(), other_const_col->val_,     \
                                 other->size());                          \
  } break;
        FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
      default:
        LOG(FATAL) << "Unsupported data type for union of ConstColumns: "
                   << elem_type().ToString();
        return nullptr;
      }
    }
  }
  switch (other->elem_type().id()) {
#define TYPE_DISPATCHER(enum_val, cpp_type)                                   \
  case DataTypeId::enum_val: {                                                \
    auto other_col = std::dynamic_pointer_cast<ValueColumn<cpp_type>>(other); \
    return union_impl<cpp_type>(val_, size(), *other_col);                    \
  } break;
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    LOG(FATAL) << "Unsupported data type for union with ConstColumn: "
               << other->elem_type().ToString();
    return nullptr;
  }
}

}  // namespace execution
}  // namespace neug