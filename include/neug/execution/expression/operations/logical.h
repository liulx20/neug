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

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/expression/operations/expr_executors.h"

namespace neug {
namespace execution {
struct NotOperator {
  template <typename T>
  static inline T Operation(T input) {
    return !input;
  }
};

template <class OP>
void TemplatedLogical(IContextColumn& left, IContextColumn& right, size_t count,
                      ValueColumnBuilder<bool>& result) {
  result.reserve(count);
  assert(left.elem_type().id() == DataTypeId::kBoolean &&
         right.elem_type().id() == DataTypeId::kBoolean);

  if (left.is_constant() && right.is_constant()) {
    const auto& casted_left = static_cast<const ConstColumn&>(left);
    const auto& casted_right = static_cast<const ConstColumn&>(right);
    const auto& left_val = casted_left.get_elem(0);
    const auto& right_val = casted_right.get_elem(0);
    bool left_bool = left_val.IsNull() ? false : left_val.GetValue<bool>();
    bool right_bool = right_val.IsNull() ? false : right_val.GetValue<bool>();
    bool res = false;
    bool is_null = OP::Operation(left_bool, right_bool, left_val.IsNull(),
                                 right_val.IsNull(), res);
    if (is_null) {
      for (size_t i = 0; i < count; i++) {
        result.push_back_null();
      }
    } else {
      for (size_t i = 0; i < count; i++) {
        result.push_back_opt(res);
      }
    }

  } else if (left.is_constant()) {
    const auto& casted_left = static_cast<const ConstColumn&>(left);
    const auto& left_val = casted_left.get_elem(0);
    bool left_bool = left_val.IsNull() ? false : left_val.GetValue<bool>();
    const auto& casted_right = static_cast<const ValueColumn<bool>&>(right);
    if (casted_right.is_optional()) {
      for (size_t i = 0; i < count; i++) {
        if (casted_right.has_value(i)) {
          bool right_bool = casted_right.get_value(i);
          bool res = false;
          bool is_null = OP::Operation(left_bool, right_bool, left_val.IsNull(),
                                       false, res);
          if (is_null) {
            result.push_back_null();
          } else {
            result.push_back_opt(res);
          }
        } else {
          bool right_bool = false;
          bool res = false;
          bool is_null = OP::Operation(left_bool, right_bool, left_val.IsNull(),
                                       true, res);
          if (is_null) {
            result.push_back_null();
          } else {
            result.push_back_opt(res);
          }
        }
      }
    } else {
      for (size_t i = 0; i < count; i++) {
        bool right_bool = casted_right.get_value(i);
        bool res = false;
        bool is_null =
            OP::Operation(left_bool, right_bool, left_val.IsNull(), false, res);
        if (is_null) {
          result.push_back_null();
        } else {
          result.push_back_opt(res);
        }
      }
    }
  } else if (right.is_constant()) {
    const auto& casted_right = static_cast<const ConstColumn&>(right);
    const auto& right_val = casted_right.get_elem(0);
    bool right_bool = right_val.IsNull() ? false : right_val.GetValue<bool>();
    const auto& casted_left = static_cast<const ValueColumn<bool>&>(left);
    if (casted_left.is_optional()) {
      for (size_t i = 0; i < count; i++) {
        if (casted_left.has_value(i)) {
          bool left_bool = casted_left.get_value(i);
          bool res = false;
          bool is_null = OP::Operation(left_bool, right_bool, false,
                                       right_val.IsNull(), res);
          if (is_null) {
            result.push_back_null();
          } else {
            result.push_back_opt(res);
          }
        } else {
          bool left_bool = false;
          bool res = false;
          bool is_null = OP::Operation(left_bool, right_bool, true,
                                       right_val.IsNull(), res);
          if (is_null) {
            result.push_back_null();
          } else {
            result.push_back_opt(res);
          }
        }
      }
    } else {
      for (size_t i = 0; i < count; i++) {
        bool left_bool = casted_left.get_value(i);
        bool res = false;
        bool is_null = OP::Operation(left_bool, right_bool, false,
                                     right_val.IsNull(), res);
        if (is_null) {
          result.push_back_null();
        } else {
          result.push_back_opt(res);
        }
      }
    }
  } else {
    const auto& casted_left = static_cast<const ValueColumn<bool>&>(left);
    const auto& casted_right = static_cast<const ValueColumn<bool>&>(right);
    if (casted_left.is_optional() || casted_right.is_optional()) {
      for (size_t i = 0; i < count; i++) {
        bool left_bool =
            casted_left.has_value(i) ? casted_left.get_value(i) : false;
        bool right_bool =
            casted_right.has_value(i) ? casted_right.get_value(i) : false;
        bool left_null = !casted_left.has_value(i);
        bool right_null = !casted_right.has_value(i);
        bool res = false;
        bool is_null =
            OP::Operation(left_bool, right_bool, left_null, right_null, res);
        if (is_null) {
          result.push_back_null();
        } else {
          result.push_back_opt(res);
        }
      }
    } else {
      for (size_t i = 0; i < count; i++) {
        bool left_bool = casted_left.get_value(i);
        bool right_bool = casted_right.get_value(i);
        bool res = false;
        OP::Operation(left_bool, right_bool, false, false, res);
        result.push_back_opt(res);
      }
    }
  }
}

struct TernaryOr {
  static bool Operation(bool left, bool right, bool left_null, bool right_null,
                        bool& result) {
    if (left_null && right_null) {
      // both NULL:
      // result is NULL
      return true;
    } else if (left_null) {
      // left is NULL:
      // result is TRUE if right is true
      // result is NULL if right is false
      result = right;
      return !right;
    } else if (right_null) {
      // right is NULL:
      // result is TRUE if left is true
      // result is NULL if left is false
      result = left;
      return !left;
    } else {
      // no NULL: perform the OR
      result = left || right;
      return false;
    }
  }
};

struct TernaryAnd {
  static bool Operation(bool left, bool right, bool left_null, bool right_null,
                        bool& result) {
    if (left_null && right_null) {
      // both NULL:
      // result is NULL
      return true;
    } else if (left_null) {
      // left is NULL:
      // result is FALSE if right is false
      // result is NULL if right is true
      result = right;
      return right;
    } else if (right_null) {
      // right is NULL:
      // result is FALSE if left is false
      // result is NULL if left is true
      result = left;
      return left;
    } else {
      // no NULL: perform the AND
      result = left && right;
      return false;
    }
  }
};

}  // namespace execution
}  // namespace neug