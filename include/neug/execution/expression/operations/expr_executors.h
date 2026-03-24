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

#include "neug/execution/common/columns/const_column.h"
#include "neug/execution/common/columns/value_columns.h"

namespace neug {
namespace execution {

struct BinaryStandardOperatorWrapper {
  template <typename OP, typename LEFT_TYPE, typename RIGHT_TYPE,
            typename RESULT_TYPE>
  static inline RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right) {
    return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left,
                                                                      right);
  }
};

struct BinarySingleArgumentOperatorWrapper {
  template <class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
  static inline RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right) {
    return OP::template Operation<LEFT_TYPE>(left, right);
  }
};

struct UnarySingleArgumentOperatorWrapper {
  template <class OP, class INPUT_TYPE, class RESULT_TYPE>
  static inline RESULT_TYPE Operation(INPUT_TYPE input) {
    return OP::template Operation<INPUT_TYPE>(input);
  }
};
struct UnaryStandardOperatorWrapper {
  template <typename OP, typename INPUT_TYPE, typename RESULT_TYPE>
  static inline RESULT_TYPE Operation(INPUT_TYPE input) {
    return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
  }
};
template <typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE,
          typename OPWRAPPER, typename OP, bool LEFT_CONSTANT,
          bool RIGHT_CONSTANT>
struct BinaryExecutor {
  static void evaluate(const IContextColumn& left, const IContextColumn& right,
                       size_t row_num,
                       ValueColumnBuilder<RESULT_TYPE>& result) {
    result.reserve(row_num);
    if constexpr (LEFT_CONSTANT && RIGHT_CONSTANT) {
      const auto& left_const = static_cast<const ConstColumn&>(left);
      const auto& right_const = static_cast<const ConstColumn&>(right);
      if (left_const.is_null() || right_const.is_null()) {
        for (size_t i = 0; i < row_num; i++) {
          result.push_back_null();
        }
        return;
      }
      auto left_val = left_const.get_value<LEFT_TYPE>();
      auto right_val = right_const.get_value<RIGHT_TYPE>();
      auto res =
          OPWRAPPER::template Operation<OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
              left_val, right_val);
      for (size_t i = 0; i < row_num; i++) {
        result.push_back_opt(res);
      }
    } else if constexpr (LEFT_CONSTANT) {
      const auto& left_const = static_cast<const ConstColumn&>(left);
      if (left_const.is_null()) {
        for (size_t i = 0; i < row_num; i++) {
          result.push_back_null();
        }
        return;
      }
      auto left_val = left_const.get_value<LEFT_TYPE>();
      const auto& casted_right =
          static_cast<const ValueColumn<RIGHT_TYPE>&>(right);
      if (casted_right.is_optional()) {
        for (size_t i = 0; i < row_num; i++) {
          if (casted_right.has_value(i)) {
            auto right_val = casted_right.get_value(i);
            auto res =
                OPWRAPPER::template Operation<OP, LEFT_TYPE, RIGHT_TYPE,
                                              RESULT_TYPE>(left_val, right_val);
            result.push_back_opt(res);
          } else {
            result.push_back_null();
          }
        }
      } else {
        for (size_t i = 0; i < row_num; i++) {
          auto right_val = casted_right.get_value(i);
          auto res =
              OPWRAPPER::template Operation<OP, LEFT_TYPE, RIGHT_TYPE,
                                            RESULT_TYPE>(left_val, right_val);
          result.push_back_opt(res);
        }
      }
    } else if constexpr (RIGHT_CONSTANT) {
      const auto& right_const = static_cast<const ConstColumn&>(right);
      if (right_const.is_null()) {
        for (size_t i = 0; i < row_num; i++) {
          result.push_back_null();
        }
        return;
      }
      auto right_val = right_const.get_value<RIGHT_TYPE>();
      auto casted_left = static_cast<const ValueColumn<LEFT_TYPE>&>(left);
      if (casted_left.is_optional()) {
        for (size_t i = 0; i < row_num; i++) {
          if (casted_left.has_value(i)) {
            auto left_val = casted_left.get_value(i);
            auto res =
                OPWRAPPER::template Operation<OP, LEFT_TYPE, RIGHT_TYPE,
                                              RESULT_TYPE>(left_val, right_val);
            result.push_back_opt(res);
          } else {
            result.push_back_null();
          }
        }
      } else {
        for (size_t i = 0; i < row_num; i++) {
          auto left_val = casted_left.get_value(i);
          auto res =
              OPWRAPPER::template Operation<OP, LEFT_TYPE, RIGHT_TYPE,
                                            RESULT_TYPE>(left_val, right_val);
          result.push_back_opt(res);
        }
      }
    } else {
      auto casted_left = static_cast<const ValueColumn<LEFT_TYPE>&>(left);
      auto casted_right = static_cast<const ValueColumn<RIGHT_TYPE>&>(right);
      if (casted_left.is_optional() || casted_right.is_optional()) {
        for (size_t i = 0; i < row_num; i++) {
          if (casted_left.has_value(i) && casted_right.has_value(i)) {
            auto left_val = casted_left.get_value(i);
            auto right_val = casted_right.get_value(i);
            auto res =
                OPWRAPPER::template Operation<OP, LEFT_TYPE, RIGHT_TYPE,
                                              RESULT_TYPE>(left_val, right_val);
            result.push_back_opt(res);
          } else {
            result.push_back_null();
          }
        }
      } else {
        for (size_t i = 0; i < row_num; i++) {
          auto left_val = casted_left.get_value(i);
          auto right_val = casted_right.get_value(i);
          auto res =
              OPWRAPPER::template Operation<OP, LEFT_TYPE, RIGHT_TYPE,
                                            RESULT_TYPE>(left_val, right_val);
          result.push_back_opt(res);
        }
      }
    }
  }
};

template <typename INPUT_TYPE, typename RESULT_TYPE, typename OPWRAPPER,
          typename OP, bool INPUT_CONSTANT>
struct UnaryExecutor {
  static void evaluate(const IContextColumn& input, size_t row_num,
                       ValueColumnBuilder<RESULT_TYPE>& result) {
    result.reserve(row_num);
    if constexpr (INPUT_CONSTANT) {
      const auto& input_const = static_cast<const ConstColumn&>(input);
      if (input_const.is_null()) {
        result.push_back_null();
        return;
      }
      auto input_val = input_const.get_value<INPUT_TYPE>();
      auto res =
          OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(input_val);
      result.push_back_opt(res);
    } else {
      const auto& casted_input =
          static_cast<const ValueColumn<INPUT_TYPE>&>(input);
      if (casted_input.is_optional()) {
        for (size_t i = 0; i < row_num; i++) {
          if (casted_input.has_value(i)) {
            auto input_val = casted_input.get_value(i);
            auto res =
                OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(
                    input_val);
            result.push_back_opt(res);
          } else {
            result.push_back_null();
          }
        }
      } else {
        for (size_t i = 0; i < row_num; i++) {
          auto input_val = casted_input.get_value(i);
          auto res = OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(
              input_val);
          result.push_back_opt(res);
        }
      }
    }
  }
};

template <bool INVERSE>
struct IsNullExecutor {
  static void evaluate(const IContextColumn& input, size_t row_num,
                       ValueColumnBuilder<bool>& result) {
    if (input.is_optional()) {
      for (size_t i = 0; i < row_num; i++) {
        if (input.has_value(i)) {
          result.push_back_opt(INVERSE ? true : false);
        } else {
          result.push_back_opt(INVERSE ? false : true);
        }
      }
    } else {
      for (size_t i = 0; i < row_num; i++) {
        result.push_back_opt(INVERSE ? true : false);
      }
    }
  }
};
}  // namespace execution
}  // namespace neug