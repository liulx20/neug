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

#include <cmath>

#include "neug/common/types.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/expression/operations/expr_executors.h"
#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
namespace execution {

struct Equals {
  template <typename T>
  static inline bool Operation(const T& left, const T& right) {
    return left == right;
  }
};

template <>
inline bool Equals::Operation(const double& left, const double& right) {
  return std::abs(left - right) < 1e-9;
}

template <>
inline bool Equals::Operation(const float& left, const float& right) {
  return std::abs(left - right) < 1e-6;
}

struct NotEquals {
  template <typename T>
  static inline bool Operation(const T& left, const T& right) {
    return !Equals::Operation(left, right);
  }
};

struct LessThan {
  template <typename T>
  static inline bool Operation(const T& left, const T& right) {
    return left < right;
  }
};

template <>
inline bool LessThan::Operation(const double& left, const double& right) {
  return left < right && std::abs(left - right) >= 1e-9;
}

template <>
inline bool LessThan::Operation(const float& left, const float& right) {
  return left < right && std::abs(left - right) >= 1e-6;
}

struct GreaterThan {
  template <typename T>
  static inline bool Operation(const T& left, const T& right) {
    return LessThan::Operation(right, left);
  }
};

struct GreaterThanEquals {
  template <typename T>
  static inline bool Operation(const T& left, const T& right) {
    return !LessThan::Operation(left, right);
  }
};

struct LessThanEquals {
  template <typename T>
  static inline bool Operation(const T& left, const T& right) {
    return !GreaterThan::Operation(left, right);
  }
};

struct ComparisonDispatcher {
 public:
  static void execute(const IContextColumn& left, const IContextColumn& right,
                      size_t row_num, const ::common::Logical& logic,
                      ValueColumnBuilder<bool>& result) {
    auto left_type = left.elem_type();
    auto right_type = right.elem_type();
    if (left_type != right_type) {
      LOG(FATAL) << "Type mismatch in comparison: " << left_type.ToString()
                 << " vs " << right_type.ToString();
    }
    switch (left_type.id()) {
#define TYPE_DISPATCHER(enum_val, cpp_type)                           \
  case DataTypeId::enum_val:                                          \
    dispatch_constant<cpp_type>(left, right, row_num, logic, result); \
    break;
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    case DataTypeId::kVarchar:
      dispatch_constant<std::string>(left, right, row_num, logic, result);
      break;
    default:
      LOG(FATAL) << "Unsupported data type for comparison: "
                 << left_type.ToString();
    }
  }

 private:
  template <typename T>
  static void dispatch_constant(const IContextColumn& left,
                                const IContextColumn& right, size_t row_num,
                                const ::common::Logical& logic,
                                ValueColumnBuilder<bool>& result) {
    if (left.is_constant() && right.is_constant()) {
      dispatch_logical<T, true, true>(left, right, row_num, logic, result);
    } else if (left.is_constant()) {
      dispatch_logical<T, true, false>(left, right, row_num, logic, result);
    } else if (right.is_constant()) {
      dispatch_logical<T, false, true>(left, right, row_num, logic, result);
    } else {
      dispatch_logical<T, false, false>(left, right, row_num, logic, result);
    }
  }
  template <typename T, bool LEFT_CONST, bool RIGHT_CONST>
  static void dispatch_logical(const IContextColumn& left,
                               const IContextColumn& right, size_t row_num,
                               const ::common::Logical& logic,
                               ValueColumnBuilder<bool>& result) {
    switch (logic) {
    case ::common::Logical::EQ:
      BinaryExecutor<T, T, bool, BinarySingleArgumentOperatorWrapper, Equals,
                     LEFT_CONST, RIGHT_CONST>::evaluate(left, right, row_num,
                                                        result);
      break;

    case ::common::Logical::NE:
      BinaryExecutor<T, T, bool, BinarySingleArgumentOperatorWrapper, NotEquals,
                     LEFT_CONST, RIGHT_CONST>::evaluate(left, right, row_num,
                                                        result);
      break;

    case ::common::Logical::LT:
      BinaryExecutor<T, T, bool, BinarySingleArgumentOperatorWrapper, LessThan,
                     LEFT_CONST, RIGHT_CONST>::evaluate(left, right, row_num,
                                                        result);
      break;
    case ::common::Logical::GT:
      BinaryExecutor<T, T, bool, BinarySingleArgumentOperatorWrapper,
                     GreaterThan, LEFT_CONST, RIGHT_CONST>::evaluate(left,
                                                                     right,
                                                                     row_num,
                                                                     result);
      break;
    case ::common::Logical::LE:
      BinaryExecutor<T, T, bool, BinarySingleArgumentOperatorWrapper,
                     LessThanEquals, LEFT_CONST, RIGHT_CONST>::evaluate(left,
                                                                        right,
                                                                        row_num,
                                                                        result);
      break;
    case ::common::Logical::GE:
      BinaryExecutor<T, T, bool, BinarySingleArgumentOperatorWrapper,
                     GreaterThanEquals, LEFT_CONST,
                     RIGHT_CONST>::evaluate(left, right, row_num, result);
      break;
    default:
      LOG(FATAL) << "Unsupported comparison operation: "
                 << static_cast<int>(logic);
    }
  }
};
}  // namespace execution
}  // namespace neug