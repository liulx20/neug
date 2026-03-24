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
#include "neug/execution/expression/operations/expr_executors.h"
#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
namespace execution {

struct AddOperator {
  template <typename TA, typename TB, typename TR>
  static inline TR Operation(TA left, TB right) {
    return left + right;
  }
};

struct SubtractOperator {
  template <typename TA, typename TB, typename TR>
  static inline TR Operation(TA left, TB right) {
    return left - right;
  }
};

struct MultiplyOperator {
  template <typename TA, typename TB, typename TR>
  static inline TR Operation(TA left, TB right) {
    return left * right;
  }
};

struct DivideOperator {
  template <typename TA, typename TB, typename TR>
  static inline TR Operation(TA left, TB right) {
    return left / right;
  }
};

struct ModuloOperator {
  template <typename TA, typename TB, typename TR>
  static inline TR Operation(TA left, TB right) {
    return left % right;
  }
};

template <>
float ModuloOperator::Operation(float left, float right) {
  return std::fmod(left, right);
}

template <>
double ModuloOperator::Operation(double left, double right) {
  return std::fmod(left, right);
}

struct ArithmeticDispatcher {
  template <typename TR>
  static void execute(const IContextColumn& left, const IContextColumn& right,
                      size_t row_num, const ::common::Arithmetic& logic,
                      ValueColumnBuilder<TR>& result) {
    if constexpr (std::is_same_v<TR, interval_t>) {
      if (left.elem_type().id() == DataTypeId::kTimestampMs &&
          right.elem_type().id() == DataTypeId::kTimestampMs &&
          logic == ::common::Arithmetic::SUB) {
        dispatch_constant<timestamp_ms_t, timestamp_ms_t, interval_t,
                          SubtractOperator>(left, right, row_num, logic,
                                            result);
      } else if (left.elem_type().id() == DataTypeId::kDate &&
                 right.elem_type().id() == DataTypeId::kDate &&
                 logic == ::common::Arithmetic::SUB) {
        dispatch_constant<date_t, date_t, interval_t, SubtractOperator>(
            left, right, row_num, logic, result);
      } else {
        LOG(FATAL)
            << "Unsupported data type combination for interval arithmetic: "
            << left.elem_type().ToString() << " and "
            << right.elem_type().ToString() << " with operation "
            << static_cast<int>(logic);
      }
    } else {
      if (left.elem_type() == right.elem_type()) {
        switch (left.elem_type().id()) {
#define TYPE_DISPATCHER(enum_val, cpp_type)                                    \
  case DataTypeId::enum_val:                                                   \
    if constexpr (std::is_same_v<cpp_type, TR>) {                              \
      dispatch_arithmetic<cpp_type, cpp_type, TR>(left, right, row_num, logic, \
                                                  result);                     \
    }                                                                          \
    break;
          FOR_EACH_NUMERIC_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
        default:
          LOG(FATAL) << "Unsupported data type for arithmetic operation: "
                     << left.elem_type().ToString();
        }
      } else {
        LOG(FATAL) << "Mismatched data types for arithmetic operation: "
                   << left.elem_type().ToString() << " and "
                   << right.elem_type().ToString();
      }
    }
  }

 private:
  template <typename TA, typename TB, typename TR>
  static void dispatch_arithmetic(const IContextColumn& left,
                                  const IContextColumn& right, size_t row_num,
                                  const ::common::Arithmetic& logic,
                                  ValueColumnBuilder<TR>& result) {
    switch (logic) {
    case ::common::Arithmetic::ADD:
      dispatch_constant<TA, TB, TR, AddOperator>(left, right, row_num, logic,
                                                 result);
      break;
    case ::common::Arithmetic::SUB:
      dispatch_constant<TA, TB, TR, SubtractOperator>(left, right, row_num,
                                                      logic, result);

      break;
    case ::common::Arithmetic::MUL:
      dispatch_constant<TA, TB, TR, MultiplyOperator>(left, right, row_num,
                                                      logic, result);
      break;
    case ::common::Arithmetic::DIV:
      dispatch_constant<TA, TB, TR, DivideOperator>(left, right, row_num, logic,
                                                    result);
      break;
    case ::common::Arithmetic::MOD:
      dispatch_constant<TA, TB, TR, ModuloOperator>(left, right, row_num, logic,
                                                    result);
      break;
    default:
      LOG(FATAL) << "Unsupported arithmetic operation: "
                 << static_cast<int>(logic);
    }
  }

  template <typename TA, typename TB, typename TR, typename OP>
  static void dispatch_constant(const IContextColumn& left,
                                const IContextColumn& right, size_t row_num,
                                const ::common::Arithmetic& logic,
                                ValueColumnBuilder<TR>& result) {
    if (left.is_constant() && right.is_constant()) {
      BinaryExecutor<TA, TB, TR, BinaryStandardOperatorWrapper, OP, true,
                     true>::evaluate(left, right, row_num, result);
    } else if (left.is_constant()) {
      BinaryExecutor<TA, TB, TR, BinaryStandardOperatorWrapper, OP, true,
                     false>::evaluate(left, right, row_num, result);
    } else if (right.is_constant()) {
      BinaryExecutor<TA, TB, TR, BinaryStandardOperatorWrapper, OP, false,
                     true>::evaluate(left, right, row_num, result);
    } else {
      BinaryExecutor<TA, TB, TR, BinaryStandardOperatorWrapper, OP, false,
                     false>::evaluate(left, right, row_num, result);
    }
  }
};
}  // namespace execution
}  // namespace neug