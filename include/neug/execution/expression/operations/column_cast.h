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

namespace neug {
namespace execution {

struct ColumnCast {
  template <typename INPUT_TYPE, typename RESULT_TYPE>
  static inline RESULT_TYPE Operation(INPUT_TYPE input) {
    RESULT_TYPE res;
    (void) ValueConverter<RESULT_TYPE>::cast(input, res);
    return res;
  }
};

struct ColumnCastDispatcher {
  template <typename RESULT_TYPE>
  static void evaluate(const IContextColumn& input, size_t row_num,
                       ValueColumnBuilder<RESULT_TYPE>& result) {
    if constexpr (std::is_same_v<RESULT_TYPE, int64_t>) {
      if (input.elem_type().id() == DataTypeId::kDate) {
        dispatch<Date, RESULT_TYPE>(input, row_num, result);
        return;
      } else if (input.elem_type().id() == DataTypeId::kTimestampMs) {
        dispatch<DateTime, RESULT_TYPE>(input, row_num, result);
        return;
      } else if (input.elem_type().id() == DataTypeId::kInterval) {
        dispatch<Interval, RESULT_TYPE>(input, row_num, result);
        return;
      }
    }
    switch (input.elem_type().id()) {
#define TYPE_DISPATCHER(type_enum, cpp_type)                 \
  case DataTypeId::type_enum:                                \
    dispatch<cpp_type, RESULT_TYPE>(input, row_num, result); \
    break;
      FOR_EACH_DATA_TYPE_PRIMITIVE(TYPE_DISPATCHER)
      TYPE_DISPATCHER(kVarchar, std::string)
#undef TYPE_DISPATCHER
    default: {
      LOG(FATAL) << "Unsupported input type for ColumnCast: "
                 << input.elem_type().ToString();
    }
    }
  }

 private:
  template <typename INPUT_TYPE, typename RESULT_TYPE>
  static void dispatch(const IContextColumn& input, size_t row_num,
                       ValueColumnBuilder<RESULT_TYPE>& result) {
    if (input.is_constant()) {
      UnaryExecutor<INPUT_TYPE, RESULT_TYPE, UnaryStandardOperatorWrapper,
                    ColumnCast, true>::evaluate(input, row_num, result);

    } else {
      UnaryExecutor<INPUT_TYPE, RESULT_TYPE, UnaryStandardOperatorWrapper,
                    ColumnCast, false>::evaluate(input, row_num, result);
    }
  }
};
}  // namespace execution
}  // namespace neug