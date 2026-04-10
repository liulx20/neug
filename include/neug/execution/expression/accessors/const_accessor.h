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
#include "neug/execution/expression/expr.h"

namespace neug {
namespace execution {
class ConstExpr : public ExprBase,
                  public VertexExprBase,
                  public EdgeExprBase,
                  public RecordExprBase {
 public:
  ConstExpr(const Value& value) : inner_(value) {}
  ~ConstExpr() override = default;

  Value eval_record(const Context&, size_t) const override { return inner_; }

  Value eval_vertex(label_t, vid_t) const override { return inner_; }

  Value eval_edge(const LabelTriplet&, vid_t, vid_t,
                  const void*) const override {
    return inner_;
  }

  bool typed_eval_vertex(label_t, vid_t, void* out_value) const override {
    return extract_inner(out_value);
  }

  bool typed_eval_edge(const LabelTriplet&, vid_t, vid_t, const void*,
                       void* out_value) const override {
    return extract_inner(out_value);
  }

  bool typed_eval_record(const Context&, size_t,
                         void* out_value) const override {
    return extract_inner(out_value);
  }

  const DataType& type() const override { return inner_.type(); }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

  std::string name() const override { return "ConstExpr"; }

 private:
  bool extract_inner(void* out_value) const {
    if (inner_.IsNull()) {
      return true;
    }
    switch (inner_.type().id()) {
    case DataTypeId::kBoolean:
      *static_cast<bool*>(out_value) = inner_.GetValue<bool>();
      return false;
    case DataTypeId::kInt32:
      *static_cast<int32_t*>(out_value) = inner_.GetValue<int32_t>();
      return false;
    case DataTypeId::kInt64:
      *static_cast<int64_t*>(out_value) = inner_.GetValue<int64_t>();
      return false;
    case DataTypeId::kUInt32:
      *static_cast<uint32_t*>(out_value) = inner_.GetValue<uint32_t>();
      return false;
    case DataTypeId::kUInt64:
      *static_cast<uint64_t*>(out_value) = inner_.GetValue<uint64_t>();
      return false;
    case DataTypeId::kFloat:
      *static_cast<float*>(out_value) = inner_.GetValue<float>();
      return false;
    case DataTypeId::kDouble:
      *static_cast<double*>(out_value) = inner_.GetValue<double>();
      return false;
    default:
      return true;
    }
  }

  Value inner_;
};

class ParamExpr : public ExprBase {
 public:
  ParamExpr(const std::string& name, const DataType& type)
      : name_(name), type_(type) {}

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

  const DataType& type() const override { return type_; }

  std::string name() const override { return "ParamExpr"; }

 private:
  std::string name_;
  DataType type_;
};
}  // namespace execution
}  // namespace neug