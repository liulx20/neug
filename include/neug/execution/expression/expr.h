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
#include <memory>
#include "neug/common/types.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {

class BindedExprBase;

enum class VarType {
  kVertex,
  kEdge,
  kRecord,
};

class VertexExprBase;
class EdgeExprBase;
class RecordExprBase;
class ExprBase {
 public:
  virtual ~ExprBase() = default;
  virtual const DataType& type() const = 0;
  virtual std::unique_ptr<BindedExprBase> bind(
      const IStorageInterface* storage, const ParamsMap& params) const = 0;
  virtual std::string name() const { return "unnamed_expr"; }

  // Attempt JIT compilation for expressions.
  // Falls back to interpreter-based bind() if JIT is not available or fails.
  // The var_type parameter determines which eval path to compile:
  //   kRecord -> record eval, kVertex -> vertex eval, kEdge -> edge eval.
  std::unique_ptr<BindedExprBase> jit_bind(
      const IStorageInterface* storage, const ParamsMap& params,
      VarType var_type = VarType::kRecord) const;

  // Pre-compile the expression tree into a JIT template without binding.
  // The returned template can be bound later with different storage/params
  // via jit_bind_with_template(), avoiding redundant JIT compilation.
  // Returns nullptr if JIT is not available or compilation fails.
  std::shared_ptr<void> jit_compile(VarType var_type = VarType::kRecord) const;

  // Bind a pre-compiled JIT template with storage/params.
  // The template should be obtained from jit_compile().
  // Falls back to interpreter-based bind() if template is null.
  std::unique_ptr<BindedExprBase> jit_bind_with_template(
      const std::shared_ptr<void>& jit_template,
      const IStorageInterface* storage, const ParamsMap& params) const;
};

class BindedExprBase {
 public:
  virtual ~BindedExprBase() = default;
  virtual const DataType& type() const = 0;

  template <typename TARGET>
  TARGET& Cast() {
    if constexpr (std::is_same_v<TARGET, VertexExprBase>) {
      assert(vertex_ptr_ != nullptr);
      return *vertex_ptr_;
    } else if constexpr (std::is_same_v<TARGET, EdgeExprBase>) {
      assert(edge_ptr_ != nullptr);
      return *edge_ptr_;
    } else if constexpr (std::is_same_v<TARGET, RecordExprBase>) {
      assert(record_ptr_ != nullptr);
      return *record_ptr_;
    } else {
      static_assert(sizeof(TARGET) == 0, "Unsupported cast type");
    }
  }

  template <typename TARGET>
  const TARGET& Cast() const {
    if constexpr (std::is_same_v<TARGET, VertexExprBase>) {
      assert(vertex_ptr_ != nullptr);
      return *vertex_ptr_;
    } else if constexpr (std::is_same_v<TARGET, EdgeExprBase>) {
      assert(edge_ptr_ != nullptr);
      return *edge_ptr_;
    } else if constexpr (std::is_same_v<TARGET, RecordExprBase>) {
      assert(record_ptr_ != nullptr);
      return *record_ptr_;
    } else {
      static_assert(sizeof(TARGET) == 0, "Unsupported cast type");
    }
  }

 protected:
  VertexExprBase* vertex_ptr_;
  EdgeExprBase* edge_ptr_;
  RecordExprBase* record_ptr_;
};

class VertexExprBase : public virtual BindedExprBase {
 public:
  VertexExprBase() { vertex_ptr_ = this; }
  virtual ~VertexExprBase() = default;
  virtual Value eval_vertex(label_t v_label, vid_t v_id) const = 0;

  // Typed eval: write the primitive value directly into out_value,
  // avoiding Value construction overhead. Returns true if null.
  // Default implementation falls back to eval_vertex() + Value extraction.
  virtual bool typed_eval_vertex(label_t v_label, vid_t v_id,
                                 void* out_value) const;
};

class EdgeExprBase : public virtual BindedExprBase {
 public:
  EdgeExprBase() { edge_ptr_ = this; }
  virtual ~EdgeExprBase() = default;
  virtual Value eval_edge(const LabelTriplet&, vid_t src, vid_t dst,
                          const void*) const = 0;

  // Typed eval: write the primitive value directly into out_value,
  // avoiding Value construction overhead. Returns true if null.
  virtual bool typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                               const void* edata, void* out_value) const;
};

class RecordExprBase : public virtual BindedExprBase {
 public:
  RecordExprBase() { record_ptr_ = this; }
  virtual ~RecordExprBase() = default;
  virtual Value eval_record(const Context& ctx, size_t idx) const = 0;

  // Typed eval: write the primitive value directly into out_value,
  // avoiding Value construction overhead. Returns true if null.
  virtual bool typed_eval_record(const Context& ctx, size_t idx,
                                 void* out_value) const;
};

std::unique_ptr<ExprBase> parse_expression(const ::common::Expression& expr,
                                           const ContextMeta& ctx_meta,
                                           VarType var_type);

}  // namespace execution
}  // namespace neug