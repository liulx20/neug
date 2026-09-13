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

#include "neug/execution/execute/ops/retrieve/scan.h"

#include <unordered_set>

#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/execution/common/operators/retrieve/scan.h"
#include "neug/execution/execute/ops/retrieve/scan_utils.h"
#include "neug/execution/expression/predicates.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace execution {
class OprTimer;

namespace ops {

static std::vector<Value> deduplicate_ids(std::vector<Value> values) {
  auto hash = [](const Value& value) {
    switch (value.type().id()) {
    case DataTypeId::kInt32:
      return std::hash<int32_t>{}(value.GetValue<int32_t>());
    case DataTypeId::kInt64:
      return std::hash<int64_t>{}(value.GetValue<int64_t>());
    case DataTypeId::kUInt32:
      return std::hash<uint32_t>{}(value.GetValue<uint32_t>());
    case DataTypeId::kUInt64:
      return std::hash<uint64_t>{}(value.GetValue<uint64_t>());
    case DataTypeId::kVarchar:
      return std::hash<std::string>{}(StringValue::Get(value));
    default:
      return size_t{0};
    }
  };
  std::unordered_set<Value, decltype(hash)> seen(0, hash);
  seen.reserve(values.size());
  std::vector<Value> result;
  result.reserve(values.size());
  for (auto& value : values) {
    if (!value.IsNull() && seen.insert(value).second) {
      result.emplace_back(std::move(value));
    }
  }
  return result;
}

class FilterOidsGPredOpr : public IOperator {
 public:
  bool consumes_input() const override { return false; }

  FilterOidsGPredOpr(ScanParams params,
                     const algebra::IndexPredicate_Triplet& oids,
                     std::unique_ptr<neug::execution::ExprBase>&& pred)
      : params_(params), oids_(oids), pred_(std::move(pred)) {}

  Kernel CreateState(IStorageInterface& graph, const ParamsMap& params,
                     neug::execution::OprTimer* timer) override {
    return make_source_kernel([this, &graph, params,
                               timer]() -> result<ContextChunk> {
      ContextChunk chunk;

      const auto& rhs_op = oids_.expression().operators(0);
      if ((rhs_op.has_const_() && rhs_op.const_().has_none()) ||
          (rhs_op.has_param() && params.at(rhs_op.param().name()).IsNull())) {
        static const std::vector<Value> no_oids;
        auto empty_chunk = Scan::filter_oids(std::move(chunk), graph, params_,
                                             DummyPred(), no_oids);
        if (!empty_chunk) {
          return tl::make_unexpected(empty_chunk.error());
        }
        chunk = std::move(*empty_chunk);
        return std::move(chunk);
      }
      std::vector<Value> oid_values = ScanUtils::parse_ids(oids_, params);
      if (oids_.cmp() == common::Logical::WITHIN) {
        oid_values = deduplicate_ids(std::move(oid_values));
      }

      if (pred_ == nullptr) {
        if (params_.tables.size() == 1 && oid_values.size() == 1) {
          {
            return Scan::find_vertex_with_oid(std::move(chunk), graph,
                                              params_.tables[0], oid_values[0],
                                              params_.alias);
          }
        }
        {
          return Scan::filter_oids(std::move(chunk), graph, params_,
                                   DummyPred(), oid_values);
        }
      } else {
        auto pred = pred_->bind(&graph, params);
        GeneralPred predicate_wrapper(std::move(pred));
        {
          return Scan::filter_oids(std::move(chunk), graph, params_,
                                   predicate_wrapper, oid_values);
        }
      }
    });
  }

  std::string get_operator_name() const override {
    return "FilterOidsGPredOpr";
  }

 private:
  ScanParams params_;
  algebra::IndexPredicate_Triplet oids_;
  std::unique_ptr<neug::execution::ExprBase> pred_;
};

// Shared physical ranges, with MVCC checks performed by each local reader.
class VertexMorselSource final : public MorselSource {
 public:
  using ReadRange = std::function<result<ContextChunk>(size_t, size_t, size_t)>;
  using ReaderFactory = std::function<ReadRange()>;
  VertexMorselSource(const IStorageInterface& storage, const ScanParams& params,
                     ReaderFactory factory)
      : factory_(std::move(factory)) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(storage);
    for (auto label : params.tables) {
      sizes_.push_back(graph.GetVertexSet(label).size());
    }
  }
  result<std::optional<Morsel>> Pick() override {
    if (partition_ == sizes_.size()) {
      return std::optional<Morsel>{};
    }
    auto end = std::min(begin_ + size_t{4096}, sizes_[partition_]);
    Morsel work{partition_, begin_, end, nullptr};
    begin_ = end;
    if (end == sizes_[partition_]) {
      ++partition_;
      begin_ = 0;
    }
    return std::optional<Morsel>(work);
  }
  std::unique_ptr<MorselReader> CreateReader() override {
    class Reader final : public MorselReader {
     public:
      explicit Reader(ReadRange read) : read_(std::move(read)) {}
      void Start(const Morsel& work) override {
        work_ = work;
        done_ = false;
      }
      result<std::optional<ContextChunk>> Next() override {
        if (done_) {
          return std::optional<ContextChunk>{};
        }
        auto end = std::min(work_.begin + size_t{1024}, work_.end);
        GS_AUTO(chunk, read_(work_.partition, work_.begin, end));
        work_.begin = end;
        done_ = end == work_.end;
        return std::optional<ContextChunk>(std::move(chunk));
      }

     private:
      ReadRange read_;
      Morsel work_;
      bool done_ = true;
    };
    return std::make_unique<Reader>(factory_());
  }

 private:
  ReaderFactory factory_;
  std::vector<size_t> sizes_;
  size_t partition_ = 0;
  size_t begin_ = 0;
};

class ScanWithSPredOpr : public MorselSourceOperator {
 public:
  bool consumes_input() const override { return false; }

  ScanWithSPredOpr(const ScanParams& scan_params,
                   const SpecialPredicateConfig& config)
      : scan_params_(scan_params), config_(config) {}

  std::string get_operator_name() const override { return "ScanWithSPredOpr"; }

  std::unique_ptr<MorselSource> CreateMorselSource(
      IStorageInterface& graph, const ParamsMap& params) override {
    return std::make_unique<VertexMorselSource>(
        graph, scan_params_, [this, &graph, params] {
          return [this, &graph, params](size_t partition, size_t begin,
                                        size_t end) {
            ScanParams range;
            range.alias = scan_params_.alias;
            range.tables = {scan_params_.tables[partition]};
            return Scan::scan_vertex_with_special_vertex_predicate(
                ContextChunk{}, graph, range, config_, params, begin, end);
          };
        });
  }

 private:
  ScanParams scan_params_;
  SpecialPredicateConfig config_;
};

class ScanWithGPredOpr : public MorselSourceOperator {
 public:
  bool consumes_input() const override { return false; }

  ScanWithGPredOpr(const ScanParams& scan_params,
                   std::unique_ptr<neug::execution::ExprBase> pred)
      : scan_params_(scan_params), pred_(std::move(pred)) {}
  std::unique_ptr<MorselSource> CreateMorselSource(
      IStorageInterface& graph, const ParamsMap& params) override {
    return std::make_unique<VertexMorselSource>(
        graph, scan_params_, [this, &graph, params] {
          auto predicate =
              pred_ ? std::make_shared<GeneralPred>(pred_->bind(&graph, params))
                    : nullptr;
          return [this, &graph, predicate](size_t partition, size_t begin,
                                           size_t end) {
            ScanParams range;
            range.alias = scan_params_.alias;
            range.tables = {scan_params_.tables[partition]};
            if (predicate) {
              return Scan::scan_vertex(ContextChunk{}, graph, range, *predicate,
                                       begin, end);
            }
            return Scan::scan_vertex(ContextChunk{}, graph, range, DummyPred(),
                                     begin, end);
          };
        });
  }
  std::string get_operator_name() const override { return "ScanWithGPredOpr"; }

 private:
  ScanParams scan_params_;
  std::unique_ptr<neug::execution::ExprBase> pred_;
};

neug::result<OpBuildResultT> ScanOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta;
  int alias = -1;
  if (plan.plan(op_idx).opr().scan().has_alias()) {
    alias = plan.plan(op_idx).opr().scan().alias().value();
  }
  ret_meta.set(alias, DataType::VERTEX);
  const auto& scan_opr = plan.plan(op_idx).opr().scan();
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    LOG(ERROR) << "Currently only support scan vertex";
    return std::make_pair(nullptr, ret_meta);
  }
  if (!scan_opr.has_params()) {
    LOG(ERROR) << "Scan operator should have params";
    return std::make_pair(nullptr, ret_meta);
  }

  ScanParams scan_params;
  scan_params.alias = scan_opr.has_alias() ? scan_opr.alias().value() : -1;

  for (const auto& table : scan_opr.params().tables()) {
    scan_params.tables.emplace_back(table.id());
  }

  if (scan_opr.has_idx_predicate()) {
    if (!ScanUtils::check_idx_predicate(scan_opr)) {
      LOG(ERROR) << "Index predicate is not supported"
                 << scan_opr.DebugString();
      return std::make_pair(nullptr, ret_meta);
    }

    // without predicate
    std::unique_ptr<ExprBase> pred = nullptr;
    if (scan_opr.params().has_predicate()) {
      pred = parse_expression(scan_opr.params().predicate(), ctx_meta,
                              VarType::kVertex);
    }

    const algebra::IndexPredicate_Triplet& idxs =
        scan_opr.idx_predicate().or_predicates(0).predicates(0);
    return std::make_pair(std::make_unique<FilterOidsGPredOpr>(
                              scan_params, idxs, std::move(pred)),
                          ret_meta);

  } else {
    if (scan_opr.params().has_predicate()) {
      SpecialPredicateConfig config;
      if (is_special_vertex_predicate(schema, scan_params.tables,
                                      scan_opr.params().predicate(), config)) {
        return std::make_pair(
            std::make_unique<ScanWithSPredOpr>(scan_params, config), ret_meta);
      }
    }
    std::unique_ptr<ExprBase> pred = nullptr;
    if (scan_opr.params().has_predicate()) {
      pred = parse_expression(scan_opr.params().predicate(), ctx_meta,
                              VarType::kVertex);
    }
    return std::make_pair(
        std::make_unique<ScanWithGPredOpr>(scan_params, std::move(pred)),
        ret_meta);
  }
}

class DummySourceOpr : public IOperator {
 public:
  bool consumes_input() const override { return false; }

  DummySourceOpr() {}

  Kernel CreateState(IStorageInterface& graph_interface,
                     const ParamsMap& params,
                     neug::execution::OprTimer* timer) override {
    return make_source_kernel(
        [this, &graph_interface, params, timer]() -> result<ContextChunk> {
          ContextChunk chunk;
          ValueColumnBuilder<int32_t> builder;
          builder.push_back_opt(0);
          chunk.set(-1, builder.finish());

          return std::move(chunk);
        });
  }

  std::string get_operator_name() const override { return "DummySourceOpr"; }
};  // namespace ops

neug::result<OpBuildResultT> DummySourceOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(std::make_unique<DummySourceOpr>(), ctx_meta);
}
}  // namespace ops
}  // namespace execution
}  // namespace neug
