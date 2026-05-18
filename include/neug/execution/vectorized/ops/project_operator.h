#pragma once
#include <memory>
#include <vector>

#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class ProjectOperator : public IVecOperator {
 public:
	explicit ProjectOperator(std::vector<int> keep_tags)
	    : keep_tags_(std::move(keep_tags)), output_tags_(keep_tags_) {}

	ProjectOperator(std::vector<int> keep_tags, std::vector<int> output_tags)
	    : keep_tags_(std::move(keep_tags)),
	      output_tags_(std::move(output_tags)) {}

	std::string GetName() const override { return "ProjectOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override {
		return std::make_unique<OperatorState>();
	}

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState&,
	                           const VecExecContext&) override {
		output.Reset();
		for (size_t i = 0; i < keep_tags_.size(); ++i) {
			int src_tag = keep_tags_[i];
			int dst_tag = (i < output_tags_.size()) ? output_tags_[i] : src_tag;
			int col_idx = input.FindColumnByTag(src_tag);
			if (col_idx >= 0) {
				output.AddColumn(dst_tag, std::move(input.GetVector(col_idx)));
			}
		}
		output.SetCardinality(input.size());
		return OperatorResultType::kNeedMoreInput;
	}

 private:
	std::vector<int> keep_tags_;
	std::vector<int> output_tags_;
};

struct ExprProjection {
	std::unique_ptr<VecExpression> expr;
	int src_tag;
	int dst_tag;
	DataType type;
};

class ExprProjectOperator : public IVecOperator {
 public:
	ExprProjectOperator(std::vector<ExprProjection> projections, bool is_append)
	    : projections_(std::move(projections)), is_append_(is_append) {}

	std::string GetName() const override { return "ExprProjectOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override {
		return std::make_unique<OperatorState>();
	}

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState&,
	                           const VecExecContext& ctx) override {
		output.Reset();
		size_t count = input.size();
		if (count == 0) {
			output = std::move(input);
			return OperatorResultType::kNeedMoreInput;
		}
		input.Flatten();

		// Evaluate all expressions first (they reference input)
		std::vector<GraphVector> expr_results;
		for (auto& proj : projections_) {
			if (proj.expr) {
				GraphVector result{proj.type};
				proj.expr->Evaluate(input, count, result, ctx);
				expr_results.push_back(std::move(result));
			} else {
				expr_results.emplace_back(DataType{DataTypeId::kUnknown});
			}
		}

		// Assemble output
		if (is_append_) {
			for (size_t c = 0; c < input.ColumnCount(); c++) {
				output.AddColumn(input.GetTag(c),
				                 std::move(input.GetVector(c)));
			}
		}

		for (size_t i = 0; i < projections_.size(); i++) {
			auto& proj = projections_[i];
			if (proj.expr) {
				output.AddColumn(proj.dst_tag, std::move(expr_results[i]));
			} else {
				int col_idx = input.FindColumnByTag(proj.src_tag);
				if (col_idx >= 0) {
					output.AddColumn(proj.dst_tag,
					                 std::move(input.GetVector(col_idx)));
				}
			}
		}

		output.SetCardinality(count);
		return OperatorResultType::kNeedMoreInput;
	}

 private:
	std::vector<ExprProjection> projections_;
	bool is_append_;
};

}  // namespace neug::execution::vec
