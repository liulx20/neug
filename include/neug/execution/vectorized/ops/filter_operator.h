#pragma once
#include <functional>

#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

using FilterPredicate =
    std::function<bool(const GraphDataChunk& chunk, size_t row)>;

class FilterOperator : public IVecOperator {
 public:
	explicit FilterOperator(FilterPredicate predicate)
	    : predicate_(std::move(predicate)) {}

	std::string GetName() const override { return "FilterOperator"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override {
		return std::make_unique<OperatorState>();
	}

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState&,
	                           const VecExecContext&) override {
		size_t count = input.size();
		if (count == 0) {
			output.SetCardinality(0);
			return OperatorResultType::kNeedMoreInput;
		}

		SelectionVector sel(count);
		size_t selected = 0;
		for (size_t i = 0; i < count; i++) {
			if (predicate_(input, i)) {
				sel.SetIndex(selected++, static_cast<sel_t>(i));
			}
		}

		if (selected == 0) {
			output = std::move(input);
			output.SetCardinality(0);
			return OperatorResultType::kNeedMoreInput;
		}

		if (selected == count) {
			output = std::move(input);
		} else {
			input.Compact(sel, selected);
			output = std::move(input);
		}
		return OperatorResultType::kNeedMoreInput;
	}

 private:
	FilterPredicate predicate_;
};

}  // namespace neug::execution::vec
