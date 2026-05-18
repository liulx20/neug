#pragma once
#include <memory>
#include <string>

#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class LimitOperator : public IVecOperator {
 public:
	LimitOperator(size_t offset, size_t limit)
	    : offset_(offset), limit_(limit) {}

	std::string GetName() const override { return "Limit"; }

	std::unique_ptr<OperatorState> GetOperatorState() const override {
		return std::make_unique<LimitState>();
	}

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& ostate,
	                           const VecExecContext&) override {
		auto& state = static_cast<LimitState&>(ostate);

		if (state.emitted >= limit_) {
			output = GraphDataChunk();
			output.SetCardinality(0);
			return OperatorResultType::kFinished;
		}

		size_t count = input.size();
		if (count == 0) {
			output = std::move(input);
			return OperatorResultType::kNeedMoreInput;
		}

		// Skip rows for offset
		size_t start = 0;
		if (state.seen < offset_) {
			size_t to_skip = offset_ - state.seen;
			if (to_skip >= count) {
				state.seen += count;
				output = GraphDataChunk();
				output.SetCardinality(0);
				return OperatorResultType::kNeedMoreInput;
			}
			start = to_skip;
			state.seen += to_skip;
		}

		size_t remaining_in_limit = limit_ - state.emitted;
		size_t available = count - start;
		size_t to_emit = std::min(available, remaining_in_limit);

		if (start == 0 && to_emit == count) {
			output = std::move(input);
		} else {
			// Slice the chunk
			std::vector<sel_t> sel(to_emit);
			for (size_t i = 0; i < to_emit; i++) {
				sel[i] = static_cast<sel_t>(start + i);
			}
			output = GraphDataChunk();
			for (size_t col = 0; col < input.ColumnCount(); col++) {
				int tag = input.GetTag(col);
				const auto& vec = input.GetVector(col);
				auto sliced = GraphVector::Slice(vec, sel.data(), to_emit);
				output.AddColumn(tag, std::move(sliced));
			}
			output.SetCardinality(to_emit);
		}

		state.seen += to_emit;
		state.emitted += to_emit;

		if (state.emitted >= limit_) {
			return OperatorResultType::kFinished;
		}
		return OperatorResultType::kNeedMoreInput;
	}

 private:
	struct LimitState : public OperatorState {
		size_t seen = 0;
		size_t emitted = 0;
	};

	size_t offset_;
	size_t limit_;
};

}  // namespace neug::execution::vec
