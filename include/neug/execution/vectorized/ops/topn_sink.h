#pragma once
#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"
#include "neug/execution/vectorized/sort/sort_state.h"

namespace neug::execution::vec {

class TopNSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		TupleDataCollection data;
		std::vector<uint32_t> heap;
		size_t limit;
	};

	TopNSink(RowLayout layout, std::vector<SortKeyDef> keys, size_t limit)
	    : layout_(std::move(layout)),
	      keys_(std::move(keys)),
	      limit_(limit) {}

	std::string GetName() const override { return "TopNSink"; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override {
		auto state = std::make_unique<State>();
		state->data.Initialize(layout_);
		state->limit = limit_;
		return state;
	}

	std::unique_ptr<LocalSinkState> GetLocalSinkState(
	    GlobalSinkState&) const override {
		return std::make_unique<LocalSinkState>();
	}

	SinkResultType Sink(GraphDataChunk& chunk, GlobalSinkState& gstate,
	                    LocalSinkState&) override {
		auto& state = static_cast<State&>(gstate);
		if (chunk.size() == 0) return SinkResultType::kNeedMoreInput;

		chunk.Flatten();
		size_t count = chunk.size();

		// Append all rows to data collection
		size_t base = state.data.Count();
		state.data.Append(chunk, count);

		// Use a max-heap: the "worst" element sits at top.
		// Comparator for max-heap: returns true if lhs is "better" (should be
		// below rhs in heap). std::push_heap/pop_heap use a max-heap where the
		// comparator defines "less than" — the largest element goes to top.
		// We want the "worst" element at top so we can evict it.
		// "worse" = would come AFTER in final sorted order.
		// So heap comparator: lhs < rhs in final order means lhs is "better"
		// → lhs should sink below rhs → return true (lhs < rhs for max-heap).
		SortKeyComparator sort_cmp(state.data, keys_);

		for (size_t i = 0; i < count; i++) {
			uint32_t row_idx = static_cast<uint32_t>(base + i);
			if (state.heap.size() < state.limit) {
				state.heap.push_back(row_idx);
				std::push_heap(state.heap.begin(), state.heap.end(), sort_cmp);
			} else if (!state.heap.empty()) {
				// Compare new row with heap top (the "worst" kept element)
				// If new row is better (sorts before heap top), replace
				if (sort_cmp(row_idx, state.heap.front())) {
					std::pop_heap(state.heap.begin(), state.heap.end(),
					              sort_cmp);
					state.heap.back() = row_idx;
					std::push_heap(state.heap.begin(), state.heap.end(),
					               sort_cmp);
				}
			}
		}

		return SinkResultType::kNeedMoreInput;
	}

	void Finalize(GlobalSinkState& gstate) override {
		auto& state = static_cast<State&>(gstate);
		SortKeyComparator cmp(state.data, keys_);
		std::sort(state.heap.begin(), state.heap.end(), cmp);
	}

 private:
	RowLayout layout_;
	std::vector<SortKeyDef> keys_;
	size_t limit_;
};

}  // namespace neug::execution::vec
