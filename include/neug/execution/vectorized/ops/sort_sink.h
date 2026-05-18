#pragma once
#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"
#include "neug/execution/vectorized/sort/sort_state.h"

namespace neug::execution::vec {

class SortSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		TupleDataCollection data;
		std::vector<uint32_t> sorted_indices;
	};

	SortSink(RowLayout layout, std::vector<SortKeyDef> keys)
	    : layout_(std::move(layout)), keys_(std::move(keys)) {}

	std::string GetName() const override { return "SortSink"; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override {
		auto state = std::make_unique<State>();
		state->data.Initialize(layout_);
		return state;
	}

	std::unique_ptr<LocalSinkState> GetLocalSinkState(
	    GlobalSinkState&) const override {
		return std::make_unique<LocalSinkState>();
	}

	SinkResultType Sink(GraphDataChunk& chunk, GlobalSinkState& gstate,
	                    LocalSinkState&) override {
		auto& state = static_cast<State&>(gstate);
		if (chunk.size() > 0) {
			chunk.Flatten();
			state.data.Append(chunk, chunk.size());
		}
		return SinkResultType::kNeedMoreInput;
	}

	void Finalize(GlobalSinkState& gstate) override {
		auto& state = static_cast<State&>(gstate);
		size_t n = state.data.Count();
		state.sorted_indices.resize(n);
		std::iota(state.sorted_indices.begin(), state.sorted_indices.end(), 0);
		SortKeyComparator cmp(state.data, keys_);
		std::sort(state.sorted_indices.begin(), state.sorted_indices.end(), cmp);
	}

 private:
	RowLayout layout_;
	std::vector<SortKeyDef> keys_;
};

}  // namespace neug::execution::vec
