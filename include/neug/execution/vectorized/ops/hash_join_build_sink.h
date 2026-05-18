#pragma once
#include <memory>
#include <vector>

#include "neug/execution/vectorized/join/join_hash_table.h"
#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class HashJoinBuildSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		TupleDataCollection tuples;
		JoinHashTable hash_table;
	};

	HashJoinBuildSink(RowLayout layout, std::vector<int> key_col_indices)
	    : layout_(std::move(layout)),
	      key_col_indices_(std::move(key_col_indices)) {}

	std::string GetName() const override { return "HashJoinBuildSink"; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override {
		auto state = std::make_unique<State>();
		state->tuples.Initialize(layout_);
		state->hash_table.Initialize(layout_, key_col_indices_);
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
			state.tuples.Append(chunk, chunk.size());
		}
		return SinkResultType::kNeedMoreInput;
	}

	void Finalize(GlobalSinkState& gstate) override {
		auto& state = static_cast<State&>(gstate);
		state.hash_table.Build(state.tuples);
	}

 private:
	RowLayout layout_;
	std::vector<int> key_col_indices_;
};

}  // namespace neug::execution::vec
