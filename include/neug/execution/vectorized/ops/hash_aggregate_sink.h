#pragma once
#include <memory>
#include <vector>

#include "neug/execution/vectorized/aggregate/aggregate_hash_table.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class HashAggregateSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		AggregateHashTable hash_table;
	};

	HashAggregateSink(RowLayout key_layout, std::vector<int> key_tags,
	                  std::vector<AggFuncDef> agg_funcs)
	    : key_layout_(std::move(key_layout)),
	      key_tags_(std::move(key_tags)),
	      agg_funcs_(std::move(agg_funcs)) {}

	std::string GetName() const override { return "HashAggregateSink"; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override {
		auto state = std::make_unique<State>();
		state->hash_table.Initialize(key_layout_, key_tags_, agg_funcs_);
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
			state.hash_table.AddChunk(chunk, chunk.size());
		}
		return SinkResultType::kNeedMoreInput;
	}

 private:
	RowLayout key_layout_;
	std::vector<int> key_tags_;
	std::vector<AggFuncDef> agg_funcs_;
};

}  // namespace neug::execution::vec
