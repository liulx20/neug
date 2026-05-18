#pragma once
#include <memory>
#include <numeric>
#include <vector>

#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class MaterializeSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		TupleDataCollection data;
		size_t total_rows = 0;
	};

	explicit MaterializeSink(RowLayout layout)
	    : layout_(std::move(layout)) {}

	std::string GetName() const override { return "MaterializeSink"; }

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
			state.total_rows += chunk.size();
		}
		return SinkResultType::kNeedMoreInput;
	}

 private:
	RowLayout layout_;
};

}  // namespace neug::execution::vec
