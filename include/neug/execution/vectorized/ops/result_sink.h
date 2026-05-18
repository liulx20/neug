#pragma once
#include <vector>

#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class ResultSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		std::vector<GraphDataChunk> chunks;
		size_t total_rows = 0;
	};

	std::string GetName() const override { return "ResultSink"; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override {
		return std::make_unique<State>();
	}

	std::unique_ptr<LocalSinkState> GetLocalSinkState(
	    GlobalSinkState&) const override {
		return std::make_unique<LocalSinkState>();
	}

	SinkResultType Sink(GraphDataChunk& chunk, GlobalSinkState& gstate,
	                    LocalSinkState&) override {
		auto& state = static_cast<State&>(gstate);
		state.total_rows += chunk.size();
		state.chunks.push_back(std::move(chunk));
		return SinkResultType::kNeedMoreInput;
	}

	static const std::vector<GraphDataChunk>& GetChunks(
	    const GlobalSinkState& gstate) {
		return static_cast<const State&>(gstate).chunks;
	}

	static size_t GetTotalRows(const GlobalSinkState& gstate) {
		return static_cast<const State&>(gstate).total_rows;
	}
};

}  // namespace neug::execution::vec
