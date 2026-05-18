#include "neug/execution/vectorized/ops/sort_source.h"

#include "neug/execution/vectorized/core/constants.h"

namespace neug::execution::vec {

struct SortSourceGlobalState : public GlobalSourceState {
	size_t total_rows = 0;
};

struct SortSourceLocalState : public LocalSourceState {
	size_t offset = 0;
};

std::unique_ptr<GlobalSourceState> SortSource::GetGlobalSourceState(
    const VecExecContext&) const {
	auto state = std::make_unique<SortSourceGlobalState>();
	state->total_rows = sorted_indices_->size();
	return state;
}

std::unique_ptr<LocalSourceState> SortSource::GetLocalSourceState(
    GlobalSourceState&) const {
	return std::make_unique<SortSourceLocalState>();
}

SourceResultType SortSource::GetData(GraphDataChunk& chunk,
                                     GlobalSourceState& gstate,
                                     LocalSourceState& lstate) {
	auto& gs = static_cast<SortSourceGlobalState&>(gstate);
	auto& ls = static_cast<SortSourceLocalState&>(lstate);

	if (ls.offset >= gs.total_rows) {
		chunk.SetCardinality(0);
		return SourceResultType::kFinished;
	}

	size_t remaining = gs.total_rows - ls.offset;
	size_t batch = std::min(remaining, static_cast<size_t>(STANDARD_VECTOR_SIZE));

	data_->Gather(sorted_indices_->data() + ls.offset, batch, chunk);
	ls.offset += batch;

	return ls.offset >= gs.total_rows ? SourceResultType::kFinished
	                                  : SourceResultType::kHaveMoreOutput;
}

}  // namespace neug::execution::vec
