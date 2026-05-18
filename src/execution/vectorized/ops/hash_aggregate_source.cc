#include "neug/execution/vectorized/ops/hash_aggregate_source.h"

#include "neug/execution/vectorized/core/constants.h"

namespace neug::execution::vec {

struct AggSourceGlobalState : public GlobalSourceState {
	size_t total_groups = 0;
};

struct AggSourceLocalState : public LocalSourceState {
	size_t offset = 0;
};

std::unique_ptr<GlobalSourceState>
HashAggregateSource::GetGlobalSourceState(const VecExecContext&) const {
	auto state = std::make_unique<AggSourceGlobalState>();
	state->total_groups = hash_table_->NumGroups();
	return state;
}

std::unique_ptr<LocalSourceState> HashAggregateSource::GetLocalSourceState(
    GlobalSourceState&) const {
	return std::make_unique<AggSourceLocalState>();
}

SourceResultType HashAggregateSource::GetData(GraphDataChunk& chunk,
                                              GlobalSourceState& gstate,
                                              LocalSourceState& lstate) {
	auto& gs = static_cast<AggSourceGlobalState&>(gstate);
	auto& ls = static_cast<AggSourceLocalState&>(lstate);

	if (ls.offset >= gs.total_groups) {
		chunk.SetCardinality(0);
		return SourceResultType::kFinished;
	}

	size_t remaining = gs.total_groups - ls.offset;
	size_t batch = std::min(remaining, static_cast<size_t>(STANDARD_VECTOR_SIZE));

	hash_table_->Scan(ls.offset, batch, chunk);
	ls.offset += batch;

	return ls.offset >= gs.total_groups ? SourceResultType::kFinished
	                                    : SourceResultType::kHaveMoreOutput;
}

}  // namespace neug::execution::vec
