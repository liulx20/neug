#include "neug/execution/vectorized/ops/materialize_source.h"

#include <cassert>
#include <numeric>

#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/expression/vec_expression.h"

namespace neug::execution::vec {

struct MaterializeGlobalState : public GlobalSourceState {
	size_t total_rows = 0;
};

struct MaterializeLocalState : public LocalSourceState {
	size_t offset = 0;
};

std::unique_ptr<GlobalSourceState> MaterializeSource::GetGlobalSourceState(
    const VecExecContext&) const {
	auto state = std::make_unique<MaterializeGlobalState>();
	state->total_rows = total_rows_;
	return state;
}

std::unique_ptr<LocalSourceState> MaterializeSource::GetLocalSourceState(
    GlobalSourceState&) const {
	return std::make_unique<MaterializeLocalState>();
}

SourceResultType MaterializeSource::GetData(GraphDataChunk& chunk,
                                            GlobalSourceState& gstate,
                                            LocalSourceState& lstate) {
	assert(data_ != nullptr);
	auto& gs = static_cast<MaterializeGlobalState&>(gstate);
	auto& ls = static_cast<MaterializeLocalState&>(lstate);

	if (ls.offset >= gs.total_rows) {
		chunk.SetCardinality(0);
		return SourceResultType::kFinished;
	}

	size_t remaining = gs.total_rows - ls.offset;
	size_t batch = std::min(remaining,
	                        static_cast<size_t>(STANDARD_VECTOR_SIZE));

	std::vector<uint32_t> indices(batch);
	std::iota(indices.begin(), indices.end(),
	          static_cast<uint32_t>(ls.offset));

	data_->Gather(indices.data(), batch, chunk);
	ls.offset += batch;

	return ls.offset >= gs.total_rows ? SourceResultType::kFinished
	                                  : SourceResultType::kHaveMoreOutput;
}

}  // namespace neug::execution::vec
