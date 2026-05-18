#pragma once
#include <memory>
#include <string>

#include "neug/execution/vectorized/aggregate/aggregate_hash_table.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class HashAggregateSource : public IVecSource {
 public:
	explicit HashAggregateSource(AggregateHashTable* hash_table)
	    : hash_table_(hash_table) {}

	std::string GetName() const override { return "HashAggregateSource"; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(
	    const VecExecContext& ctx) const override;
	std::unique_ptr<LocalSourceState> GetLocalSourceState(
	    GlobalSourceState& gstate) const override;

	SourceResultType GetData(GraphDataChunk& chunk, GlobalSourceState& gstate,
	                         LocalSourceState& lstate) override;

 private:
	AggregateHashTable* hash_table_;
};

}  // namespace neug::execution::vec
