#pragma once
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class SortSource : public IVecSource {
 public:
	SortSource(TupleDataCollection* data, std::vector<uint32_t>* sorted_indices)
	    : data_(data), sorted_indices_(sorted_indices) {}

	std::string GetName() const override { return "SortSource"; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(
	    const VecExecContext& ctx) const override;
	std::unique_ptr<LocalSourceState> GetLocalSourceState(
	    GlobalSourceState& gstate) const override;

	SourceResultType GetData(GraphDataChunk& chunk, GlobalSourceState& gstate,
	                         LocalSourceState& lstate) override;

 private:
	TupleDataCollection* data_;
	std::vector<uint32_t>* sorted_indices_;
};

}  // namespace neug::execution::vec
