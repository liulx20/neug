#pragma once
#include <string>
#include <vector>

#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class VertexScanSource : public IVecSource {
 public:
	VertexScanSource(std::vector<label_t> labels, int vertex_tag);

	std::string GetName() const override { return "VertexScanSource"; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(
	    const VecExecContext& ctx) const override;
	std::unique_ptr<LocalSourceState> GetLocalSourceState(
	    GlobalSourceState& gstate) const override;

	SourceResultType GetData(GraphDataChunk& chunk, GlobalSourceState& gstate,
	                         LocalSourceState& lstate) override;

 private:
	std::vector<label_t> labels_;
	int vertex_tag_;
};

}  // namespace neug::execution::vec
