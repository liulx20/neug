#pragma once
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/vectorized/pipeline/compiled_vec_pipeline.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

struct VecExecContext;

struct UnionSubPipeline {
	std::unique_ptr<IVecSource> source;
	std::vector<std::unique_ptr<IVecOperator>> operators;

	// For multi-stage sub-plans (e.g., with OrderBy/GroupBy)
	std::unique_ptr<CompiledVecPipeline> full_pipeline;
};

class UnionSource : public IVecSource {
 public:
	UnionSource(std::vector<std::unique_ptr<UnionSubPipeline>> sub_pipelines);

	std::string GetName() const override { return "UnionSource"; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(
	    const VecExecContext& ctx) const override;
	std::unique_ptr<LocalSourceState> GetLocalSourceState(
	    GlobalSourceState& gstate) const override;

	SourceResultType GetData(GraphDataChunk& chunk, GlobalSourceState& gstate,
	                         LocalSourceState& lstate) override;

 private:
	std::vector<std::unique_ptr<UnionSubPipeline>> sub_pipelines_;
	const VecExecContext* ctx_ = nullptr;
};

}  // namespace neug::execution::vec
