#pragma once
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/vectorized/compiler/vec_pipeline_compiler.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"
#include "neug/generated/proto/response/response.pb.h"

namespace neug::execution::vec {

class VecResultCollector : public IVecSink {
 public:
	VecResultCollector(std::vector<int> output_tags,
	                   std::vector<DataType> output_types);

	std::string GetName() const override { return "VecResultCollector"; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override;
	std::unique_ptr<LocalSinkState> GetLocalSinkState(
	    GlobalSinkState&) const override;

	SinkResultType Sink(GraphDataChunk& chunk, GlobalSinkState& gstate,
	                    LocalSinkState& lstate) override;

	void SerializeToResponse(neug::QueryResponse* response) const;

	size_t TotalRows() const { return total_rows_; }

 private:
	std::vector<int> output_tags_;
	std::vector<DataType> output_types_;
	std::vector<GraphDataChunk> chunks_;
	size_t total_rows_ = 0;
};

}  // namespace neug::execution::vec
