#pragma once
#include "neug/execution/vectorized/pipeline/compiled_vec_pipeline.h"
#include "neug/execution/vectorized/pipeline/vec_pipeline.h"

namespace neug::execution::vec {

class PipelineExecutor {
 public:
	void Execute(VecPipeline& pipeline);

	void Execute(CompiledVecPipeline& pipeline,
	             IVecSink* result_sink,
	             const VecExecContext& ctx);
};

}  // namespace neug::execution::vec
