#pragma once
#include <functional>
#include <memory>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

struct VecExecContext;

struct VecOutputInfo {
	std::vector<int> tags;
	std::vector<DataType> types;
};

struct PipelineStage {
	std::unique_ptr<IVecSource> source;
	std::vector<std::unique_ptr<IVecOperator>> operators;
	std::unique_ptr<IVecSink> sink;
};

struct CompiledVecPipeline {
	std::vector<PipelineStage> stages;
	VecOutputInfo output_info;

	struct StageLink {
		using SinkStates =
		    std::vector<std::unique_ptr<GlobalSinkState>>;
		using SourceFactory =
		    std::function<std::unique_ptr<IVecSource>(SinkStates&)>;
		using BindCallback = std::function<void(SinkStates&)>;
		SourceFactory factory;
		BindCallback bind_callback;
	};
	std::vector<StageLink> stage_links;
};

}  // namespace neug::execution::vec
