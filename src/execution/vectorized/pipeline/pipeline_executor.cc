#include "neug/execution/vectorized/pipeline/pipeline_executor.h"

#include <cassert>

#include "neug/execution/vectorized/expression/vec_expression.h"

namespace neug::execution::vec {

void PipelineExecutor::Execute(VecPipeline& pipeline) {
	assert(pipeline.source != nullptr);
	assert(pipeline.sink != nullptr);

	static const VecExecContext empty_ctx{};
	const VecExecContext& ctx = pipeline.ctx ? *pipeline.ctx : empty_ctx;

	auto global_source_state = pipeline.source->GetGlobalSourceState(ctx);
	auto local_source_state =
	    pipeline.source->GetLocalSourceState(*global_source_state);
	auto global_sink_state = pipeline.sink->GetGlobalSinkState();
	auto local_sink_state =
	    pipeline.sink->GetLocalSinkState(*global_sink_state);

	std::vector<std::unique_ptr<OperatorState>> op_states;
	op_states.reserve(pipeline.operators.size());
	for (auto* op : pipeline.operators) {
		op_states.push_back(op->GetOperatorState());
	}

	GraphDataChunk source_chunk;
	GraphDataChunk buf_a, buf_b;

	while (true) {
		source_chunk.Reset();
		auto source_result = pipeline.source->GetData(
		    source_chunk, *global_source_state, *local_source_state);

		if (source_chunk.size() == 0) {
			break;
		}

		GraphDataChunk* input = &source_chunk;
		bool have_more = true;

		while (have_more) {
			have_more = false;
			GraphDataChunk* current = input;

			for (size_t i = 0; i < pipeline.operators.size(); i++) {
				GraphDataChunk* output = (current == &buf_a) ? &buf_b : &buf_a;
				output->Reset();

				auto result = pipeline.operators[i]->Execute(
				    *current, *output, *op_states[i], ctx);

				if (result == OperatorResultType::kHaveMoreOutput) {
					have_more = true;
				}

				current = output;
			}

			if (current->size() > 0) {
				pipeline.sink->Sink(
				    *current, *global_sink_state, *local_sink_state);
			}
		}

		if (source_result == SourceResultType::kFinished) {
			break;
		}
	}

	pipeline.sink->Combine(*global_sink_state, *local_sink_state);
	pipeline.sink->Finalize(*global_sink_state);
}

void PipelineExecutor::Execute(CompiledVecPipeline& pipeline,
                               IVecSink* result_sink,
                               const VecExecContext& ctx) {
	assert(!pipeline.stages.empty());

	std::vector<std::unique_ptr<GlobalSinkState>> prev_sink_states;

	for (size_t si = 0; si < pipeline.stages.size(); si++) {
		auto& stage = pipeline.stages[si];

		// Apply stage link from previous stage
		IVecSource* source = stage.source.get();
		std::unique_ptr<IVecSource> runtime_source;
		if (si > 0 && si - 1 < pipeline.stage_links.size()) {
			auto& link = pipeline.stage_links[si - 1];
			if (link.factory) {
				runtime_source = link.factory(prev_sink_states);
				source = runtime_source.get();
			}
			if (link.bind_callback) {
				link.bind_callback(prev_sink_states);
			}
		}

		assert(source != nullptr);

		bool is_last = (si == pipeline.stages.size() - 1);
		IVecSink* sink = is_last ? result_sink : stage.sink.get();
		assert(sink != nullptr);

		// Execute this stage inline to capture sink state
		auto global_source_state = source->GetGlobalSourceState(ctx);
		auto local_source_state =
		    source->GetLocalSourceState(*global_source_state);
		auto global_sink_state = sink->GetGlobalSinkState();
		auto local_sink_state =
		    sink->GetLocalSinkState(*global_sink_state);

		std::vector<std::unique_ptr<OperatorState>> op_states;
		for (auto& op : stage.operators) {
			op_states.push_back(op->GetOperatorState());
		}

		GraphDataChunk source_chunk;
		GraphDataChunk buf_a, buf_b;

		while (true) {
			source_chunk.Reset();
			auto source_result = source->GetData(
			    source_chunk, *global_source_state, *local_source_state);

			if (source_chunk.size() == 0) {
				if (source_result == SourceResultType::kFinished) break;
				continue;
			}

			GraphDataChunk* input = &source_chunk;
			bool have_more = true;

			while (have_more) {
				have_more = false;
				GraphDataChunk* current = input;

				for (size_t i = 0; i < stage.operators.size(); i++) {
					GraphDataChunk* output =
					    (current == &buf_a) ? &buf_b : &buf_a;
					output->Reset();

					auto result = stage.operators[i]->Execute(
					    *current, *output, *op_states[i], ctx);

					if (result == OperatorResultType::kHaveMoreOutput) {
						have_more = true;
					}
					current = output;
				}

				if (current->size() > 0) {
					sink->Sink(*current, *global_sink_state,
					           *local_sink_state);
				}
			}

			if (source_result == SourceResultType::kFinished) break;
		}

		sink->Combine(*global_sink_state, *local_sink_state);
		sink->Finalize(*global_sink_state);

		if (!is_last) {
			prev_sink_states.push_back(std::move(global_sink_state));
		}
	}
}

}  // namespace neug::execution::vec
