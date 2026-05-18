#include "neug/execution/vectorized/pipeline/pipeline_executor.h"

#include <cassert>

#include "neug/execution/utils/opr_timer.h"
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
		std::vector<bool> has_more(pipeline.operators.size(), false);

		{
			GraphDataChunk* current = input;
			for (size_t i = 0; i < pipeline.operators.size(); i++) {
				GraphDataChunk* output = (current == &buf_a) ? &buf_b : &buf_a;
				output->Reset();
				auto result = pipeline.operators[i]->Execute(
				    *current, *output, *op_states[i], ctx);
				if (result == OperatorResultType::kHaveMoreOutput) {
					has_more[i] = true;
				}
				current = output;
			}
			if (current->size() > 0) {
				pipeline.sink->Sink(
				    *current, *global_sink_state, *local_sink_state);
			}
		}

		while (true) {
			int start = -1;
			for (int i = static_cast<int>(pipeline.operators.size()) - 1;
			     i >= 0; i--) {
				if (has_more[i]) {
					start = i;
					break;
				}
			}
			if (start < 0) break;

			has_more[start] = false;
			GraphDataChunk* current = input;

			for (size_t i = static_cast<size_t>(start);
			     i < pipeline.operators.size(); i++) {
				GraphDataChunk* output = (current == &buf_a) ? &buf_b : &buf_a;
				output->Reset();
				auto result = pipeline.operators[i]->Execute(
				    *current, *output, *op_states[i], ctx);
				if (result == OperatorResultType::kHaveMoreOutput) {
					has_more[i] = true;
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

	execution::OprTimer* cur_timer = ctx.timer;

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

		// Build per-operator timer chain for this stage:
		// source_timer -> op_timer[0] -> ... -> op_timer[N-1] -> sink_timer
		execution::OprTimer* source_timer = nullptr;
		std::vector<execution::OprTimer*> op_timers;
		execution::OprTimer* sink_timer = nullptr;

		if (cur_timer) {
			source_timer = cur_timer;
			source_timer->set_name(source->GetName());

			// Build chain from back to front (set_next takes ownership)
			std::unique_ptr<execution::OprTimer> chain =
			    std::make_unique<execution::OprTimer>();
			chain->set_name(sink->GetName());
			sink_timer = chain.get();

			for (int i = static_cast<int>(stage.operators.size()) - 1;
			     i >= 0; i--) {
				auto node = std::make_unique<execution::OprTimer>();
				node->set_name(stage.operators[i]->GetName());
				node->set_next(std::move(chain));
				chain = std::move(node);
			}
			source_timer->set_next(std::move(chain));

			// Collect raw pointers by walking the chain
			execution::OprTimer* t = source_timer->next();
			for (size_t i = 0; i < stage.operators.size(); i++) {
				op_timers.push_back(t);
				t = t->next();
			}
			sink_timer = t;
		}

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
		execution::TimerUnit tu;

		while (true) {
			source_chunk.Reset();
			if (source_timer) tu.start();
			auto source_result = source->GetData(
			    source_chunk, *global_source_state, *local_source_state);
			if (source_timer) {
				source_timer->record(tu);
				source_timer->add_num_tuples(source_chunk.size());
			}

			if (source_chunk.size() == 0) {
				if (source_result == SourceResultType::kFinished) break;
				continue;
			}

			std::vector<bool> has_more(stage.operators.size(), false);
			GraphDataChunk* input = &source_chunk;

			// Initial pass
			{
				GraphDataChunk* current = input;
				for (size_t i = 0; i < stage.operators.size(); i++) {
					GraphDataChunk* output =
					    (current == &buf_a) ? &buf_b : &buf_a;
					output->Reset();
					if (cur_timer) tu.start();
					auto result = stage.operators[i]->Execute(
					    *current, *output, *op_states[i], ctx);
					if (cur_timer) {
						op_timers[i]->record(tu);
						op_timers[i]->add_num_tuples(output->size());
					}
					if (result == OperatorResultType::kHaveMoreOutput) {
						has_more[i] = true;
					}
					current = output;
				}
				if (current->size() > 0) {
					if (sink_timer) tu.start();
					sink->Sink(*current, *global_sink_state,
					           *local_sink_state);
					if (sink_timer) {
						sink_timer->record(tu);
						sink_timer->add_num_tuples(current->size());
					}
				}
			}

			// Drain loop
			while (true) {
				int start = -1;
				for (int i = static_cast<int>(stage.operators.size()) - 1;
				     i >= 0; i--) {
					if (has_more[i]) {
						start = i;
						break;
					}
				}
				if (start < 0) break;

				has_more[start] = false;
				GraphDataChunk* current = input;

				for (size_t i = static_cast<size_t>(start);
				     i < stage.operators.size(); i++) {
					GraphDataChunk* output =
					    (current == &buf_a) ? &buf_b : &buf_a;
					output->Reset();
					if (cur_timer) tu.start();
					auto result = stage.operators[i]->Execute(
					    *current, *output, *op_states[i], ctx);
					if (cur_timer) {
						op_timers[i]->record(tu);
						op_timers[i]->add_num_tuples(output->size());
					}
					if (result == OperatorResultType::kHaveMoreOutput) {
						has_more[i] = true;
					}
					current = output;
				}
				if (current->size() > 0) {
					if (sink_timer) tu.start();
					sink->Sink(*current, *global_sink_state,
					           *local_sink_state);
					if (sink_timer) {
						sink_timer->record(tu);
						sink_timer->add_num_tuples(current->size());
					}
				}
			}

			if (source_result == SourceResultType::kFinished) break;
		}

		sink->Combine(*global_sink_state, *local_sink_state);
		sink->Finalize(*global_sink_state);

		if (!is_last) {
			prev_sink_states.push_back(std::move(global_sink_state));
		}

		// Advance cur_timer to next stage's position
		if (cur_timer && sink_timer) {
			cur_timer = sink_timer->next();
		}
	}
}

}  // namespace neug::execution::vec
