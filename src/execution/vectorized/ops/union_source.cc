#include "neug/execution/vectorized/ops/union_source.h"

#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/ops/materialize_sink.h"
#include "neug/execution/vectorized/ops/materialize_source.h"
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"

namespace neug::execution::vec {

struct UnionGlobalState : public GlobalSourceState {
	size_t current_pipeline = 0;

	// For multi-stage sub-pipelines: results after execution
	struct ExecutedResult {
		std::vector<GraphDataChunk> chunks;
		size_t cursor = 0;
	};
	std::vector<std::unique_ptr<ExecutedResult>> executed_results;
	bool multi_stage_executed = false;
};

struct UnionLocalState : public LocalSourceState {
	std::vector<std::unique_ptr<GlobalSourceState>> src_gstates;
	std::vector<std::unique_ptr<LocalSourceState>> src_lstates;
	std::vector<std::vector<std::unique_ptr<OperatorState>>> op_states;
};

UnionSource::UnionSource(
    std::vector<std::unique_ptr<UnionSubPipeline>> sub_pipelines)
    : sub_pipelines_(std::move(sub_pipelines)) {}

std::unique_ptr<GlobalSourceState> UnionSource::GetGlobalSourceState(
    const VecExecContext& ctx) const {
	const_cast<UnionSource*>(this)->ctx_ = &ctx;
	return std::make_unique<UnionGlobalState>();
}

std::unique_ptr<LocalSourceState> UnionSource::GetLocalSourceState(
    GlobalSourceState& gstate) const {
	auto lstate = std::make_unique<UnionLocalState>();

	static const VecExecContext empty_ctx{};
	const VecExecContext& ctx = ctx_ ? *ctx_ : empty_ctx;

	for (auto& sp : sub_pipelines_) {
		if (sp->full_pipeline) {
			lstate->src_gstates.push_back(nullptr);
			lstate->src_lstates.push_back(nullptr);
			lstate->op_states.emplace_back();
		} else {
			auto g = sp->source->GetGlobalSourceState(ctx);
			auto l = sp->source->GetLocalSourceState(*g);
			lstate->src_gstates.push_back(std::move(g));
			lstate->src_lstates.push_back(std::move(l));

			std::vector<std::unique_ptr<OperatorState>> ops;
			for (auto& op : sp->operators) {
				ops.push_back(op->GetOperatorState());
			}
			lstate->op_states.push_back(std::move(ops));
		}
	}

	return lstate;
}

SourceResultType UnionSource::GetData(GraphDataChunk& chunk,
                                      GlobalSourceState& gstate,
                                      LocalSourceState& lstate) {
	auto& gs = static_cast<UnionGlobalState&>(gstate);
	auto& ls = static_cast<UnionLocalState&>(lstate);

	static const VecExecContext empty_ctx{};
	const VecExecContext& ctx = ctx_ ? *ctx_ : empty_ctx;

	// Execute multi-stage sub-pipelines once on first call
	if (!gs.multi_stage_executed) {
		gs.multi_stage_executed = true;
		gs.executed_results.resize(sub_pipelines_.size());
		for (size_t pi = 0; pi < sub_pipelines_.size(); pi++) {
			auto& sp = sub_pipelines_[pi];
			if (!sp->full_pipeline) continue;

			auto result = std::make_unique<UnionGlobalState::ExecutedResult>();

			// Execute into a collector sink
			class CollectorSink : public IVecSink {
			 public:
				struct State : public GlobalSinkState {
					std::vector<GraphDataChunk>* out;
				};
				explicit CollectorSink(std::vector<GraphDataChunk>* out)
				    : out_(out) {}
				std::string GetName() const override { return "CollectorSink"; }
				std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override {
					auto s = std::make_unique<State>();
					s->out = out_;
					return s;
				}
				std::unique_ptr<LocalSinkState> GetLocalSinkState(
				    GlobalSinkState&) const override {
					return std::make_unique<LocalSinkState>();
				}
				SinkResultType Sink(GraphDataChunk& c, GlobalSinkState& g,
				                    LocalSinkState&) override {
					if (c.size() > 0) {
						static_cast<State&>(g).out->push_back(std::move(c));
					}
					return SinkResultType::kNeedMoreInput;
				}
			 private:
				std::vector<GraphDataChunk>* out_;
			};

			CollectorSink sink(&result->chunks);
			PipelineExecutor executor;
			executor.Execute(*sp->full_pipeline, &sink, ctx);
			gs.executed_results[pi] = std::move(result);
		}
	}

	while (gs.current_pipeline < sub_pipelines_.size()) {
		auto& sp = sub_pipelines_[gs.current_pipeline];

		// Multi-stage: return pre-executed results
		if (sp->full_pipeline) {
			auto& er = gs.executed_results[gs.current_pipeline];
			if (!er || er->cursor >= er->chunks.size()) {
				gs.current_pipeline++;
				continue;
			}
			chunk = std::move(er->chunks[er->cursor]);
			er->cursor++;
			bool all_done_here = er->cursor >= er->chunks.size();
			if (all_done_here) gs.current_pipeline++;
			bool all_done = gs.current_pipeline >= sub_pipelines_.size();
			return all_done ? SourceResultType::kFinished
			               : SourceResultType::kHaveMoreOutput;
		}

		// Simple single-stage: inline execution
		auto& src_gs = *ls.src_gstates[gs.current_pipeline];
		auto& src_ls = *ls.src_lstates[gs.current_pipeline];
		auto& ops = ls.op_states[gs.current_pipeline];

		GraphDataChunk src_chunk;
		auto src_result = sp->source->GetData(src_chunk, src_gs, src_ls);

		if (src_chunk.size() == 0) {
			if (src_result == SourceResultType::kFinished) {
				gs.current_pipeline++;
				continue;
			}
			continue;
		}

		GraphDataChunk buf_a, buf_b;
		GraphDataChunk* current = &src_chunk;

		for (size_t i = 0; i < sp->operators.size(); i++) {
			GraphDataChunk* output = (current == &buf_a) ? &buf_b : &buf_a;
			if (current == &src_chunk) output = &buf_a;
			output->Reset();
			sp->operators[i]->Execute(*current, *output, *ops[i], ctx);
			current = output;
		}

		if (current->size() > 0) {
			chunk = std::move(*current);
			if (src_result == SourceResultType::kFinished) {
				gs.current_pipeline++;
			}
			bool all_done = gs.current_pipeline >= sub_pipelines_.size();
			return all_done ? SourceResultType::kFinished
			               : SourceResultType::kHaveMoreOutput;
		}

		if (src_result == SourceResultType::kFinished) {
			gs.current_pipeline++;
		}
	}

	chunk = GraphDataChunk();
	chunk.SetCardinality(0);
	return SourceResultType::kFinished;
}

}  // namespace neug::execution::vec
