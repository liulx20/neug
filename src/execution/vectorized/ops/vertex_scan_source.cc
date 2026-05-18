#include "neug/execution/vectorized/ops/vertex_scan_source.h"

#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug::execution::vec {

struct VertexScanGlobalState : public GlobalSourceState {
	struct LabelVids {
		label_t label;
		std::vector<vid_t> vids;
	};
	std::vector<LabelVids> label_vids;
};

struct VertexScanLocalState : public LocalSourceState {
	size_t label_idx = 0;
	size_t vid_offset = 0;
};

VertexScanSource::VertexScanSource(std::vector<label_t> labels, int vertex_tag)
    : labels_(std::move(labels)), vertex_tag_(vertex_tag) {}

std::unique_ptr<GlobalSourceState>
VertexScanSource::GetGlobalSourceState(const VecExecContext& ctx) const {
	auto state = std::make_unique<VertexScanGlobalState>();
	state->label_vids.reserve(labels_.size());
	for (auto label : labels_) {
		VertexScanGlobalState::LabelVids lv;
		lv.label = label;
		auto vset = ctx.graph->GetVertexSet(label);
		vset.foreach_vertex([&](vid_t v) { lv.vids.push_back(v); });
		state->label_vids.push_back(std::move(lv));
	}
	return state;
}

std::unique_ptr<LocalSourceState> VertexScanSource::GetLocalSourceState(
    GlobalSourceState&) const {
	return std::make_unique<VertexScanLocalState>();
}

SourceResultType VertexScanSource::GetData(GraphDataChunk& chunk,
                                           GlobalSourceState& gstate,
                                           LocalSourceState& lstate) {
	auto& gs = static_cast<VertexScanGlobalState&>(gstate);
	auto& ls = static_cast<VertexScanLocalState&>(lstate);

	while (ls.label_idx < gs.label_vids.size()) {
		auto& lv = gs.label_vids[ls.label_idx];
		if (ls.vid_offset >= lv.vids.size()) {
			ls.label_idx++;
			ls.vid_offset = 0;
			continue;
		}

		size_t remaining = lv.vids.size() - ls.vid_offset;
		size_t batch = std::min(static_cast<size_t>(STANDARD_VECTOR_SIZE), remaining);

		auto vertex_vec = VertexVector::CreateSingleLabel(lv.label);
		auto* vids = VertexVector::GetVids(vertex_vec);
		for (size_t i = 0; i < batch; i++) {
			vids[i] = lv.vids[ls.vid_offset + i];
		}

		chunk = GraphDataChunk();
		chunk.AddColumn(vertex_tag_, std::move(vertex_vec));
		chunk.SetCardinality(batch);

		ls.vid_offset += batch;

		bool more = false;
		if (ls.vid_offset < lv.vids.size()) {
			more = true;
		} else {
			ls.label_idx++;
			ls.vid_offset = 0;
			for (size_t i = ls.label_idx; i < gs.label_vids.size(); i++) {
				if (!gs.label_vids[i].vids.empty()) {
					more = true;
					break;
				}
			}
		}

		return more ? SourceResultType::kHaveMoreOutput
		            : SourceResultType::kFinished;
	}

	chunk.SetCardinality(0);
	return SourceResultType::kFinished;
}

}  // namespace neug::execution::vec
