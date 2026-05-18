#include "neug/execution/vectorized/ops/index_scan_source.h"

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug::execution::vec {

struct IndexScanGlobalState : public GlobalSourceState {
	std::vector<std::pair<label_t, vid_t>> vertices;
	bool emitted = false;
};

struct IndexScanLocalState : public LocalSourceState {};

IndexScanSource::IndexScanSource(std::vector<Property> pks,
                                 std::vector<label_t> labels, int vertex_tag)
    : pks_(std::move(pks)),
      use_param_(false),
      labels_(std::move(labels)),
      vertex_tag_(vertex_tag) {}

IndexScanSource::IndexScanSource(std::string param_name, DataType param_type,
                                 std::vector<label_t> labels, int vertex_tag)
    : param_name_(std::move(param_name)),
      param_type_(param_type),
      use_param_(true),
      labels_(std::move(labels)),
      vertex_tag_(vertex_tag) {}

std::unique_ptr<GlobalSourceState>
IndexScanSource::GetGlobalSourceState(const VecExecContext& ctx) const {
	auto state = std::make_unique<IndexScanGlobalState>();

	std::vector<Property> pks;
	if (use_param_) {
		assert(ctx.params != nullptr);
		const auto& val = ctx.params->at(param_name_);
		Property p;
		switch (param_type_.id()) {
		case DataTypeId::kInt32:
			p.set_int32(val.GetValue<int32_t>());
			break;
		case DataTypeId::kInt64:
			p.set_int64(val.GetValue<int64_t>());
			break;
		case DataTypeId::kVarchar:
			p.set_string_view(val.GetValue<std::string>());
			break;
		default:
			p.set_int64(val.GetValue<int64_t>());
			break;
		}
		pks.push_back(std::move(p));
	} else {
		pks = pks_;
	}

	for (auto label : labels_) {
		for (auto& pk : pks) {
			vid_t vid;
			if (ctx.graph->GetVertexIndex(label, pk, vid)) {
				state->vertices.emplace_back(label, vid);
			}
		}
	}

	return state;
}

std::unique_ptr<LocalSourceState> IndexScanSource::GetLocalSourceState(
    GlobalSourceState&) const {
	return std::make_unique<IndexScanLocalState>();
}

SourceResultType IndexScanSource::GetData(GraphDataChunk& chunk,
                                          GlobalSourceState& gstate,
                                          LocalSourceState&) {
	auto& gs = static_cast<IndexScanGlobalState&>(gstate);
	if (gs.emitted || gs.vertices.empty()) {
		chunk.SetCardinality(0);
		return SourceResultType::kFinished;
	}

	gs.emitted = true;
	size_t count = gs.vertices.size();

	bool single_label = true;
	label_t first_label = gs.vertices[0].first;
	for (size_t i = 1; i < count; i++) {
		if (gs.vertices[i].first != first_label) {
			single_label = false;
			break;
		}
	}

	GraphVector vertex_vec = single_label
	    ? VertexVector::CreateSingleLabel(first_label)
	    : VertexVector::Create();

	vid_t* vids = VertexVector::GetVids(vertex_vec);
	for (size_t i = 0; i < count; i++) {
		vids[i] = gs.vertices[i].second;
	}

	if (!single_label) {
		label_t* labels = VertexVector::GetLabels(vertex_vec);
		for (size_t i = 0; i < count; i++) {
			labels[i] = gs.vertices[i].first;
		}
	}

	chunk = GraphDataChunk();
	chunk.AddColumn(vertex_tag_, std::move(vertex_vec));
	chunk.SetCardinality(count);

	return SourceResultType::kFinished;
}

}  // namespace neug::execution::vec
