#include <gtest/gtest.h>

#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/ops/filter_operator.h"
#include "neug/execution/vectorized/ops/project_operator.h"
#include "neug/execution/vectorized/ops/result_sink.h"
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"
#include "neug/execution/vectorized/pipeline/vec_pipeline.h"

using namespace neug::execution::vec;
using neug::DataType;
using neug::DataTypeId;

// ============================================================
// TestSource: produces synthetic vertex + int32 chunks
// ============================================================

class TestSource : public IVecSource {
 public:
	TestSource(neug::label_t label, size_t total_vertices, int vertex_tag,
	           int value_tag = -1)
	    : label_(label),
	      total_(total_vertices),
	      vertex_tag_(vertex_tag),
	      value_tag_(value_tag) {}

	std::string GetName() const override { return "TestSource"; }

	struct State : public LocalSourceState {
		size_t offset = 0;
	};

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(
	    const VecExecContext&) const override {
		return std::make_unique<GlobalSourceState>();
	}

	std::unique_ptr<LocalSourceState> GetLocalSourceState(
	    GlobalSourceState&) const override {
		return std::make_unique<State>();
	}

	SourceResultType GetData(GraphDataChunk& chunk, GlobalSourceState&,
	                         LocalSourceState& lstate) override {
		auto& state = static_cast<State&>(lstate);
		if (state.offset >= total_) {
			chunk.SetCardinality(0);
			return SourceResultType::kFinished;
		}

		size_t batch = std::min(static_cast<size_t>(STANDARD_VECTOR_SIZE),
		                        total_ - state.offset);

		chunk = GraphDataChunk();
		auto vertex_vec = VertexVector::CreateSingleLabel(label_);
		auto* vids = VertexVector::GetVids(vertex_vec);
		for (size_t i = 0; i < batch; i++) {
			vids[i] = static_cast<neug::vid_t>(state.offset + i);
		}
		chunk.AddColumn(vertex_tag_, std::move(vertex_vec));

		if (value_tag_ >= 0) {
			GraphVector val_vec{DataType(DataTypeId::kInt32)};
			auto* data = val_vec.GetData<int32_t>();
			for (size_t i = 0; i < batch; i++) {
				data[i] = static_cast<int32_t>(state.offset + i);
			}
			chunk.AddColumn(value_tag_, std::move(val_vec));
		}

		chunk.SetCardinality(batch);
		state.offset += batch;

		return (state.offset >= total_) ? SourceResultType::kFinished
		                                : SourceResultType::kHaveMoreOutput;
	}

 private:
	neug::label_t label_;
	size_t total_;
	int vertex_tag_;
	int value_tag_;
};

// ============================================================
// Helper: count total rows across all result chunks
// ============================================================

static size_t CountRows(const std::vector<GraphDataChunk>& chunks) {
	size_t total = 0;
	for (auto& c : chunks) {
		total += c.size();
	}
	return total;
}

// ============================================================
// Tests
// ============================================================

class SharedResultSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		std::vector<GraphDataChunk> chunks;
		size_t total_rows = 0;
	};

	explicit SharedResultSink(std::shared_ptr<State> state)
	    : state_(std::move(state)) {}

	std::string GetName() const override { return "SharedResultSink"; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const override {
		return std::make_unique<GlobalSinkState>();
	}

	std::unique_ptr<LocalSinkState> GetLocalSinkState(
	    GlobalSinkState&) const override {
		return std::make_unique<LocalSinkState>();
	}

	SinkResultType Sink(GraphDataChunk& chunk, GlobalSinkState&,
	                    LocalSinkState&) override {
		state_->total_rows += chunk.size();
		state_->chunks.push_back(std::move(chunk));
		return SinkResultType::kNeedMoreInput;
	}

	std::shared_ptr<State> state_;
};

TEST(VectorizedPipeline, SourceToSinkBasic) {
	TestSource source(1, 100, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 100u);
	EXPECT_EQ(CountRows(result_state->chunks), 100u);

	auto& first_chunk = result_state->chunks[0];
	EXPECT_EQ(first_chunk.ColumnCount(), 1u);
	EXPECT_EQ(first_chunk.GetTag(0), 0);

	auto v = VertexVector::GetVertex(first_chunk.GetVector(0), 0);
	EXPECT_EQ(v.label_, 1);
	EXPECT_EQ(v.vid_, 0u);
}

TEST(VectorizedPipeline, MultiChunkScan) {
	size_t total = STANDARD_VECTOR_SIZE * 3 + 500;
	TestSource source(2, total, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, total);
	EXPECT_EQ(result_state->chunks.size(), 4u);
	EXPECT_EQ(result_state->chunks[0].size(), STANDARD_VECTOR_SIZE);
	EXPECT_EQ(result_state->chunks[1].size(), STANDARD_VECTOR_SIZE);
	EXPECT_EQ(result_state->chunks[2].size(), STANDARD_VECTOR_SIZE);
	EXPECT_EQ(result_state->chunks[3].size(), 500u);
}

TEST(VectorizedPipeline, FilterEvenVids) {
	TestSource source(1, 100, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	FilterOperator filter([](const GraphDataChunk& chunk, size_t row) {
		auto v = VertexVector::GetVertex(chunk.GetVector(0), row);
		return v.vid_ % 2 == 0;
	});

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&filter);
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 50u);

	for (auto& chunk : result_state->chunks) {
		for (size_t i = 0; i < chunk.size(); i++) {
			auto v = VertexVector::GetVertex(chunk.GetVector(0), i);
			EXPECT_EQ(v.vid_ % 2, 0u);
		}
	}
}

TEST(VectorizedPipeline, FilterRejectsAll) {
	TestSource source(1, 100, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	FilterOperator filter(
	    [](const GraphDataChunk&, size_t) { return false; });

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&filter);
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 0u);
}

TEST(VectorizedPipeline, FilterAcceptsAll) {
	TestSource source(1, 100, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	FilterOperator filter(
	    [](const GraphDataChunk&, size_t) { return true; });

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&filter);
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 100u);
}

TEST(VectorizedPipeline, ProjectKeepTag) {
	TestSource source(1, 50, 0, 1);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	ProjectOperator project({1});

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&project);
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 50u);
	auto& chunk = result_state->chunks[0];
	EXPECT_EQ(chunk.ColumnCount(), 1u);
	EXPECT_EQ(chunk.GetTag(0), 1);
	EXPECT_EQ(chunk.GetVector(0).type_id(), DataTypeId::kInt32);
	EXPECT_EQ(chunk.GetVector(0).GetData<int32_t>()[0], 0);
	EXPECT_EQ(chunk.GetVector(0).GetData<int32_t>()[49], 49);
}

TEST(VectorizedPipeline, FullPipelineFilterThenProject) {
	TestSource source(1, 100, 0, 1);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	FilterOperator filter([](const GraphDataChunk& chunk, size_t row) {
		return chunk.GetVector(1).GetData<int32_t>()[row] >= 50;
	});

	ProjectOperator project({1});

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&filter);
	pipeline.operators.push_back(&project);
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 50u);

	auto& chunk = result_state->chunks[0];
	EXPECT_EQ(chunk.ColumnCount(), 1u);
	EXPECT_EQ(chunk.GetTag(0), 1);

	for (size_t i = 0; i < chunk.size(); i++) {
		EXPECT_GE(chunk.GetVector(0).GetData<int32_t>()[i], 50);
	}
}

TEST(VectorizedPipeline, EmptySource) {
	TestSource source(1, 0, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 0u);
	EXPECT_TRUE(result_state->chunks.empty());
}

TEST(VectorizedPipeline, MultipleOperators) {
	TestSource source(1, 200, 0, 1);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	FilterOperator filter1([](const GraphDataChunk& chunk, size_t row) {
		return chunk.GetVector(1).GetData<int32_t>()[row] % 2 == 0;
	});

	FilterOperator filter2([](const GraphDataChunk& chunk, size_t row) {
		return chunk.GetVector(1).GetData<int32_t>()[row] % 3 == 0;
	});

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&filter1);
	pipeline.operators.push_back(&filter2);
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	// divisible by both 2 and 3 = divisible by 6
	// 0, 6, 12, ..., 198 → 200/6 = 33.33 → 34 values (0 counts)
	size_t expected = 0;
	for (int i = 0; i < 200; i++) {
		if (i % 6 == 0) expected++;
	}
	EXPECT_EQ(result_state->total_rows, expected);

	for (auto& chunk : result_state->chunks) {
		for (size_t i = 0; i < chunk.size(); i++) {
			int32_t val = chunk.GetVector(1).GetData<int32_t>()[i];
			EXPECT_EQ(val % 6, 0);
		}
	}
}

TEST(VectorizedPipeline, ConstantLabelPreserved) {
	TestSource source(7, 50, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.sink = &sink;

	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	auto& vertex_vec = chunk.GetVector(0);
	EXPECT_TRUE(VertexVector::IsConstantLabel(vertex_vec));
	EXPECT_EQ(VertexVector::GetConstantLabel(vertex_vec), 7);

	for (size_t i = 0; i < chunk.size(); i++) {
		auto v = VertexVector::GetVertex(vertex_vec, i);
		EXPECT_EQ(v.label_, 7);
		EXPECT_EQ(v.vid_, i);
	}
}
