#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/ops/filter_operator.h"
#include "neug/execution/vectorized/ops/project_operator.h"
#include "neug/execution/vectorized/ops/property_read_operator.h"
#include "neug/execution/vectorized/ops/result_sink.h"
#include "neug/execution/vectorized/ops/vec_select_operator.h"
#include "neug/execution/vectorized/ops/vertex_scan_source.h"
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/pipeline/vec_pipeline.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"

using namespace neug::execution::vec;
using neug::DataType;
using neug::DataTypeId;
using neug::Property;
using neug::execution::Value;

// ============================================================
// Shared sink for result inspection
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

// ============================================================
// Test fixture: in-memory PropertyGraph with Person vertices
// ============================================================

class VectorizedStorageTest : public ::testing::Test {
 protected:
	std::string work_dir_;
	std::unique_ptr<neug::PropertyGraph> graph_;
	std::unique_ptr<neug::StorageReadInterface> reader_;
	neug::label_t person_label_;
	static constexpr size_t kNumPersons = 100;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_storage_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		graph_ = std::make_unique<neug::PropertyGraph>();
		graph_->Open(work_dir_, neug::MemoryLevel::kInMemory);

		neug::CreateVertexTypeParamBuilder builder;
		auto status = graph_->CreateVertexType(
		    builder.VertexLabel("person")
		        .AddProperty("id", Value::INT64(0))
		        .AddProperty("name", Value::STRING(""))
		        .AddProperty("age", Value::INT32(0))
		        .AddPrimaryKeyName("id")
		        .Build());
		ASSERT_TRUE(status.ok()) << status.error_message();

		person_label_ = graph_->schema().get_vertex_label_id("person");

		for (size_t i = 0; i < kNumPersons; i++) {
			neug::vid_t vid;
			auto s = graph_->AddVertex(
			    person_label_, Property::from_int64(static_cast<int64_t>(i)),
			    {Property::from_string_view("person_" + std::to_string(i)),
			     Property::from_int32(static_cast<int32_t>(20 + (i % 40)))},
			    vid, 0);
			ASSERT_TRUE(s.ok()) << s.error_message();
		}

		reader_ = std::make_unique<neug::StorageReadInterface>(*graph_, 0);
	}

	void TearDown() override {
		reader_.reset();
		graph_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}
};

// ============================================================
// Tests
// ============================================================

TEST_F(VectorizedStorageTest, VertexScanBasic) {
	VertexScanSource source({person_label_}, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.sink = &sink;

	VecExecContext ctx{nullptr, reader_.get()};
	pipeline.ctx = &ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, kNumPersons);

	auto& first_chunk = result_state->chunks[0];
	EXPECT_EQ(first_chunk.ColumnCount(), 1u);
	EXPECT_EQ(first_chunk.GetTag(0), 0);

	auto& vertex_vec = first_chunk.GetVector(0);
	EXPECT_TRUE(VertexVector::IsConstantLabel(vertex_vec));
	EXPECT_EQ(VertexVector::GetConstantLabel(vertex_vec), person_label_);
}

TEST_F(VectorizedStorageTest, VertexScanMultiChunk) {
	// Need > STANDARD_VECTOR_SIZE vertices for multi-chunk
	// Add more vertices
	for (size_t i = kNumPersons; i < STANDARD_VECTOR_SIZE + 500; i++) {
		neug::vid_t vid;
		graph_->AddVertex(
		    person_label_, Property::from_int64(static_cast<int64_t>(i)),
		    {Property::from_string_view("person_" + std::to_string(i)),
		     Property::from_int32(static_cast<int32_t>(20 + (i % 40)))},
		    vid, 0);
	}
	auto new_reader =
	    std::make_unique<neug::StorageReadInterface>(*graph_, 0);

	VertexScanSource source({person_label_}, 0);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.sink = &sink;

	VecExecContext ctx{nullptr, new_reader.get()};
	pipeline.ctx = &ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	size_t expected_total = STANDARD_VECTOR_SIZE + 500;
	EXPECT_EQ(result_state->total_rows, expected_total);
	EXPECT_GE(result_state->chunks.size(), 2u);
	EXPECT_EQ(result_state->chunks[0].size(), STANDARD_VECTOR_SIZE);
}

TEST_F(VectorizedStorageTest, PropertyReadInt32) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext ctx{nullptr, reader_.get()};
	pipeline.ctx = &ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, kNumPersons);

	auto& chunk = result_state->chunks[0];
	EXPECT_EQ(chunk.ColumnCount(), 2u);

	int age_col = chunk.FindColumnByTag(1);
	ASSERT_GE(age_col, 0);
	auto& age_vec = chunk.GetVector(static_cast<size_t>(age_col));
	EXPECT_EQ(age_vec.type_id(), DataTypeId::kInt32);

	// Verify first few values: age = 20 + (i % 40)
	auto* ages = age_vec.GetData<int32_t>();
	for (size_t i = 0; i < std::min(chunk.size(), size_t(10)); i++) {
		auto v = VertexVector::GetVertex(chunk.GetVector(0), i);
		int32_t expected_age = static_cast<int32_t>(20 + (v.vid_ % 40));
		EXPECT_EQ(ages[i], expected_age)
		    << "Mismatch at row " << i << " vid=" << v.vid_;
	}
}

TEST_F(VectorizedStorageTest, PropertyReadString) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "name", 1);
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext ctx{nullptr, reader_.get()};
	pipeline.ctx = &ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, kNumPersons);

	auto& chunk = result_state->chunks[0];
	int name_col = chunk.FindColumnByTag(1);
	ASSERT_GE(name_col, 0);
	auto& name_vec = chunk.GetVector(static_cast<size_t>(name_col));
	EXPECT_EQ(name_vec.type_id(), DataTypeId::kVarchar);

	// Verify first string value
	auto* str_data = StringVector::GetStringData(name_vec);
	auto v0 = VertexVector::GetVertex(chunk.GetVector(0), 0);
	std::string expected = "person_" + std::to_string(v0.vid_);
	EXPECT_EQ(str_data[0].GetString(), expected);
}

TEST_F(VectorizedStorageTest, VecSelectFilter) {
	// Pipeline: Scan → PropRead(age) → Select(age > 40)
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	VecSelectOperator select(1, CompareOp::kGt, Value::INT32(40));
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.operators.push_back(&select);
	pipeline.sink = &sink;

	VecExecContext ctx{nullptr, reader_.get()};
	pipeline.ctx = &ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	// ages are 20 + (i % 40) for i in [0, 100)
	// age > 40 means 20 + (i%40) > 40, i.e. i%40 > 20
	// That's i%40 in {21,22,...,39} → 19 values per period of 40
	// For i in [0,100): periods [0,39] and [40,79] each have 19, [80,99] has 19
	// Total: 19 + 19 + 9 = 47... let me compute exactly
	size_t expected = 0;
	for (size_t i = 0; i < kNumPersons; i++) {
		if (static_cast<int32_t>(20 + (i % 40)) > 40) {
			expected++;
		}
	}
	EXPECT_EQ(result_state->total_rows, expected);

	for (auto& chunk : result_state->chunks) {
		int age_col = chunk.FindColumnByTag(1);
		ASSERT_GE(age_col, 0);
		auto* ages = chunk.GetVector(static_cast<size_t>(age_col)).GetData<int32_t>();
		for (size_t i = 0; i < chunk.size(); i++) {
			EXPECT_GT(ages[i], 40);
		}
	}
}

TEST_F(VectorizedStorageTest, VecSelectRejectsAll) {
	// age is always in [20, 59], so age > 100 matches nothing
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	VecSelectOperator select(1, CompareOp::kGt, Value::INT32(100));
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.operators.push_back(&select);
	pipeline.sink = &sink;

	VecExecContext ctx{nullptr, reader_.get()};
	pipeline.ctx = &ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	EXPECT_EQ(result_state->total_rows, 0u);
}

TEST_F(VectorizedStorageTest, EndToEndQuery) {
	// MATCH (n:Person) WHERE n.age > 30 RETURN n.name
	// Pipeline: Scan → PropRead(age,tag=1) → Select(tag=1 > 30)
	//           → PropRead(name,tag=2) → Project({2}) → Sink
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator read_age(0, "age", 1);
	VecSelectOperator select(1, CompareOp::kGt, Value::INT32(30));
	PropertyReadOperator read_name(0, "name", 2);
	ProjectOperator project({2});
	auto result_state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&read_age);
	pipeline.operators.push_back(&select);
	pipeline.operators.push_back(&read_name);
	pipeline.operators.push_back(&project);
	pipeline.sink = &sink;

	VecExecContext ctx{nullptr, reader_.get()};
	pipeline.ctx = &ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	// Count how many persons have age > 30
	size_t expected = 0;
	for (size_t i = 0; i < kNumPersons; i++) {
		if (static_cast<int32_t>(20 + (i % 40)) > 30) {
			expected++;
		}
	}
	EXPECT_EQ(result_state->total_rows, expected);
	EXPECT_GT(expected, 0u);

	// Each result chunk should have exactly 1 column (name, tag=2)
	for (auto& chunk : result_state->chunks) {
		EXPECT_EQ(chunk.ColumnCount(), 1u);
		EXPECT_EQ(chunk.GetTag(0), 2);
		EXPECT_EQ(chunk.GetVector(0).type_id(), DataTypeId::kVarchar);
	}

	// Verify a name value is present and non-empty
	if (!result_state->chunks.empty() && result_state->chunks[0].size() > 0) {
		auto* str_data =
		    StringVector::GetStringData(result_state->chunks[0].GetVector(0));
		std::string name = str_data[0].GetString();
		EXPECT_TRUE(name.find("person_") == 0)
		    << "Expected name starting with 'person_', got: " << name;
	}
}
