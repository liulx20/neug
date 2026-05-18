#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/ops/project_operator.h"
#include "neug/execution/vectorized/ops/property_read_operator.h"
#include "neug/execution/vectorized/ops/vec_expr_filter_operator.h"
#include "neug/execution/vectorized/ops/vertex_scan_source.h"
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"
#include "neug/execution/vectorized/pipeline/vec_pipeline.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"

using namespace neug::execution::vec;
static const VecExecContext ctx_{};
using neug::DataType;
using neug::DataTypeId;
using neug::Property;
using neug::execution::Value;

// ============================================================
// Shared sink (same as test_vectorized_storage.cc)
// ============================================================

class ExprResultSink : public IVecSink {
 public:
	struct State : public GlobalSinkState {
		std::vector<GraphDataChunk> chunks;
		size_t total_rows = 0;
	};

	explicit ExprResultSink(std::shared_ptr<State> state)
	    : state_(std::move(state)) {}

	std::string GetName() const override { return "ExprResultSink"; }

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
// Test fixture
// ============================================================

class VectorizedExpressionTest : public ::testing::Test {
 protected:
	std::string work_dir_;
	std::unique_ptr<neug::PropertyGraph> graph_;
	std::unique_ptr<neug::StorageReadInterface> reader_;
	neug::label_t person_label_;
	static constexpr size_t kNumPersons = 100;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_expr_" +
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

	// Helper: run a pipeline and return result state
	std::shared_ptr<ExprResultSink::State> RunPipeline(VecPipeline& pipeline) {
		auto result_state = std::make_shared<ExprResultSink::State>();
		ExprResultSink sink(result_state);
		pipeline.sink = &sink;
		VecExecContext exec_ctx{nullptr, reader_.get()};
		pipeline.ctx = &exec_ctx;

		PipelineExecutor executor;
		executor.Execute(pipeline);
		return result_state;
	}
};

// ============================================================
// Expression unit tests (evaluate on a pre-built chunk)
// ============================================================

TEST_F(VectorizedExpressionTest, ConstantExprInt32) {
	// Build a chunk with age column (tag=1)
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// Evaluate constant expression
	VecConstantExpr const_expr(Value::INT32(42));
	DataType int_type{DataTypeId::kInt32};
	GraphVector result{int_type};
	const_expr.Evaluate(chunk, count, result, ctx_);

	auto* data = result.GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_EQ(data[i], 42);
	}
}

TEST_F(VectorizedExpressionTest, ColumnRefExpr) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// Read age column by tag
	VecColumnRefExpr col_ref(1, DataType{DataTypeId::kInt32});
	DataType int_type{DataTypeId::kInt32};
	GraphVector result{int_type};
	col_ref.Evaluate(chunk, count, result, ctx_);

	auto* ages = result.GetData<int32_t>();
	auto* orig_ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_EQ(ages[i], orig_ages[i]);
	}
}

TEST_F(VectorizedExpressionTest, ComparisonExpr) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// age > 30
	auto cmp = std::make_unique<VecComparisonExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    CompareOp::kGt,
	    std::make_unique<VecConstantExpr>(Value::INT32(30)));

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	cmp->Evaluate(chunk, count, result, ctx_);

	auto* flags = result.GetData<bool>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_EQ(flags[i], ages[i] > 30)
		    << "Mismatch at row " << i << " age=" << ages[i];
	}
}

TEST_F(VectorizedExpressionTest, AndExpr) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// age > 30 AND age < 50
	auto and_expr = std::make_unique<VecBooleanAndExpr>(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(30))),
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kLt,
	        std::make_unique<VecConstantExpr>(Value::INT32(50))));

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	and_expr->Evaluate(chunk, count, result, ctx_);

	auto* flags = result.GetData<bool>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		bool expected = (ages[i] > 30) && (ages[i] < 50);
		EXPECT_EQ(flags[i], expected)
		    << "Mismatch at row " << i << " age=" << ages[i];
	}
}

TEST_F(VectorizedExpressionTest, OrExpr) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// age < 25 OR age > 55
	auto or_expr = std::make_unique<VecBooleanOrExpr>(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kLt,
	        std::make_unique<VecConstantExpr>(Value::INT32(25))),
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(55))));

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	or_expr->Evaluate(chunk, count, result, ctx_);

	auto* flags = result.GetData<bool>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		bool expected = (ages[i] < 25) || (ages[i] > 55);
		EXPECT_EQ(flags[i], expected)
		    << "Mismatch at row " << i << " age=" << ages[i];
	}
}

TEST_F(VectorizedExpressionTest, NotExpr) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// NOT (age > 40)
	auto not_expr = std::make_unique<VecBooleanNotExpr>(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(40))));

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	not_expr->Evaluate(chunk, count, result, ctx_);

	auto* flags = result.GetData<bool>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		bool expected = !(ages[i] > 40);
		EXPECT_EQ(flags[i], expected)
		    << "Mismatch at row " << i << " age=" << ages[i];
	}
}

// ============================================================
// Filter operator tests (full pipeline)
// ============================================================

TEST_F(VectorizedExpressionTest, ExprFilterBasic) {
	// Pipeline: Scan → PropRead(age) → ExprFilter(age > 40) → Sink
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);

	auto predicate = std::make_unique<VecComparisonExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    CompareOp::kGt,
	    std::make_unique<VecConstantExpr>(Value::INT32(40)));
	VecExprFilterOperator filter(std::move(predicate));

	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.operators.push_back(&filter);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

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
		auto* ages =
		    chunk.GetVector(static_cast<size_t>(age_col)).GetData<int32_t>();
		for (size_t i = 0; i < chunk.size(); i++) {
			EXPECT_GT(ages[i], 40);
		}
	}
}

TEST_F(VectorizedExpressionTest, ExprFilterCompound) {
	// MATCH (n:Person) WHERE n.age > 30 AND n.age < 50 RETURN n.name
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator read_age(0, "age", 1);

	auto predicate = std::make_unique<VecBooleanAndExpr>(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(30))),
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kLt,
	        std::make_unique<VecConstantExpr>(Value::INT32(50))));
	VecExprFilterOperator filter(std::move(predicate));

	PropertyReadOperator read_name(0, "name", 2);
	ProjectOperator project({2});

	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&read_age);
	pipeline.operators.push_back(&filter);
	pipeline.operators.push_back(&read_name);
	pipeline.operators.push_back(&project);
	pipeline.sink = &sink;

	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	// Count expected: age in (30, 50), i.e. 20 + i%40 in (30,50) ↔ i%40 in (10,30)
	size_t expected = 0;
	for (size_t i = 0; i < kNumPersons; i++) {
		int32_t age = static_cast<int32_t>(20 + (i % 40));
		if (age > 30 && age < 50) {
			expected++;
		}
	}
	EXPECT_EQ(result_state->total_rows, expected);
	EXPECT_GT(expected, 0u);

	// Each result chunk should have 1 column (name, tag=2)
	for (auto& chunk : result_state->chunks) {
		EXPECT_EQ(chunk.ColumnCount(), 1u);
		EXPECT_EQ(chunk.GetTag(0), 2);
		EXPECT_EQ(chunk.GetVector(0).type_id(), DataTypeId::kVarchar);
	}

	// Verify a name value
	if (!result_state->chunks.empty() && result_state->chunks[0].size() > 0) {
		auto* str_data =
		    StringVector::GetStringData(result_state->chunks[0].GetVector(0));
		std::string name = str_data[0].GetString();
		EXPECT_TRUE(name.find("person_") == 0)
		    << "Expected name starting with 'person_', got: " << name;
	}
}

// ============================================================
// Arithmetic expression tests
// ============================================================

TEST_F(VectorizedExpressionTest, ArithAddInt32) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// age + 10
	auto arith = std::make_unique<VecArithExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    ArithOp::kAdd,
	    std::make_unique<VecConstantExpr>(Value::INT32(10)));

	DataType int_type{DataTypeId::kInt32};
	GraphVector result{int_type};
	arith->Evaluate(chunk, count, result, ctx_);

	auto* out = result.GetData<int32_t>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_EQ(out[i], ages[i] + 10);
	}
}

TEST_F(VectorizedExpressionTest, ArithMixedWithCast) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "id", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// Cast(1, int64) + id  -- id is int64
	auto arith = std::make_unique<VecArithExpr>(
	    std::make_unique<VecCastExpr>(
	        std::make_unique<VecConstantExpr>(Value::INT32(1)),
	        DataType{DataTypeId::kInt64}),
	    ArithOp::kAdd,
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt64}));

	DataType int64_type{DataTypeId::kInt64};
	GraphVector result{int64_type};
	arith->Evaluate(chunk, count, result, ctx_);

	auto* out = result.GetData<int64_t>();
	auto* ids = chunk.GetVectorByTag(1).GetData<int64_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_EQ(out[i], 1 + ids[i]);
	}
}

TEST_F(VectorizedExpressionTest, UnaryMinus) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	auto neg = std::make_unique<VecUnaryMinusExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}));

	DataType int_type{DataTypeId::kInt32};
	GraphVector result{int_type};
	neg->Evaluate(chunk, count, result, ctx_);

	auto* out = result.GetData<int32_t>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_EQ(out[i], -ages[i]);
	}
}

// ============================================================
// Cast tests
// ============================================================

TEST_F(VectorizedExpressionTest, CastInt32ToInt64) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	auto cast = std::make_unique<VecCastExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    DataType{DataTypeId::kInt64});

	DataType int64_type{DataTypeId::kInt64};
	GraphVector result{int64_type};
	cast->Evaluate(chunk, count, result, ctx_);

	auto* out = result.GetData<int64_t>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_EQ(out[i], static_cast<int64_t>(ages[i]));
	}
}

TEST_F(VectorizedExpressionTest, CastInt32ToDouble) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	auto cast = std::make_unique<VecCastExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    DataType{DataTypeId::kDouble});

	DataType double_type{DataTypeId::kDouble};
	GraphVector result{double_type};
	cast->Evaluate(chunk, count, result, ctx_);

	auto* out = result.GetData<double>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		EXPECT_DOUBLE_EQ(out[i], static_cast<double>(ages[i]));
	}
}

// ============================================================
// Null handling tests
// ============================================================

TEST_F(VectorizedExpressionTest, IsNullAndIsNotNull) {
	// Build a chunk with a bool column that has some nulls
	DataType int_type{DataTypeId::kInt32};
	GraphDataChunk chunk;
	GraphVector vec{int_type};
	auto* data = vec.GetData<int32_t>();
	for (size_t i = 0; i < 10; i++) {
		data[i] = static_cast<int32_t>(i);
	}
	// Mark rows 2, 5, 8 as null
	vec.validity().SetInvalid(2);
	vec.validity().SetInvalid(5);
	vec.validity().SetInvalid(8);

	chunk.AddColumn(1, std::move(vec));
	chunk.SetCardinality(10);

	// IS NULL
	auto is_null = std::make_unique<VecIsNullExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    false);

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	is_null->Evaluate(chunk, 10, result, ctx_);

	auto* flags = result.GetData<bool>();
	for (size_t i = 0; i < 10; i++) {
		bool expected = (i == 2 || i == 5 || i == 8);
		EXPECT_EQ(flags[i], expected) << "IS NULL mismatch at row " << i;
	}

	// IS NOT NULL
	auto is_not_null = std::make_unique<VecIsNullExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    true);

	GraphVector result2{bool_type};
	is_not_null->Evaluate(chunk, 10, result2, ctx_);

	auto* flags2 = result2.GetData<bool>();
	for (size_t i = 0; i < 10; i++) {
		bool expected = !(i == 2 || i == 5 || i == 8);
		EXPECT_EQ(flags2[i], expected) << "IS NOT NULL mismatch at row " << i;
	}
}

TEST_F(VectorizedExpressionTest, NullPropagation) {
	// Build a chunk with a column that has nulls at rows 1, 3
	DataType int_type{DataTypeId::kInt32};
	GraphDataChunk chunk;
	GraphVector vec{int_type};
	auto* data = vec.GetData<int32_t>();
	for (size_t i = 0; i < 6; i++) {
		data[i] = static_cast<int32_t>(i * 10);
	}
	vec.validity().SetInvalid(1);
	vec.validity().SetInvalid(3);

	chunk.AddColumn(1, std::move(vec));
	chunk.SetCardinality(6);

	// Arith: col + 5 should propagate nulls
	auto arith = std::make_unique<VecArithExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    ArithOp::kAdd,
	    std::make_unique<VecConstantExpr>(Value::INT32(5)));

	GraphVector arith_result{int_type};
	arith->Evaluate(chunk, 6, arith_result, ctx_);

	EXPECT_TRUE(arith_result.validity().IsValid(0));
	EXPECT_FALSE(arith_result.validity().IsValid(1));
	EXPECT_TRUE(arith_result.validity().IsValid(2));
	EXPECT_FALSE(arith_result.validity().IsValid(3));
	EXPECT_EQ(arith_result.GetData<int32_t>()[0], 5);
	EXPECT_EQ(arith_result.GetData<int32_t>()[2], 25);

	// Comparison: col > 15 should propagate nulls
	auto cmp = std::make_unique<VecComparisonExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    CompareOp::kGt,
	    std::make_unique<VecConstantExpr>(Value::INT32(15)));

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector cmp_result{bool_type};
	cmp->Evaluate(chunk, 6, cmp_result, ctx_);

	EXPECT_TRUE(cmp_result.validity().IsValid(0));
	EXPECT_FALSE(cmp_result.validity().IsValid(1));
	EXPECT_TRUE(cmp_result.validity().IsValid(2));
	EXPECT_FALSE(cmp_result.validity().IsValid(3));
}

// ============================================================
// String function tests
// ============================================================

TEST_F(VectorizedExpressionTest, StringStartsWith) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "name", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// name STARTS WITH "person_1"
	auto starts_with = std::make_unique<VecStringFuncExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kVarchar}),
	    std::make_unique<VecConstantExpr>(Value::STRING("person_1")),
	    StringFuncOp::kStartsWith);

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	starts_with->Evaluate(chunk, count, result, ctx_);

	auto* flags = result.GetData<bool>();
	auto* names = StringVector::GetStringData(chunk.GetVectorByTag(1));
	size_t match_count = 0;
	for (size_t i = 0; i < count; i++) {
		std::string name = names[i].GetString();
		bool expected = name.compare(0, 8, "person_1") == 0;
		EXPECT_EQ(flags[i], expected) << "STARTS WITH mismatch for " << name;
		if (flags[i]) match_count++;
	}
	EXPECT_GT(match_count, 0u);
}

TEST_F(VectorizedExpressionTest, StringContains) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "name", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// name CONTAINS "son_5"
	auto contains = std::make_unique<VecStringFuncExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kVarchar}),
	    std::make_unique<VecConstantExpr>(Value::STRING("son_5")),
	    StringFuncOp::kContains);

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	contains->Evaluate(chunk, count, result, ctx_);

	auto* flags = result.GetData<bool>();
	auto* names = StringVector::GetStringData(chunk.GetVectorByTag(1));
	size_t match_count = 0;
	for (size_t i = 0; i < count; i++) {
		std::string name = names[i].GetString();
		bool expected = name.find("son_5") != std::string::npos;
		EXPECT_EQ(flags[i], expected) << "CONTAINS mismatch for " << name;
		if (flags[i]) match_count++;
	}
	EXPECT_GT(match_count, 0u);
}

// ============================================================
// IN list test
// ============================================================

TEST_F(VectorizedExpressionTest, InListInt32) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// age IN [25, 35, 45]
	std::vector<Value> list = {Value::INT32(25), Value::INT32(35),
	                           Value::INT32(45)};
	auto in_expr = std::make_unique<VecInListExpr>(
	    std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	    std::move(list));

	DataType bool_type{DataTypeId::kBoolean};
	GraphVector result{bool_type};
	in_expr->Evaluate(chunk, count, result, ctx_);

	auto* flags = result.GetData<bool>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		bool expected = (ages[i] == 25 || ages[i] == 35 || ages[i] == 45);
		EXPECT_EQ(flags[i], expected) << "IN mismatch at age=" << ages[i];
	}
}

// ============================================================
// Full pipeline integration tests with new expressions
// ============================================================

TEST_F(VectorizedExpressionTest, FilterWithArith) {
	// WHERE 1 + age > 50 (equivalent to age > 49)
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator read_age(0, "age", 1);

	auto predicate = std::make_unique<VecComparisonExpr>(
	    std::make_unique<VecArithExpr>(
	        std::make_unique<VecConstantExpr>(Value::INT32(1)),
	        ArithOp::kAdd,
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32})),
	    CompareOp::kGt,
	    std::make_unique<VecConstantExpr>(Value::INT32(50)));
	VecExprFilterOperator filter(std::move(predicate));

	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&read_age);
	pipeline.operators.push_back(&filter);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	// 1 + age > 50 means age >= 50, i.e. 20 + i%40 >= 50 → i%40 >= 30
	size_t expected = 0;
	for (size_t i = 0; i < kNumPersons; i++) {
		int32_t age = static_cast<int32_t>(20 + (i % 40));
		if (1 + age > 50) expected++;
	}
	EXPECT_EQ(result_state->total_rows, expected);
	EXPECT_GT(expected, 0u);
}

TEST_F(VectorizedExpressionTest, CaseWhenExpr) {
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator prop_read(0, "age", 1);
	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&prop_read);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	auto& chunk = result_state->chunks[0];
	size_t count = chunk.size();

	// CASE WHEN age > 50 THEN 1 WHEN age > 30 THEN 2 ELSE 3 END
	std::vector<VecCaseWhenExpr::WhenThen> when_thens;
	when_thens.emplace_back(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(50))),
	    std::make_unique<VecConstantExpr>(Value::INT32(1)));
	when_thens.emplace_back(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(30))),
	    std::make_unique<VecConstantExpr>(Value::INT32(2)));

	auto case_expr = std::make_unique<VecCaseWhenExpr>(
	    std::move(when_thens),
	    std::make_unique<VecConstantExpr>(Value::INT32(3)),
	    DataType{DataTypeId::kInt32});

	DataType int_type{DataTypeId::kInt32};
	GraphVector result{int_type};
	case_expr->Evaluate(chunk, count, result, ctx_);

	auto* out = result.GetData<int32_t>();
	auto* ages = chunk.GetVectorByTag(1).GetData<int32_t>();
	for (size_t i = 0; i < count; i++) {
		int32_t expected;
		if (ages[i] > 50)
			expected = 1;
		else if (ages[i] > 30)
			expected = 2;
		else
			expected = 3;
		EXPECT_EQ(out[i], expected)
		    << "CASE WHEN mismatch at row " << i << " age=" << ages[i];
	}
}

TEST_F(VectorizedExpressionTest, CaseWhenNoElse) {
	// CASE WHEN age > 50 THEN 1 END (no else → null for non-matching rows)
	DataType int_type{DataTypeId::kInt32};
	GraphDataChunk chunk;
	GraphVector vec{int_type};
	auto* data = vec.GetData<int32_t>();
	data[0] = 60;
	data[1] = 40;
	data[2] = 55;
	data[3] = 20;
	chunk.AddColumn(1, std::move(vec));
	chunk.SetCardinality(4);

	std::vector<VecCaseWhenExpr::WhenThen> when_thens;
	when_thens.emplace_back(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(50))),
	    std::make_unique<VecConstantExpr>(Value::INT32(99)));

	auto case_expr = std::make_unique<VecCaseWhenExpr>(
	    std::move(when_thens), nullptr, DataType{DataTypeId::kInt32});

	GraphVector result{int_type};
	case_expr->Evaluate(chunk, 4, result, ctx_);

	auto* out = result.GetData<int32_t>();
	// Row 0 (60>50) → 99, Row 1 (40) → null, Row 2 (55>50) → 99, Row 3 (20) → null
	EXPECT_EQ(out[0], 99);
	EXPECT_TRUE(result.validity().IsValid(0));
	EXPECT_FALSE(result.validity().IsValid(1));
	EXPECT_EQ(out[2], 99);
	EXPECT_TRUE(result.validity().IsValid(2));
	EXPECT_FALSE(result.validity().IsValid(3));
}

TEST_F(VectorizedExpressionTest, FilterComplex) {
	// WHERE age > 30 AND name STARTS WITH "person_5"
	VertexScanSource source({person_label_}, 0);
	PropertyReadOperator read_age(0, "age", 1);
	PropertyReadOperator read_name(0, "name", 2);

	auto predicate = std::make_unique<VecBooleanAndExpr>(
	    std::make_unique<VecComparisonExpr>(
	        std::make_unique<VecColumnRefExpr>(1, DataType{DataTypeId::kInt32}),
	        CompareOp::kGt,
	        std::make_unique<VecConstantExpr>(Value::INT32(30))),
	    std::make_unique<VecStringFuncExpr>(
	        std::make_unique<VecColumnRefExpr>(2, DataType{DataTypeId::kVarchar}),
	        std::make_unique<VecConstantExpr>(Value::STRING("person_5")),
	        StringFuncOp::kStartsWith));
	VecExprFilterOperator filter(std::move(predicate));

	auto result_state = std::make_shared<ExprResultSink::State>();
	ExprResultSink sink(result_state);

	VecPipeline pipeline;
	pipeline.source = &source;
	pipeline.operators.push_back(&read_age);
	pipeline.operators.push_back(&read_name);
	pipeline.operators.push_back(&filter);
	pipeline.sink = &sink;
	VecExecContext exec_ctx{nullptr, reader_.get()};
	pipeline.ctx = &exec_ctx;
	PipelineExecutor executor;
	executor.Execute(pipeline);

	// Count: age > 30 AND name starts with "person_5"
	// age = 20 + i%40, name = "person_" + i
	// name starts with "person_5" means i in {5, 50..59}
	// age > 30 means i%40 > 10
	size_t expected = 0;
	for (size_t i = 0; i < kNumPersons; i++) {
		int32_t age = static_cast<int32_t>(20 + (i % 40));
		std::string name = "person_" + std::to_string(i);
		if (age > 30 && name.compare(0, 8, "person_5") == 0) {
			expected++;
		}
	}
	EXPECT_EQ(result_state->total_rows, expected);
}
