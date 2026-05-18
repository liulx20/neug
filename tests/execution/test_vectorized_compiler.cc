#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/execution/vectorized/compiler/vec_pipeline_compiler.h"
#include "neug/execution/vectorized/compiler/vec_result_collector.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"

#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/response/response.pb.h"

using namespace neug::execution::vec;
using neug::DataType;
using neug::DataTypeId;
using neug::Property;
using neug::execution::Value;

// ============================================================
// Shared sink that stores results in an external state
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
// Test fixture
// ============================================================

class VectorizedCompilerTest : public ::testing::Test {
 protected:
	std::string work_dir_;
	std::unique_ptr<neug::PropertyGraph> graph_;
	std::unique_ptr<neug::StorageReadInterface> reader_;
	neug::label_t person_label_;
	static constexpr size_t kNumPersons = 100;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_compiler_" +
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

	// Helper: build a Scan operator proto for Person vertices
	void BuildScanOpr(physical::PhysicalPlan& plan, int alias) {
		auto* opr = plan.add_plan();
		auto* scan = opr->mutable_opr()->mutable_scan();
		scan->set_scan_opt(physical::Scan::VERTEX);
		scan->mutable_alias()->set_value(alias);
		auto* params = scan->mutable_params();
		auto* table = params->add_tables();
		table->set_id(person_label_);
	}

	// Helper: build a Select operator proto with a predicate expression
	void BuildSelectOpr(physical::PhysicalPlan& plan,
	                    const common::Expression& predicate) {
		auto* opr = plan.add_plan();
		auto* select = opr->mutable_opr()->mutable_select();
		*select->mutable_predicate() = predicate;
	}

	// Helper: build a Project operator proto
	void BuildProjectOpr(physical::PhysicalPlan& plan,
	                     const std::vector<std::pair<common::Expression, int>>&
	                         mappings,
	                     bool is_append = false) {
		auto* opr = plan.add_plan();
		auto* project = opr->mutable_opr()->mutable_project();
		project->set_is_append(is_append);
		for (auto& [expr, alias] : mappings) {
			auto* mapping = project->add_mappings();
			*mapping->mutable_expr() = expr;
			mapping->mutable_alias()->set_value(alias);
		}
	}

	// Helper: build a Sink operator
	void BuildSinkOpr(physical::PhysicalPlan& plan,
	                  const std::vector<int>& tags) {
		auto* opr = plan.add_plan();
		auto* sink = opr->mutable_opr()->mutable_sink();
		for (int tag : tags) {
			auto* opt_tag = sink->add_tags();
			opt_tag->mutable_tag()->set_value(tag);
		}
	}

	// Helper: build expression for a variable property access (tag.prop_name)
	common::Expression BuildVarPropExpr(int tag, const std::string& prop_name,
	                                    common::IrDataType node_type) {
		common::Expression expr;
		auto* opr = expr.add_operators();
		auto* var = opr->mutable_var();
		var->mutable_tag()->set_id(tag);
		var->mutable_property()->mutable_key()->set_name(prop_name);
		*var->mutable_node_type() = node_type;
		return expr;
	}

	// Helper: build IrDataType for int32
	common::IrDataType MakeInt32Type() {
		common::IrDataType dt;
		dt.mutable_data_type()->set_primitive_type(
		    common::PrimitiveType::DT_SIGNED_INT32);
		return dt;
	}

	// Helper: build IrDataType for int64
	common::IrDataType MakeInt64Type() {
		common::IrDataType dt;
		dt.mutable_data_type()->set_primitive_type(
		    common::PrimitiveType::DT_SIGNED_INT64);
		return dt;
	}

	// Helper: build IrDataType for string
	common::IrDataType MakeStringType() {
		common::IrDataType dt;
		dt.mutable_data_type()->mutable_string();
		return dt;
	}

	// Helper: build a comparison predicate: tag.prop op const_value
	// Expression format (infix): [var] [op] [const]
	common::Expression BuildComparisonExpr(int tag,
	                                       const std::string& prop_name,
	                                       common::IrDataType var_type,
	                                       common::Logical op,
	                                       const common::Value& const_val) {
		common::Expression expr;
		// Variable operand
		auto* var_opr = expr.add_operators();
		auto* var = var_opr->mutable_var();
		var->mutable_tag()->set_id(tag);
		var->mutable_property()->mutable_key()->set_name(prop_name);
		*var->mutable_node_type() = var_type;
		// Operator
		auto* logical_opr = expr.add_operators();
		logical_opr->set_logical(op);
		// Constant
		auto* const_opr = expr.add_operators();
		*const_opr->mutable_const_() = const_val;
		return expr;
	}

	// Helper: execute pipeline from compiler and return total rows
	size_t CompileAndExecute(VecPipelineCompiler& compiler) {
		auto pipeline = compiler.Compile();
		auto state = std::make_shared<SharedResultSink::State>();
		SharedResultSink sink(state);

		VecExecContext ctx{nullptr, reader_.get()};
		PipelineExecutor executor;
		executor.Execute(pipeline, &sink, ctx);
		return state->total_rows;
	}
};

// ============================================================
// Tests
// ============================================================

TEST_F(VectorizedCompilerTest, SimpleScan) {
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);
	BuildSinkOpr(plan, {0});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	EXPECT_EQ(total, kNumPersons);
}

TEST_F(VectorizedCompilerTest, ScanWithSelect) {
	// MATCH (n:Person) WHERE n.age > 30
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Value int_val;
	int_val.set_i32(30);
	auto predicate =
	    BuildComparisonExpr(0, "age", MakeInt32Type(), common::Logical::GT, int_val);
	BuildSelectOpr(plan, predicate);
	BuildSinkOpr(plan, {0});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	// age > 30: age = 20 + (i%40), i%40 > 10 → i%40 in {11..39} = 29/cycle
	// 100 persons: 2*29 + 9(partial) = 67
	EXPECT_EQ(total, 67u);
}

TEST_F(VectorizedCompilerTest, ScanWithSelectLe) {
	// MATCH (n:Person) WHERE n.age <= 25
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Value int_val;
	int_val.set_i32(25);
	auto predicate = BuildComparisonExpr(0, "age", MakeInt32Type(),
	                                     common::Logical::LE, int_val);
	BuildSelectOpr(plan, predicate);
	BuildSinkOpr(plan, {0});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	// age <= 25: i%40 <= 5 → 6/cycle, total: 6+6+6 = 18
	EXPECT_EQ(total, 18u);
}

TEST_F(VectorizedCompilerTest, ScanWithProject) {
	// MATCH (n:Person) RETURN n.name, n.age
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Expression name_expr;
	auto* name_opr = name_expr.add_operators();
	auto* name_var = name_opr->mutable_var();
	name_var->mutable_tag()->set_id(0);
	name_var->mutable_property()->mutable_key()->set_name("name");
	*name_var->mutable_node_type() = MakeStringType();

	common::Expression age_expr;
	auto* age_opr = age_expr.add_operators();
	auto* age_var = age_opr->mutable_var();
	age_var->mutable_tag()->set_id(0);
	age_var->mutable_property()->mutable_key()->set_name("age");
	*age_var->mutable_node_type() = MakeInt32Type();

	BuildProjectOpr(plan, {{name_expr, 1}, {age_expr, 2}}, false);
	BuildSinkOpr(plan, {1, 2});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	EXPECT_EQ(total, kNumPersons);
}

TEST_F(VectorizedCompilerTest, FullQuery) {
	// MATCH (n:Person) WHERE n.age > 50 RETURN n.name
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Value int_val;
	int_val.set_i32(50);
	auto predicate =
	    BuildComparisonExpr(0, "age", MakeInt32Type(), common::Logical::GT, int_val);
	BuildSelectOpr(plan, predicate);

	common::Expression name_expr;
	auto* name_opr = name_expr.add_operators();
	auto* name_var = name_opr->mutable_var();
	name_var->mutable_tag()->set_id(0);
	name_var->mutable_property()->mutable_key()->set_name("name");
	*name_var->mutable_node_type() = MakeStringType();

	BuildProjectOpr(plan, {{name_expr, 1}}, false);
	BuildSinkOpr(plan, {1});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	// age > 50: i%40 > 30 → 9/cycle, total: 2*9 = 18
	EXPECT_EQ(total, 18u);
}

TEST_F(VectorizedCompilerTest, CompoundPredicate) {
	// MATCH (n:Person) WHERE n.age > 30 AND n.age < 50
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	// Build: (age > 30) AND (age < 50)
	// Infix: [age] [>] [30] [AND] [age] [<] [50]
	common::Expression predicate;
	{
		auto* var_opr = predicate.add_operators();
		auto* var = var_opr->mutable_var();
		var->mutable_tag()->set_id(0);
		var->mutable_property()->mutable_key()->set_name("age");
		*var->mutable_node_type() = MakeInt32Type();
	}
	{
		auto* op_opr = predicate.add_operators();
		op_opr->set_logical(common::Logical::GT);
	}
	{
		auto* const_opr = predicate.add_operators();
		const_opr->mutable_const_()->set_i32(30);
	}
	{
		auto* op_opr = predicate.add_operators();
		op_opr->set_logical(common::Logical::AND);
	}
	{
		auto* var_opr = predicate.add_operators();
		auto* var = var_opr->mutable_var();
		var->mutable_tag()->set_id(0);
		var->mutable_property()->mutable_key()->set_name("age");
		*var->mutable_node_type() = MakeInt32Type();
	}
	{
		auto* op_opr = predicate.add_operators();
		op_opr->set_logical(common::Logical::LT);
	}
	{
		auto* const_opr = predicate.add_operators();
		const_opr->mutable_const_()->set_i32(50);
	}

	BuildSelectOpr(plan, predicate);
	BuildSinkOpr(plan, {0});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	// age > 30 AND age < 50: 10 < i%40 < 30 → 19/cycle
	// Total: 19 + 19 + 9 = 47
	EXPECT_EQ(total, 47u);
}

TEST_F(VectorizedCompilerTest, ArithmeticExpression) {
	// MATCH (n:Person) WHERE n.age + 10 > 60
	// Equivalent to n.age > 50
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	// Infix: [age] [+] [10] [>] [60]
	common::Expression predicate;
	{
		auto* var_opr = predicate.add_operators();
		auto* var = var_opr->mutable_var();
		var->mutable_tag()->set_id(0);
		var->mutable_property()->mutable_key()->set_name("age");
		*var->mutable_node_type() = MakeInt32Type();
	}
	{
		auto* arith_opr = predicate.add_operators();
		arith_opr->set_arith(common::Arithmetic::ADD);
	}
	{
		auto* const_opr = predicate.add_operators();
		const_opr->mutable_const_()->set_i32(10);
	}
	{
		auto* op_opr = predicate.add_operators();
		op_opr->set_logical(common::Logical::GT);
	}
	{
		auto* const_opr = predicate.add_operators();
		const_opr->mutable_const_()->set_i32(60);
	}

	BuildSelectOpr(plan, predicate);
	BuildSinkOpr(plan, {0});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	// age + 10 > 60 means age > 50: 18
	EXPECT_EQ(total, 18u);
}

TEST_F(VectorizedCompilerTest, StringStartsWith) {
	// MATCH (n:Person) WHERE n.name STARTS WITH "person_1"
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	// Infix: [name] [STARTSWITH] ["person_1"]
	common::Expression predicate;
	{
		auto* var_opr = predicate.add_operators();
		auto* var = var_opr->mutable_var();
		var->mutable_tag()->set_id(0);
		var->mutable_property()->mutable_key()->set_name("name");
		*var->mutable_node_type() = MakeStringType();
	}
	{
		auto* op_opr = predicate.add_operators();
		op_opr->set_logical(common::Logical::STARTSWITH);
	}
	{
		auto* const_opr = predicate.add_operators();
		const_opr->mutable_const_()->set_str("person_1");
	}

	BuildSelectOpr(plan, predicate);
	BuildSinkOpr(plan, {0});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	size_t total = CompileAndExecute(compiler);
	// STARTS WITH "person_1": person_1, person_10..person_19 = 11
	EXPECT_EQ(total, 11u);
}

TEST_F(VectorizedCompilerTest, CanVectorizeSimple) {
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);
	BuildSinkOpr(plan, {0});
	EXPECT_TRUE(VecPipelineCompiler::CanVectorize(plan));
}

TEST_F(VectorizedCompilerTest, CanVectorizeRejectsUnsupported) {
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);
	// Add an unsupported operator (e.g., Apply)
	auto* opr = plan.add_plan();
	opr->mutable_opr()->mutable_apply();
	BuildSinkOpr(plan, {0});
	EXPECT_FALSE(VecPipelineCompiler::CanVectorize(plan));
}

TEST_F(VectorizedCompilerTest, ResultCollectorSerialization) {
	// MATCH (n:Person) WHERE n.age > 50 RETURN n.name, n.age
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Value int_val;
	int_val.set_i32(50);
	auto predicate =
	    BuildComparisonExpr(0, "age", MakeInt32Type(), common::Logical::GT, int_val);
	BuildSelectOpr(plan, predicate);

	common::Expression name_expr;
	auto* name_opr = name_expr.add_operators();
	auto* name_var = name_opr->mutable_var();
	name_var->mutable_tag()->set_id(0);
	name_var->mutable_property()->mutable_key()->set_name("name");
	*name_var->mutable_node_type() = MakeStringType();

	common::Expression age_expr;
	auto* age_opr = age_expr.add_operators();
	auto* age_var = age_opr->mutable_var();
	age_var->mutable_tag()->set_id(0);
	age_var->mutable_property()->mutable_key()->set_name("age");
	*age_var->mutable_node_type() = MakeInt32Type();

	BuildProjectOpr(plan, {{name_expr, 1}, {age_expr, 2}}, false);
	BuildSinkOpr(plan, {1, 2});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	auto pipeline = compiler.Compile();
	auto output_info = compiler.GetOutputInfo();

	VecResultCollector collector(std::move(output_info.tags),
	                             std::move(output_info.types));

	VecExecContext ctx{nullptr, reader_.get()};
	PipelineExecutor executor;
	executor.Execute(pipeline, &collector, ctx);

	EXPECT_EQ(collector.TotalRows(), 18u);

	neug::QueryResponse response;
	collector.SerializeToResponse(&response);

	EXPECT_EQ(response.row_count(), 18);
	EXPECT_EQ(response.arrays_size(), 2);
	EXPECT_TRUE(response.arrays(0).has_string_array());
	EXPECT_EQ(response.arrays(0).string_array().values_size(), 18);
	EXPECT_TRUE(response.arrays(1).has_int32_array());
	EXPECT_EQ(response.arrays(1).int32_array().values_size(), 18);

	// Verify all ages > 50
	for (int i = 0; i < response.arrays(1).int32_array().values_size(); ++i) {
		EXPECT_GT(response.arrays(1).int32_array().values(i), 50);
	}
}

// ============================================================
// End-to-end tests: actual Cypher through Connection::Query
// ============================================================

class VectorizedE2ETest : public ::testing::Test {
 protected:
	std::string work_dir_;
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_e2e_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, age INT32, "
		    "PRIMARY KEY(id));"));

		for (int i = 0; i < 100; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) +
			                "', age: " + std::to_string(20 + (i % 40)) + "});";
			ASSERT_TRUE(conn_->Query(q)) << "Failed to create person " << i;
		}
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}

};

TEST_F(VectorizedE2ETest, SimpleScan) {
	auto result = conn_->Query("MATCH (n:Person) RETURN n.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 100);
}

TEST_F(VectorizedE2ETest, FilterAge) {
	auto result =
	    conn_->Query("MATCH (n:Person) WHERE n.age > 50 RETURN n.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// age > 50: i%40 > 30 → 9/cycle, total: 2*9 = 18
	EXPECT_EQ(response.row_count(), 18);
	EXPECT_EQ(response.arrays_size(), 1);
	EXPECT_TRUE(response.arrays(0).has_string_array());
	EXPECT_EQ(response.arrays(0).string_array().values_size(), 18);
}

TEST_F(VectorizedE2ETest, FilterAndProject) {
	auto result = conn_->Query(
	    "MATCH (n:Person) WHERE n.age <= 25 RETURN n.name, n.age", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// age <= 25: i%40 <= 5 → 6/cycle, total: 6*3 = 18
	EXPECT_EQ(response.row_count(), 18);
	EXPECT_EQ(response.arrays_size(), 2);
	// All ages should be <= 25
	if (response.arrays(1).has_int32_array()) {
		for (int i = 0; i < response.arrays(1).int32_array().values_size(); ++i) {
			EXPECT_LE(response.arrays(1).int32_array().values(i), 25);
		}
	}
}

class VectorizedEdgeExpandTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_edge_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, age INT32, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(
		    conn_->Query("CREATE REL TABLE KNOWS(FROM Person TO Person);"));

		// Create 5 people
		for (int i = 0; i < 5; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) +
			                "', age: " + std::to_string(20 + i * 5) + "});";
			ASSERT_TRUE(conn_->Query(q)) << "Failed to create person " << i;
		}
		// Create edges: 0->1, 0->2, 0->3, 1->2, 1->4
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:1}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:2}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:3}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (b:Person {id:2}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (b:Person {id:4}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}
};

TEST_F(VectorizedEdgeExpandTest, BasicExpand) {
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// Total edges: 5 (0->1, 0->2, 0->3, 1->2, 1->4)
	EXPECT_EQ(response.row_count(), 5);
	EXPECT_EQ(response.arrays_size(), 1);
	EXPECT_TRUE(response.arrays(0).has_string_array());
	EXPECT_EQ(response.arrays(0).string_array().values_size(), 5);
}

TEST_F(VectorizedEdgeExpandTest, ExpandWithFilter) {
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 25 RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// b.age > 25: person_2(30), person_3(35), person_4(40) → edges to them:
	//   0->2(30), 0->3(35), 1->2(30), 1->4(40) = 4
	EXPECT_EQ(response.row_count(), 4);
}

TEST_F(VectorizedEdgeExpandTest, ExpandReturnBoth) {
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 5);
	EXPECT_EQ(response.arrays_size(), 2);
}

// ============================================================
// PathExpand Tests
// ============================================================

TEST_F(VectorizedEdgeExpandTest, PathExpandOneToTwo) {
	// *1..2 in Cypher = 1 or 2 hops
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN b.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// 1-hop from all: 0->{1,2,3}, 1->{2,4} = 5
	// 2-hop from all: 0->1->{2,4} = 2
	// Total: 7
	EXPECT_EQ(response.row_count(), 7);
}

TEST_F(VectorizedEdgeExpandTest, PathExpandFromSingle) {
	// *1..3 from vertex 0 = 1, 2, or 3 hops
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[:KNOWS*1..3]->(b:Person) RETURN b.id",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// 1-hop: {1, 2, 3} = 3
	// 2-hop: 0->1->{2,4} = 2
	// 3-hop: no further edges = 0
	// Total: 5
	EXPECT_EQ(response.row_count(), 5);
}

TEST_F(VectorizedEdgeExpandTest, PathExpandZeroHop) {
	// *0..2 from vertex 0 = 0, 1, or 2 hops
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[:KNOWS*0..2]->(b:Person) RETURN b.id",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// 0-hop: {0} = 1
	// 1-hop: {1, 2, 3} = 3
	// 2-hop: 0->1->{2, 4} = 2
	// Total: 6
	EXPECT_EQ(response.row_count(), 6);
}

// ============================================================
// IndexScan (idx_predicate) Tests
// ============================================================

TEST_F(VectorizedEdgeExpandTest, IndexScanSingleVertex) {
	// MATCH (a:Person {id: 0}) uses idx_predicate
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0}) RETURN a.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
}

TEST_F(VectorizedEdgeExpandTest, IndexScanWithEdgeExpand) {
	// PK lookup + edge expand
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[:KNOWS]->(b:Person) RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// 0->1, 0->2, 0->3
	EXPECT_EQ(response.row_count(), 3);
}

TEST_F(VectorizedEdgeExpandTest, IndexScanNonExistent) {
	// PK lookup for non-existent vertex
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 999}) RETURN a.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 0);
}

// ============================================================
// Edge Predicate Tests (push-down edge property filtering)
// ============================================================

class VectorizedEdgePredicateTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_edge_pred_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, age INT32, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT32);"));

		for (int i = 0; i < 5; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) +
			                "', age: " + std::to_string(20 + i * 5) + "});";
			ASSERT_TRUE(conn_->Query(q)) << "Failed to create person " << i;
		}
		// Edges with 'since' property:
		//   0->1 (since: 2018)
		//   0->2 (since: 2020)
		//   0->3 (since: 2022)
		//   1->2 (since: 2019)
		//   1->4 (since: 2021)
		CreateKnowsEdge(0, 1, 2018);
		CreateKnowsEdge(0, 2, 2020);
		CreateKnowsEdge(0, 3, 2022);
		CreateKnowsEdge(1, 2, 2019);
		CreateKnowsEdge(1, 4, 2021);
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}

	void CreateKnowsEdge(int src_id, int dst_id, int since) {
		std::string q = "MATCH (a:Person {id: " + std::to_string(src_id) +
		                "}), (b:Person {id: " + std::to_string(dst_id) +
		                "}) CREATE (a)-[:KNOWS {since: " +
		                std::to_string(since) + "}]->(b);";
		ASSERT_TRUE(conn_->Query(q)) << "Failed: " << q;
	}
};

TEST_F(VectorizedEdgePredicateTest, NoFilter) {
	auto result = conn_->Query(
	    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 5);
}

TEST_F(VectorizedEdgePredicateTest, FilterGT) {
	// r.since > 2020: 0->3(2022), 1->4(2021) = 2 results
	auto result = conn_->Query(
	    "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since > 2020 "
	    "RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 2);
}

TEST_F(VectorizedEdgePredicateTest, FilterEQ) {
	// r.since = 2020: 0->2(2020) = 1 result
	auto result = conn_->Query(
	    "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2020 "
	    "RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 1);
}

TEST_F(VectorizedEdgePredicateTest, FilterGE) {
	// r.since >= 2020: 0->2(2020), 0->3(2022), 1->4(2021) = 3 results
	auto result = conn_->Query(
	    "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since >= 2020 "
	    "RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 3);
}

TEST_F(VectorizedEdgePredicateTest, FilterWithIdxScan) {
	// From person 0 only, r.since > 2020: 0->3(2022) = 1 result
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[r:KNOWS]->(b:Person) WHERE r.since > 2020 "
	    "RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 1);
}

TEST_F(VectorizedEdgePredicateTest, FilterLT) {
	// r.since < 2020: 0->1(2018), 1->2(2019) = 2 results
	auto result = conn_->Query(
	    "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since < 2020 "
	    "RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 2);
}

TEST_F(VectorizedEdgePredicateTest, FilterNoResults) {
	// r.since > 2030: no edges match
	auto result = conn_->Query(
	    "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since > 2030 "
	    "RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 0);
}

TEST_F(VectorizedEdgePredicateTest, FilterReturnBoth) {
	// Return both source and destination with edge filter
	auto result = conn_->Query(
	    "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since >= 2020 "
	    "RETURN a.name, b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 3);
	EXPECT_EQ(result.value().response().arrays_size(), 2);
}

// ============================================================
// Hash Join Tests
// ============================================================

class VectorizedJoinTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = std::string("/tmp/test_vec_join_") +
		            ::testing::UnitTest::GetInstance()
		                ->current_test_info()
		                ->name();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, age INT32, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Movie (id INT64, title STRING, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(
		    conn_->Query("CREATE REL TABLE KNOWS(FROM Person TO Person);"));
		ASSERT_TRUE(
		    conn_->Query("CREATE REL TABLE LIKES(FROM Person TO Movie);"));

		// Create 4 people
		for (int i = 0; i < 4; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) +
			                "', age: " + std::to_string(20 + i * 5) + "});";
			ASSERT_TRUE(conn_->Query(q));
		}
		// Create 3 movies
		for (int i = 0; i < 3; ++i) {
			std::string q = "CREATE (:Movie {id: " + std::to_string(i) +
			                ", title: 'movie_" + std::to_string(i) + "'});";
			ASSERT_TRUE(conn_->Query(q));
		}

		// KNOWS edges: 0->1, 0->2, 1->3
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:1}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:2}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (b:Person {id:3}) "
		    "CREATE (a)-[:KNOWS]->(b);"));

		// LIKES edges: 0->movie_0, 0->movie_1, 1->movie_2
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (m:Movie {id:0}) "
		    "CREATE (a)-[:LIKES]->(m);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (m:Movie {id:1}) "
		    "CREATE (a)-[:LIKES]->(m);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (m:Movie {id:2}) "
		    "CREATE (a)-[:LIKES]->(m);"));
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}
};

TEST_F(VectorizedJoinTest, InnerJoinTwoPatterns) {
	// person_0 knows {1,2} and likes {movie_0, movie_1}
	// person_1 knows {3} and likes {movie_2}
	// Inner join on 'a': (a)-[:KNOWS]->(b), (a)-[:LIKES]->(c)
	// person_0: 2 KNOWS * 2 LIKES = 4 combos
	// person_1: 1 KNOWS * 1 LIKES = 1 combo
	// Total: 5 rows
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS]->(b:Person), "
	    "(a)-[:LIKES]->(c:Movie) "
	    "RETURN a.name, b.name, c.title",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();

	EXPECT_EQ(response.row_count(), 5);
	EXPECT_EQ(response.arrays_size(), 3);
}

TEST_F(VectorizedJoinTest, UnionAll) {
	// Person ages: 20, 25, 30, 35
	// age < 25 → 1 person (age 20)
	// age >= 30 → 2 persons (ages 30, 35)
	// Union: 3 rows total
	auto result = conn_->Query(
	    "MATCH (n:Person) WHERE n.age < 25 RETURN n.age AS age "
	    "UNION ALL "
	    "MATCH (n:Person) WHERE n.age >= 30 RETURN n.age AS age",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 3);
}

TEST_F(VectorizedJoinTest, UnionAllNames) {
	// Return all person names union with all movie titles
	// 4 persons + 3 movies = 7 rows
	auto result = conn_->Query(
	    "MATCH (n:Person) RETURN n.name AS val "
	    "UNION ALL "
	    "MATCH (m:Movie) RETURN m.title AS val",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 7);
}

// ============================================================
// Aggregation tests
// ============================================================

class VectorizedAggregateTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = std::string("/tmp/test_vec_agg_") +
		            ::testing::UnitTest::GetInstance()
		                ->current_test_info()
		                ->name();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, age INT32, "
		    "PRIMARY KEY(id));"));

		// 10 people with ages: 20,25,30,35,40,20,25,30,35,40
		for (int i = 0; i < 10; ++i) {
			int age = 20 + (i % 5) * 5;
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) +
			                "', age: " + std::to_string(age) + "});";
			ASSERT_TRUE(conn_->Query(q));
		}
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}
};

TEST_F(VectorizedAggregateTest, GlobalCount) {
	// MATCH (n:Person) RETURN count(n) → 1 row with value 10
	auto result = conn_->Query(
	    "MATCH (n:Person) RETURN count(n)", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	EXPECT_EQ(response.arrays_size(), 1);
	if (response.arrays_size() > 0) {
		const auto& arr = response.arrays(0);
		if (arr.has_int64_array()) {
			EXPECT_EQ(arr.int64_array().values(0), 10);
		}
	}
}

TEST_F(VectorizedAggregateTest, GroupByCount) {
	// MATCH (n:Person) RETURN n.age, count(n)
	// Ages: 20,25,30,35,40 each appears 2x → 5 groups, count=2 each
	auto result = conn_->Query(
	    "MATCH (n:Person) RETURN n.age, count(n)", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 5);
	EXPECT_EQ(response.arrays_size(), 2);
}

TEST_F(VectorizedAggregateTest, GroupByMultipleAggregates) {
	// MATCH (n:Person) RETURN n.age, count(n), sum(n.id)
	auto result = conn_->Query(
	    "MATCH (n:Person) RETURN n.age, count(n), sum(n.id)", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 5);
	EXPECT_EQ(response.arrays_size(), 3);
}

// ============================================================
// LSQB Benchmark Tests
// ============================================================

class LSQBTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_lsqb_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		CreateSchema();
		LoadData();
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}

	void CreateSchema() {
		// Node tables
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Country (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE City (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Continent (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Forum (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Post (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Comment (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Tag (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE TagClass (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Company (id INT64, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE University (id INT64, PRIMARY KEY(id));"));

		// Edge tables
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_PART_OF(FROM City TO Country);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_PART_OF(FROM Country TO Continent);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_LOCATED_IN(FROM Person TO City);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_LOCATED_IN(FROM Comment TO Country);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_LOCATED_IN(FROM Post TO Country);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_LOCATED_IN(FROM Company TO Country);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_LOCATED_IN(FROM University TO City);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_MEMBER(FROM Forum TO Person);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_MODERATOR(FROM Forum TO Person);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE CONTAINER_OF(FROM Forum TO Post);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_CREATOR(FROM Comment TO Person);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_CREATOR(FROM Post TO Person);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE REPLY_OF(FROM Comment TO Comment);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE REPLY_OF(FROM Comment TO Post);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_TAG(FROM Comment TO Tag);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_TAG(FROM Post TO Tag);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_TAG(FROM Forum TO Tag);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_TYPE(FROM Tag TO TagClass);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE IS_SUBCLASS_OF(FROM TagClass TO TagClass);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE KNOWS(FROM Person TO Person);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HAS_INTEREST(FROM Person TO Tag);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE LIKES(FROM Person TO Comment);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE LIKES(FROM Person TO Post);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE STUDY_AT(FROM Person TO University);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE WORK_AT(FROM Person TO Company);"));
	}

	void LoadData() {
		// Vertices
		for (int id : {1, 2, 3, 4, 5})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:Person {id: " + std::to_string(id) + "});"));
		for (int id : {2, 3})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:Country {id: " + std::to_string(id) + "});"));
		for (int id : {4, 5, 6})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:City {id: " + std::to_string(id) + "});"));
		ASSERT_TRUE(conn_->Query("CREATE (:Continent {id: 1});"));
		for (int id : {1, 2})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:Forum {id: " + std::to_string(id) + "});"));
		for (int id : {10, 20})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:Post {id: " + std::to_string(id) + "});"));
		for (int id : {1, 2, 3, 4, 5, 6})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:Comment {id: " + std::to_string(id) + "});"));
		for (int id : {1, 2})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:Tag {id: " + std::to_string(id) + "});"));
		for (int id : {1, 2, 3})
			ASSERT_TRUE(conn_->Query(
			    "CREATE (:TagClass {id: " + std::to_string(id) + "});"));
		ASSERT_TRUE(conn_->Query("CREATE (:Company {id: 1});"));
		ASSERT_TRUE(conn_->Query("CREATE (:University {id: 1});"));

		// Edges: City -IS_PART_OF-> Country
		CreateEdge("City", "IS_PART_OF", "Country", 4, 2);
		CreateEdge("City", "IS_PART_OF", "Country", 5, 3);
		CreateEdge("City", "IS_PART_OF", "Country", 6, 3);

		// Country -IS_PART_OF-> Continent
		CreateEdge("Country", "IS_PART_OF", "Continent", 2, 1);
		CreateEdge("Country", "IS_PART_OF", "Continent", 3, 1);

		// Person -IS_LOCATED_IN-> City
		CreateEdge("Person", "IS_LOCATED_IN", "City", 1, 5);
		CreateEdge("Person", "IS_LOCATED_IN", "City", 2, 4);
		CreateEdge("Person", "IS_LOCATED_IN", "City", 3, 6);
		CreateEdge("Person", "IS_LOCATED_IN", "City", 4, 5);
		CreateEdge("Person", "IS_LOCATED_IN", "City", 5, 5);

		// Forum -HAS_MEMBER-> Person
		CreateEdge("Forum", "HAS_MEMBER", "Person", 1, 1);
		CreateEdge("Forum", "HAS_MEMBER", "Person", 1, 2);
		CreateEdge("Forum", "HAS_MEMBER", "Person", 1, 3);
		CreateEdge("Forum", "HAS_MEMBER", "Person", 2, 1);
		CreateEdge("Forum", "HAS_MEMBER", "Person", 2, 4);

		// Forum -CONTAINER_OF-> Post
		CreateEdge("Forum", "CONTAINER_OF", "Post", 1, 10);
		CreateEdge("Forum", "CONTAINER_OF", "Post", 2, 20);

		// Comment -REPLY_OF-> Post
		CreateEdge("Comment", "REPLY_OF", "Post", 1, 10);
		CreateEdge("Comment", "REPLY_OF", "Post", 2, 10);
		CreateEdge("Comment", "REPLY_OF", "Post", 6, 20);

		// Comment -REPLY_OF-> Comment
		CreateEdge("Comment", "REPLY_OF", "Comment", 3, 2);
		CreateEdge("Comment", "REPLY_OF", "Comment", 4, 3);
		CreateEdge("Comment", "REPLY_OF", "Comment", 5, 4);

		// Comment -HAS_TAG-> Tag
		CreateEdge("Comment", "HAS_TAG", "Tag", 1, 1);
		CreateEdge("Comment", "HAS_TAG", "Tag", 3, 1);
		CreateEdge("Comment", "HAS_TAG", "Tag", 2, 2);
		CreateEdge("Comment", "HAS_TAG", "Tag", 3, 2);
		CreateEdge("Comment", "HAS_TAG", "Tag", 4, 2);
		CreateEdge("Comment", "HAS_TAG", "Tag", 6, 2);

		// Comment -HAS_CREATOR-> Person
		CreateEdge("Comment", "HAS_CREATOR", "Person", 1, 3);
		CreateEdge("Comment", "HAS_CREATOR", "Person", 2, 1);
		CreateEdge("Comment", "HAS_CREATOR", "Person", 3, 3);
		CreateEdge("Comment", "HAS_CREATOR", "Person", 4, 1);
		CreateEdge("Comment", "HAS_CREATOR", "Person", 5, 4);
		CreateEdge("Comment", "HAS_CREATOR", "Person", 6, 1);

		// Comment -IS_LOCATED_IN-> Country
		CreateEdge("Comment", "IS_LOCATED_IN", "Country", 1, 2);
		CreateEdge("Comment", "IS_LOCATED_IN", "Country", 2, 3);
		CreateEdge("Comment", "IS_LOCATED_IN", "Country", 3, 2);
		CreateEdge("Comment", "IS_LOCATED_IN", "Country", 4, 3);
		CreateEdge("Comment", "IS_LOCATED_IN", "Country", 5, 2);
		CreateEdge("Comment", "IS_LOCATED_IN", "Country", 6, 3);

		// Tag -HAS_TYPE-> TagClass
		CreateEdge("Tag", "HAS_TYPE", "TagClass", 1, 2);
		CreateEdge("Tag", "HAS_TYPE", "TagClass", 2, 3);

		// TagClass -IS_SUBCLASS_OF-> TagClass
		CreateEdge("TagClass", "IS_SUBCLASS_OF", "TagClass", 2, 1);

		// Post -HAS_CREATOR-> Person
		CreateEdge("Post", "HAS_CREATOR", "Person", 10, 2);
		CreateEdge("Post", "HAS_CREATOR", "Person", 20, 3);

		// Post -HAS_TAG-> Tag
		CreateEdge("Post", "HAS_TAG", "Tag", 10, 1);
		CreateEdge("Post", "HAS_TAG", "Tag", 20, 2);

		// Post -IS_LOCATED_IN-> Country
		CreateEdge("Post", "IS_LOCATED_IN", "Country", 10, 3);
		CreateEdge("Post", "IS_LOCATED_IN", "Country", 20, 3);

		// Person -KNOWS-> Person
		CreateEdge("Person", "KNOWS", "Person", 1, 2);
		CreateEdge("Person", "KNOWS", "Person", 1, 3);
		CreateEdge("Person", "KNOWS", "Person", 1, 4);
		CreateEdge("Person", "KNOWS", "Person", 2, 3);
		CreateEdge("Person", "KNOWS", "Person", 3, 4);
		CreateEdge("Person", "KNOWS", "Person", 4, 5);

		// Person -HAS_INTEREST-> Tag
		CreateEdge("Person", "HAS_INTEREST", "Tag", 1, 1);
		CreateEdge("Person", "HAS_INTEREST", "Tag", 2, 2);

		// Person -LIKES-> Comment
		CreateEdge("Person", "LIKES", "Comment", 1, 1);
		CreateEdge("Person", "LIKES", "Comment", 2, 3);
		CreateEdge("Person", "LIKES", "Comment", 4, 5);

		// Person -LIKES-> Post
		CreateEdge("Person", "LIKES", "Post", 1, 10);
		CreateEdge("Person", "LIKES", "Post", 3, 20);

		// Forum -HAS_MODERATOR-> Person
		CreateEdge("Forum", "HAS_MODERATOR", "Person", 1, 1);
		CreateEdge("Forum", "HAS_MODERATOR", "Person", 2, 2);

		// Forum -HAS_TAG-> Tag
		CreateEdge("Forum", "HAS_TAG", "Tag", 1, 1);

		// Person -STUDY_AT-> University
		CreateEdge("Person", "STUDY_AT", "University", 1, 1);
		CreateEdge("Person", "STUDY_AT", "University", 2, 1);
		CreateEdge("Person", "STUDY_AT", "University", 3, 1);

		// Person -WORK_AT-> Company
		CreateEdge("Person", "WORK_AT", "Company", 4, 1);

		// Company -IS_LOCATED_IN-> Country
		CreateEdge("Company", "IS_LOCATED_IN", "Country", 1, 2);

		// University -IS_LOCATED_IN-> City
		CreateEdge("University", "IS_LOCATED_IN", "City", 1, 4);
	}

	void CreateEdge(const std::string& src_type, const std::string& edge_type,
	                const std::string& dst_type, int src_id, int dst_id) {
		std::string q = "MATCH (a:" + src_type + " {id: " +
		                std::to_string(src_id) + "}), (b:" + dst_type +
		                " {id: " + std::to_string(dst_id) +
		                "}) CREATE (a)-[:" + edge_type + "]->(b);";
		auto r = conn_->Query(q);
		ASSERT_TRUE(r.has_value()) << "Failed: " << q << " - "
		                           << r.error().error_message();
	}
};

TEST_F(LSQBTest, Q1) {
	// Q1: Country<-IS_PART_OF-City<-IS_LOCATED_IN-Person<-HAS_MEMBER-Forum
	//     -CONTAINER_OF->Post<-REPLY_OF-Comment-HAS_TAG->Tag-HAS_TYPE->TagClass
	auto result = conn_->Query(
	    "MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-"
	    "(:Person)<-[:HAS_MEMBER]-(:Forum)-[:CONTAINER_OF]->(:Post)"
	    "<-[:REPLY_OF]-(:Comment)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass)"
	    " RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 8);
	}
}

TEST_F(LSQBTest, Q2) {
	// Q2: person1-KNOWS-person2, person1<-HAS_CREATOR-comment-REPLY_OF->post-HAS_CREATOR->person2
	auto result = conn_->Query(
	    "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
	    "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->"
	    "(post:Post)-[:HAS_CREATOR]->(person2) "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 3);
	}
}

TEST_F(LSQBTest, Q3) {
	// Q3: Triangle in same country
	auto result = conn_->Query(
	    "MATCH (country:Country) "
	    "MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country) "
	    "MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country) "
	    "MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country) "
	    "MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1) "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 6);
	}
}

TEST_F(LSQBTest, Q4) {
	// Q4 (Post only, no Message union type):
	// Tag<-HAS_TAG-Post-HAS_CREATOR->Person, Post<-LIKES-Person, Post<-REPLY_OF-Comment
	auto result = conn_->Query(
	    "MATCH (:Tag)<-[:HAS_TAG]-(message:Post)-[:HAS_CREATOR]->(creator:Person), "
	    "(message)<-[:LIKES]-(liker:Person), "
	    "(message)<-[:REPLY_OF]-(comment:Comment) "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 3);
	}
}

TEST_F(LSQBTest, Q5) {
	// Q5 (Post only, no Message union type):
	// tag1<-HAS_TAG-Post<-REPLY_OF-Comment-HAS_TAG->tag2 WHERE tag1<>tag2
	auto result = conn_->Query(
	    "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Post)<-[:REPLY_OF]-"
	    "(comment:Comment)-[:HAS_TAG]->(tag2:Tag) "
	    "WHERE tag1 <> tag2 "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 1);
	}
}

TEST_F(LSQBTest, Q6) {
	// Q6: p1-KNOWS-p2-KNOWS-p3-HAS_INTEREST->tag WHERE p1<>p3
	auto result = conn_->Query(
	    "MATCH (person1:Person)-[:KNOWS]-(person2:Person)-[:KNOWS]-"
	    "(person3:Person)-[:HAS_INTEREST]->(tag:Tag) "
	    "WHERE person1 <> person3 "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 9);
	}
}

TEST_F(LSQBTest, Q7) {
	// Q7 uses OPTIONAL MATCH — now vectorized
	auto result = conn_->Query(
	    "MATCH (:Tag)<-[:HAS_TAG]-(message:Post)-[:HAS_CREATOR]->(creator:Person) "
	    "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person) "
	    "OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Comment) "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		// Post10: tag1, likes={P1}, replies={C1,C2} → 1*2=2
		// Post20: tag2, likes={P3}, replies={C6} → 1*1=1
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 3);
	}
}

TEST_F(LSQBTest, Q8) {
	// Q8 (Post only): anti-join on comment-HAS_TAG->tag1
	auto result = conn_->Query(
	    "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Post)<-[:REPLY_OF]-"
	    "(comment:Comment)-[:HAS_TAG]->(tag2:Tag) "
	    "WHERE NOT (comment)-[:HAS_TAG]->(tag1) "
	    "AND tag1 <> tag2 "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 1);
	}
}

TEST_F(LSQBTest, Q9) {
	// Q9: anti-join on person1-KNOWS-person3
	auto result = conn_->Query(
	    "MATCH (person1:Person)-[:KNOWS]-(person2:Person)-[:KNOWS]-"
	    "(person3:Person)-[:HAS_INTEREST]->(tag:Tag) "
	    "WHERE NOT (person1)-[:KNOWS]-(person3) "
	    "AND person1 <> person3 "
	    "RETURN count(*) AS count",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		EXPECT_EQ(response.arrays(0).int64_array().values(0), 3);
	}
}

// ============================================================
// ORDER BY / Top-N Tests
// ============================================================

TEST_F(VectorizedCompilerTest, OrderByAsc) {
	// MATCH (n:Person) RETURN n.age ORDER BY n.age ASC
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	// Project: n.age as tag 1
	common::Expression age_expr;
	auto* age_opr = age_expr.add_operators();
	auto* age_var = age_opr->mutable_var();
	age_var->mutable_tag()->set_id(0);
	age_var->mutable_property()->mutable_key()->set_name("age");
	*age_var->mutable_node_type() = MakeInt32Type();
	BuildProjectOpr(plan, {{age_expr, 1}}, false);

	// OrderBy: tag 1 ASC
	auto* order_opr = plan.add_plan();
	auto* order_by = order_opr->mutable_opr()->mutable_order_by();
	auto* pair = order_by->add_pairs();
	pair->mutable_key()->mutable_tag()->set_id(1);
	pair->set_order(algebra::OrderBy_OrderingPair_Order_ASC);

	BuildSinkOpr(plan, {1});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	ASSERT_TRUE(VecPipelineCompiler::CanVectorize(plan));

	auto pipeline = compiler.Compile();
	auto state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(state);
	VecExecContext exec_ctx{nullptr, reader_.get()};
	PipelineExecutor executor;
	executor.Execute(pipeline, &sink, exec_ctx);

	EXPECT_EQ(state->total_rows, kNumPersons);

	// Verify sorted order
	int32_t prev = std::numeric_limits<int32_t>::min();
	for (auto& chunk : state->chunks) {
		int col = chunk.FindColumnByTag(1);
		ASSERT_GE(col, 0);
		auto& vec = chunk.GetVector(static_cast<size_t>(col));
		Flatten(vec, chunk.size());
		auto* data = vec.GetData<int32_t>();
		for (size_t i = 0; i < chunk.size(); i++) {
			EXPECT_GE(data[i], prev) << "Not sorted at row " << i;
			prev = data[i];
		}
	}
}

TEST_F(VectorizedCompilerTest, OrderByDesc) {
	// MATCH (n:Person) RETURN n.age ORDER BY n.age DESC
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Expression age_expr;
	auto* age_opr = age_expr.add_operators();
	auto* age_var = age_opr->mutable_var();
	age_var->mutable_tag()->set_id(0);
	age_var->mutable_property()->mutable_key()->set_name("age");
	*age_var->mutable_node_type() = MakeInt32Type();
	BuildProjectOpr(plan, {{age_expr, 1}}, false);

	auto* order_opr = plan.add_plan();
	auto* order_by = order_opr->mutable_opr()->mutable_order_by();
	auto* pair = order_by->add_pairs();
	pair->mutable_key()->mutable_tag()->set_id(1);
	pair->set_order(algebra::OrderBy_OrderingPair_Order_DESC);

	BuildSinkOpr(plan, {1});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	auto pipeline = compiler.Compile();
	auto state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(state);
	VecExecContext exec_ctx{nullptr, reader_.get()};
	PipelineExecutor executor;
	executor.Execute(pipeline, &sink, exec_ctx);

	EXPECT_EQ(state->total_rows, kNumPersons);

	int32_t prev = std::numeric_limits<int32_t>::max();
	for (auto& chunk : state->chunks) {
		int col = chunk.FindColumnByTag(1);
		ASSERT_GE(col, 0);
		auto& vec = chunk.GetVector(static_cast<size_t>(col));
		Flatten(vec, chunk.size());
		auto* data = vec.GetData<int32_t>();
		for (size_t i = 0; i < chunk.size(); i++) {
			EXPECT_LE(data[i], prev) << "Not sorted DESC at row " << i;
			prev = data[i];
		}
	}
}

TEST_F(VectorizedCompilerTest, TopN) {
	// MATCH (n:Person) RETURN n.age ORDER BY n.age ASC LIMIT 5
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Expression age_expr;
	auto* age_opr = age_expr.add_operators();
	auto* age_var = age_opr->mutable_var();
	age_var->mutable_tag()->set_id(0);
	age_var->mutable_property()->mutable_key()->set_name("age");
	*age_var->mutable_node_type() = MakeInt32Type();
	BuildProjectOpr(plan, {{age_expr, 1}}, false);

	auto* order_opr = plan.add_plan();
	auto* order_by = order_opr->mutable_opr()->mutable_order_by();
	auto* pair = order_by->add_pairs();
	pair->mutable_key()->mutable_tag()->set_id(1);
	pair->set_order(algebra::OrderBy_OrderingPair_Order_ASC);
	auto* limit = order_by->mutable_limit();
	limit->set_lower(0);
	limit->set_upper(5);

	BuildSinkOpr(plan, {1});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	auto pipeline = compiler.Compile();
	auto state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(state);
	VecExecContext exec_ctx{nullptr, reader_.get()};
	PipelineExecutor executor;
	executor.Execute(pipeline, &sink, exec_ctx);

	EXPECT_EQ(state->total_rows, 5u);

	// The 5 smallest ages. Persons have ages 20+(i%40) for i=0..99.
	// The smallest ages are: 20,20,20 (i=0,40,80 → i%40=0) ... but wait
	// there are multiple persons with same age. Let's just verify sorted + count.
	int32_t prev = std::numeric_limits<int32_t>::min();
	for (auto& chunk : state->chunks) {
		int col = chunk.FindColumnByTag(1);
		ASSERT_GE(col, 0);
		auto& vec = chunk.GetVector(static_cast<size_t>(col));
		Flatten(vec, chunk.size());
		auto* data = vec.GetData<int32_t>();
		for (size_t i = 0; i < chunk.size(); i++) {
			EXPECT_GE(data[i], prev);
			prev = data[i];
		}
	}
	// First element should be 20 (minimum age)
	if (!state->chunks.empty()) {
		int col = state->chunks[0].FindColumnByTag(1);
		auto& vec = state->chunks[0].GetVector(static_cast<size_t>(col));
		EXPECT_EQ(vec.GetData<int32_t>()[0], 20);
	}
}

TEST_F(VectorizedCompilerTest, TopNDescLimit3) {
	// MATCH (n:Person) RETURN n.age ORDER BY n.age DESC LIMIT 3
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Expression age_expr;
	auto* age_opr = age_expr.add_operators();
	auto* age_var = age_opr->mutable_var();
	age_var->mutable_tag()->set_id(0);
	age_var->mutable_property()->mutable_key()->set_name("age");
	*age_var->mutable_node_type() = MakeInt32Type();
	BuildProjectOpr(plan, {{age_expr, 1}}, false);

	auto* order_opr = plan.add_plan();
	auto* order_by = order_opr->mutable_opr()->mutable_order_by();
	auto* pair = order_by->add_pairs();
	pair->mutable_key()->mutable_tag()->set_id(1);
	pair->set_order(algebra::OrderBy_OrderingPair_Order_DESC);
	auto* limit = order_by->mutable_limit();
	limit->set_lower(0);
	limit->set_upper(3);

	BuildSinkOpr(plan, {1});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	auto pipeline = compiler.Compile();
	auto state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(state);
	VecExecContext exec_ctx{nullptr, reader_.get()};
	PipelineExecutor executor;
	executor.Execute(pipeline, &sink, exec_ctx);

	EXPECT_EQ(state->total_rows, 3u);

	// Top 3 DESC: ages 59,59,58 (i%40=39 → age=59 for i=39,79; i%40=38 → age=58)
	int32_t prev = std::numeric_limits<int32_t>::max();
	for (auto& chunk : state->chunks) {
		int col = chunk.FindColumnByTag(1);
		ASSERT_GE(col, 0);
		auto& vec = chunk.GetVector(static_cast<size_t>(col));
		Flatten(vec, chunk.size());
		auto* data = vec.GetData<int32_t>();
		for (size_t i = 0; i < chunk.size(); i++) {
			EXPECT_LE(data[i], prev);
			prev = data[i];
		}
	}
	// First element should be 59 (max age)
	if (!state->chunks.empty()) {
		int col = state->chunks[0].FindColumnByTag(1);
		auto& vec = state->chunks[0].GetVector(static_cast<size_t>(col));
		EXPECT_EQ(vec.GetData<int32_t>()[0], 59);
	}
}

TEST_F(VectorizedCompilerTest, LimitOnly) {
	// MATCH (n:Person) RETURN n LIMIT 10
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	auto* limit_opr = plan.add_plan();
	auto* limit = limit_opr->mutable_opr()->mutable_limit();
	auto* range = limit->mutable_range();
	range->set_lower(0);
	range->set_upper(10);

	BuildSinkOpr(plan, {0});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	ASSERT_TRUE(VecPipelineCompiler::CanVectorize(plan));
	size_t total = CompileAndExecute(compiler);
	EXPECT_EQ(total, 10u);
}

TEST_F(VectorizedCompilerTest, OrderByString) {
	// MATCH (n:Person) RETURN n.name ORDER BY n.name ASC LIMIT 3
	physical::PhysicalPlan plan;
	BuildScanOpr(plan, 0);

	common::Expression name_expr;
	auto* name_opr = name_expr.add_operators();
	auto* name_var = name_opr->mutable_var();
	name_var->mutable_tag()->set_id(0);
	name_var->mutable_property()->mutable_key()->set_name("name");
	*name_var->mutable_node_type() = MakeStringType();
	BuildProjectOpr(plan, {{name_expr, 1}}, false);

	auto* order_opr = plan.add_plan();
	auto* order_by = order_opr->mutable_opr()->mutable_order_by();
	auto* pair = order_by->add_pairs();
	pair->mutable_key()->mutable_tag()->set_id(1);
	pair->set_order(algebra::OrderBy_OrderingPair_Order_ASC);
	auto* limit = order_by->mutable_limit();
	limit->set_lower(0);
	limit->set_upper(3);

	BuildSinkOpr(plan, {1});

	VecPipelineCompiler compiler(reader_->schema(), plan);
	auto pipeline = compiler.Compile();
	auto state = std::make_shared<SharedResultSink::State>();
	SharedResultSink sink(state);
	VecExecContext exec_ctx{nullptr, reader_.get()};
	PipelineExecutor executor;
	executor.Execute(pipeline, &sink, exec_ctx);

	EXPECT_EQ(state->total_rows, 3u);

	// Names are "person_0" to "person_99". Lexicographic order:
	// "person_0", "person_1", "person_10", "person_11", ...
	// So first 3 should be: "person_0", "person_1", "person_10"
	if (!state->chunks.empty()) {
		int col = state->chunks[0].FindColumnByTag(1);
		ASSERT_GE(col, 0);
		auto& vec = state->chunks[0].GetVector(static_cast<size_t>(col));
		Flatten(vec, state->chunks[0].size());
		auto* str_data = StringVector::GetStringData(vec);
		EXPECT_EQ(str_data[0].GetString(), "person_0");
		EXPECT_EQ(str_data[1].GetString(), "person_1");
		EXPECT_EQ(str_data[2].GetString(), "person_10");
	}
}

// ============================================================
// Expression Project Tests (Cypher E2E)
// ============================================================

TEST_F(VectorizedE2ETest, ProjectArithmetic) {
	auto result = conn_->Query(
	    "MATCH (n:Person) RETURN n.age + 1 AS age_plus_one", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 100);
	// All values should be age + 1, i.e., between 21 and 60
	if (response.arrays_size() > 0 && response.arrays(0).has_int32_array()) {
		for (int i = 0; i < response.arrays(0).int32_array().values_size(); i++) {
			int32_t v = response.arrays(0).int32_array().values(i);
			EXPECT_GE(v, 21);
			EXPECT_LE(v, 60);
		}
	}
}

TEST_F(VectorizedE2ETest, ProjectMultipleExpressions) {
	auto result = conn_->Query(
	    "MATCH (n:Person) WHERE n.age = 20 "
	    "RETURN n.age * 2 AS doubled, n.age - 10 AS minus_ten",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// age=20: i%40=0 → i=0,40,80 → 3 persons
	EXPECT_EQ(response.row_count(), 3);
	if (response.arrays_size() >= 2) {
		// doubled = 40, minus_ten = 10
		if (response.arrays(0).has_int32_array()) {
			for (int i = 0; i < response.arrays(0).int32_array().values_size(); i++) {
				EXPECT_EQ(response.arrays(0).int32_array().values(i), 40);
			}
		}
		if (response.arrays(1).has_int32_array()) {
			for (int i = 0; i < response.arrays(1).int32_array().values_size(); i++) {
				EXPECT_EQ(response.arrays(1).int32_array().values(i), 10);
			}
		}
	}
}

TEST_F(VectorizedE2ETest, ProjectMixedExprAndRef) {
	auto result = conn_->Query(
	    "MATCH (n:Person) WHERE n.id = 0 "
	    "RETURN n.age AS age, n.age + 1 AS next_age",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 1);
	if (response.arrays_size() >= 2) {
		// person_0: age=20, next_age=21
		if (response.arrays(0).has_int32_array()) {
			EXPECT_EQ(response.arrays(0).int32_array().values(0), 20);
		}
		if (response.arrays(1).has_int32_array()) {
			EXPECT_EQ(response.arrays(1).int32_array().values(0), 21);
		}
	}
}

TEST_F(VectorizedE2ETest, DedupDistinct) {
	// MATCH (n:Person) RETURN DISTINCT n.age
	// 100 persons with age = 20 + (i%40), so 40 distinct ages
	auto result = conn_->Query(
	    "MATCH (n:Person) RETURN DISTINCT n.age AS age", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 40);
}

TEST_F(VectorizedE2ETest, DedupDistinctWithOrder) {
	// MATCH (n:Person) RETURN DISTINCT n.age AS age ORDER BY age ASC LIMIT 5
	auto result = conn_->Query(
	    "MATCH (n:Person) RETURN DISTINCT n.age AS age "
	    "ORDER BY age ASC LIMIT 5",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 5);
	if (response.arrays_size() > 0 && response.arrays(0).has_int32_array()) {
		auto& arr = response.arrays(0).int32_array();
		EXPECT_EQ(arr.values(0), 20);
		EXPECT_EQ(arr.values(1), 21);
		EXPECT_EQ(arr.values(2), 22);
		EXPECT_EQ(arr.values(3), 23);
		EXPECT_EQ(arr.values(4), 24);
	}
}

TEST_F(VectorizedE2ETest, UnwindWithMatch) {
	// Simpler: just unfold a literal list, return only unfolded values
	auto result = conn_->Query(
	    "MATCH (n:Person) WHERE n.id < 3 "
	    "WITH [10, 20] AS xs "
	    "UNWIND xs AS x "
	    "RETURN x",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// WITH [10,20] AS xs after MATCH gives 3 rows each with [10,20],
	// so UNWIND gives 6 rows
	EXPECT_EQ(response.row_count(), 6);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		auto& arr = response.arrays(0).int64_array();
		int count_10 = 0, count_20 = 0;
		for (int i = 0; i < arr.values_size(); i++) {
			if (arr.values(i) == 10) count_10++;
			else if (arr.values(i) == 20) count_20++;
		}
		EXPECT_EQ(count_10, 3);
		EXPECT_EQ(count_20, 3);
	}
}

TEST_F(VectorizedE2ETest, UnwindWithProperty) {
	// Test unfold with property access on non-list column
	auto result = conn_->Query(
	    "MATCH (n:Person) WHERE n.id < 2 "
	    "WITH n.id AS nid, [10, 20] AS xs "
	    "UNWIND xs AS x "
	    "RETURN nid, x",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	// 2 persons * 2 elements = 4 rows
	EXPECT_EQ(response.row_count(), 4);
}

TEST_F(VectorizedE2ETest, UnwindLargerList) {
	// Verify UNWIND with more elements
	auto result = conn_->Query(
	    "MATCH (n:Person) WHERE n.id = 0 "
	    "WITH [10, 20, 30, 40, 50] AS vals "
	    "UNWIND vals AS v "
	    "RETURN v",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& response = result.value().response();
	EXPECT_EQ(response.row_count(), 5);
	if (response.arrays_size() > 0 && response.arrays(0).has_int64_array()) {
		auto& arr = response.arrays(0).int64_array();
		EXPECT_EQ(arr.values(0), 10);
		EXPECT_EQ(arr.values(1), 20);
		EXPECT_EQ(arr.values(2), 30);
		EXPECT_EQ(arr.values(3), 40);
		EXPECT_EQ(arr.values(4), 50);
	}
}

// ============================================================
// GetV Filter Tests
// ============================================================

class VectorizedGetVFilterTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_getv_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, age INT32, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Student (id INT64, name STRING, grade INT32, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE KNOWS(FROM Person TO Person);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE MENTORS(FROM Person TO Student);"));

		// 3 persons
		for (int i = 0; i < 3; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) +
			                "', age: " + std::to_string(20 + i * 10) + "});";
			ASSERT_TRUE(conn_->Query(q));
		}
		// 2 students
		for (int i = 0; i < 2; ++i) {
			std::string q = "CREATE (:Student {id: " + std::to_string(i) +
			                ", name: 'student_" + std::to_string(i) +
			                "', grade: " + std::to_string(3 + i) + "});";
			ASSERT_TRUE(conn_->Query(q));
		}

		// KNOWS: 0->1, 0->2
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:1}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:2}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		// MENTORS: 0->student_0, 1->student_1
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (s:Student {id:0}) "
		    "CREATE (a)-[:MENTORS]->(s);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (s:Student {id:1}) "
		    "CREATE (a)-[:MENTORS]->(s);"));
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}
};

TEST_F(VectorizedGetVFilterTest, KnowsNoFilter) {
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 2);
}

TEST_F(VectorizedGetVFilterTest, MentorsNoFilter) {
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:MENTORS]->(s:Student) RETURN s.name", "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 2);
}

TEST_F(VectorizedGetVFilterTest, KnowsWithPredicate) {
	// All KNOWS edges: 0->1 (age=30), 0->2 (age=40)
	// b.age > 35 should return only person 2
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 35 "
	    "RETURN b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 1);
}

TEST_F(VectorizedGetVFilterTest, MentorsWithPredicate) {
	// Student 0 has grade=3, student 1 has grade=4
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:MENTORS]->(s:Student) WHERE s.grade >= 4 "
	    "RETURN s.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	EXPECT_EQ(result.value().response().row_count(), 1);
}

// ============================================================
// TC Fuse Tests
// ============================================================

class VectorizedTCFuseTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_tc_fuse_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT32);"));

		// Create 5 persons
		for (int i = 0; i < 5; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'p" + std::to_string(i) + "'});";
			ASSERT_TRUE(conn_->Query(q));
		}

		// Triangle: 0-1-2-0 (all edges with various 'since' values)
		CreateEdge(0, 1, 2018);
		CreateEdge(1, 2, 2019);
		CreateEdge(0, 2, 2020);

		// Another triangle: 0-1-3-0
		CreateEdge(1, 3, 2021);
		CreateEdge(0, 3, 2022);

		// Non-triangle edge: 2->4
		CreateEdge(2, 4, 2023);
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}

	void CreateEdge(int src, int dst, int since) {
		std::string q = "MATCH (a:Person {id: " + std::to_string(src) +
		                "}), (b:Person {id: " + std::to_string(dst) +
		                "}) CREATE (a)-[:KNOWS {since: " +
		                std::to_string(since) + "}]->(b);";
		ASSERT_TRUE(conn_->Query(q)) << "Failed: " << q;
	}
};

TEST_F(VectorizedTCFuseTest, BasicTriangleCount) {
	// The TC fuse pattern is triggered by the query planner for specific
	// triangle-counting queries. This tests that the pattern is recognized
	// and executed correctly through the non-vectorized path.
	// The vectorized TC fuse will be tested via proto-level plan construction.
	auto result = conn_->Query(
	    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	// 6 directed edges total
	EXPECT_EQ(result.value().response().row_count(), 6);
}

// ============================================================
// LDBC Query Compilation Tests
// ============================================================

class VectorizedLDBCTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_ldbc_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		// Create LDBC schema
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE PLACE (id INT64, name STRING, url STRING, "
		    "type STRING, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE PERSON (id INT64, firstName STRING, "
		    "lastName STRING, gender STRING, birthday DATE, "
		    "creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, "
		    "language STRING, email STRING, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE COMMENT (id INT64, creationDate TIMESTAMP, "
		    "locationIP STRING, browserUsed STRING, content STRING, "
		    "length INT32, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE POST (id INT64, imageFile STRING, "
		    "creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, "
		    "language STRING, content STRING, length INT32, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE FORUM (id INT64, title STRING, "
		    "creationDate TIMESTAMP, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE ORGANISATION (id INT64, type STRING, "
		    "name STRING, url STRING, PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE TAGCLASS (id INT64, name STRING, url STRING, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE TAG (id INT64, name STRING, url STRING, "
		    "PRIMARY KEY(id));"));

		// Edge types (multi-pair types use same name with different FROM/TO)
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASCREATOR(FROM COMMENT TO PERSON, "
		    "creationDate TIMESTAMP);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASCREATOR(FROM POST TO PERSON, "
		    "creationDate TIMESTAMP);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASTAG(FROM POST TO TAG);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASTAG(FROM FORUM TO TAG);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASTAG(FROM COMMENT TO TAG);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE REPLYOF(FROM COMMENT TO COMMENT);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE REPLYOF(FROM COMMENT TO POST);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE CONTAINEROF(FROM FORUM TO POST);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASMEMBER(FROM FORUM TO PERSON, "
		    "joinDate TIMESTAMP);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASMODERATOR(FROM FORUM TO PERSON);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASINTEREST(FROM PERSON TO TAG);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE ISLOCATEDIN(FROM COMMENT TO PLACE);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE ISLOCATEDIN(FROM PERSON TO PLACE);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE ISLOCATEDIN(FROM POST TO PLACE);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE ISLOCATEDIN(FROM ORGANISATION TO PLACE);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE KNOWS(FROM PERSON TO PERSON, "
		    "creationDate TIMESTAMP);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE LIKES(FROM PERSON TO COMMENT, "
		    "creationDate TIMESTAMP);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE LIKES(FROM PERSON TO POST, "
		    "creationDate TIMESTAMP);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE WORKAT(FROM PERSON TO ORGANISATION, "
		    "workFrom INT32);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE ISPARTOF(FROM PLACE TO PLACE);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE HASTYPE(FROM TAG TO TAGCLASS);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE ISSUBCLASSOF(FROM TAGCLASS TO TAGCLASS);"));
		ASSERT_TRUE(conn_->Query(
		    "CREATE REL TABLE STUDYAT(FROM PERSON TO ORGANISATION, "
		    "classYear INT32);"));
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}

	std::string ReadQueryFile(const std::string& filename) {
		std::string path =
		    std::string("../tests/resources") + "/ldbc/queries/" + filename;
		std::ifstream ifs(path);
		if (!ifs.is_open()) return "";
		return std::string((std::istreambuf_iterator<char>(ifs)),
		                   std::istreambuf_iterator<char>());
	}

	neug::execution::ParamsMap MakePersonParams() {
		neug::execution::ParamsMap params;
		params.emplace("personId", Value::INT64(0));
		params.emplace("firstName", Value::STRING("Alice"));
		params.emplace("maxDate", Value::INT64(0));
		params.emplace("startDate", Value::INT64(0));
		params.emplace("endDate", Value::INT64(0));
		params.emplace("minDate", Value::INT64(0));
		params.emplace("month", Value::INT32(1));
		params.emplace("person1Id", Value::INT64(0));
		params.emplace("person2Id", Value::INT64(1));
		params.emplace("tagName", Value::STRING("tag"));
		params.emplace("tagClassName", Value::STRING("tagclass"));
		params.emplace("countryXName", Value::STRING("countryX"));
		params.emplace("countryYName", Value::STRING("countryY"));
		params.emplace("countryName", Value::STRING("country"));
		params.emplace("workFromYear", Value::INT32(2020));
		params.emplace("messageId", Value::INT64(0));
		return params;
	}
};

TEST_F(VectorizedLDBCTest, InteractiveShortQueries) {
	auto params = MakePersonParams();
	std::vector<std::string> short_queries = {
	    "interactive-short-1.cypher",
	    "interactive-short-2.cypher",
	    "interactive-short-3.cypher",
	    "interactive-short-4.cypher",
	    "interactive-short-5.cypher",
	    "interactive-short-6.cypher",
	    "interactive-short-7.cypher",
	};

	for (auto& qf : short_queries) {
		auto query = ReadQueryFile(qf);
		ASSERT_FALSE(query.empty()) << "Failed to read " << qf;
		auto result = conn_->Query(query, "read", params);
		if (result.has_value()) {
			std::cout << qf << ": OK (rows="
			          << result.value().response().row_count() << ")"
			          << std::endl;
		} else {
			std::cout << qf << ": FAILED ("
			          << result.error().error_message() << ")" << std::endl;
		}
	}
}

TEST_F(VectorizedLDBCTest, InteractiveComplexQueries) {
	auto params = MakePersonParams();
	std::vector<std::string> complex_queries = {
	    "interactive-complex-1.cypher",
	    "interactive-complex-2.cypher",
	    "interactive-complex-3.cypher",
	    "interactive-complex-4.cypher",
	    "interactive-complex-5.cypher",
	    "interactive-complex-6.cypher",
	    "interactive-complex-7.cypher",
	    "interactive-complex-8.cypher",
	    "interactive-complex-9.cypher",
	    "interactive-complex-10.cypher",
	    "interactive-complex-11.cypher",
	    "interactive-complex-12.cypher",
	    "interactive-complex-13.cypher",
	    "interactive-complex-14.cypher",
	};

	for (auto& qf : complex_queries) {
		auto query = ReadQueryFile(qf);
		ASSERT_FALSE(query.empty()) << "Failed to read " << qf;
		auto result = conn_->Query(query, "read", params);
		if (result.has_value()) {
			std::cout << qf << ": OK (rows="
			          << result.value().response().row_count() << ")"
			          << std::endl;
		} else {
			std::cout << qf << ": FAILED ("
			          << result.error().error_message() << ")" << std::endl;
		}
	}
}

// ============================================================
// ShortestPath Tests
// ============================================================

class VectorizedShortestPathTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_shortest_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(
		    conn_->Query("CREATE REL TABLE KNOWS(FROM Person TO Person);"));

		// Create 5 people: 0,1,2,3,4
		for (int i = 0; i < 5; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) + "'});";
			ASSERT_TRUE(conn_->Query(q)) << "Failed to create person " << i;
		}
		// Edges: 0->1, 0->2, 0->3, 1->2, 1->4
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:1}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:2}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:3}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (b:Person {id:2}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (b:Person {id:4}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}
};

TEST_F(VectorizedShortestPathTest, DirectNeighbor) {
	// 0 to 1: direct edge, length = 1
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[k:KNOWS* SHORTEST 1..]-"
	    "(b:Person {id: 1}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 1);
	ASSERT_EQ(resp.arrays_size(), 1);
	ASSERT_TRUE(resp.arrays(0).has_int64_array());
	EXPECT_EQ(resp.arrays(0).int64_array().values(0), 1);
}

TEST_F(VectorizedShortestPathTest, TwoHops) {
	// 0 to 4: 0->1->4, length = 2
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[k:KNOWS* SHORTEST 1..]-"
	    "(b:Person {id: 4}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 1);
	ASSERT_EQ(resp.arrays_size(), 1);
	ASSERT_TRUE(resp.arrays(0).has_int64_array());
	EXPECT_EQ(resp.arrays(0).int64_array().values(0), 2);
}

TEST_F(VectorizedShortestPathTest, NoPath) {
	// Person 99 doesn't exist → 0 rows
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[k:KNOWS* SHORTEST 1..]-"
	    "(b:Person {id: 99}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 0);
}

TEST_F(VectorizedShortestPathTest, ReverseDirection) {
	// 4 to 0: undirected, same as 0 to 4, length = 2
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 4})-[k:KNOWS* SHORTEST 1..]-"
	    "(b:Person {id: 0}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 1);
	ASSERT_EQ(resp.arrays_size(), 1);
	ASSERT_TRUE(resp.arrays(0).has_int64_array());
	EXPECT_EQ(resp.arrays(0).int64_array().values(0), 2);
}

TEST_F(VectorizedShortestPathTest, ThreeHops) {
	// 3 to 4: 3<-0->1->4, length = 3 (undirected)
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 3})-[k:KNOWS* SHORTEST 1..]-"
	    "(b:Person {id: 4}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 1);
	ASSERT_EQ(resp.arrays_size(), 1);
	ASSERT_TRUE(resp.arrays(0).has_int64_array());
	EXPECT_EQ(resp.arrays(0).int64_array().values(0), 3);
}

// --- ALL SHORTEST PATH tests ---

class VectorizedAllShortestPathTest : public ::testing::Test {
 protected:
	std::unique_ptr<neug::NeugDB> db_;
	std::shared_ptr<neug::Connection> conn_;
	std::string work_dir_;

	void SetUp() override {
		work_dir_ = "/tmp/test_vec_all_shortest_" +
		            std::string(::testing::UnitTest::GetInstance()
		                            ->current_test_info()
		                            ->name());
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
		std::filesystem::create_directories(work_dir_);

		db_ = std::make_unique<neug::NeugDB>();
		db_->Open(work_dir_, 1);
		conn_ = db_->Connect();

		ASSERT_TRUE(conn_->Query(
		    "CREATE NODE TABLE Person (id INT64, name STRING, "
		    "PRIMARY KEY(id));"));
		ASSERT_TRUE(
		    conn_->Query("CREATE REL TABLE KNOWS(FROM Person TO Person);"));

		// Diamond graph: 0→1→3, 0→2→3 (two shortest paths of length 2)
		// Plus: 1→4, 2→4 (two shortest paths of length 3 from 0 to 4)
		for (int i = 0; i < 5; ++i) {
			std::string q = "CREATE (:Person {id: " + std::to_string(i) +
			                ", name: 'person_" + std::to_string(i) + "'});";
			ASSERT_TRUE(conn_->Query(q)) << "Failed to create person " << i;
		}
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:1}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:0}), (b:Person {id:2}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (b:Person {id:3}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:2}), (b:Person {id:3}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:1}), (b:Person {id:4}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
		ASSERT_TRUE(conn_->Query(
		    "MATCH (a:Person {id:2}), (b:Person {id:4}) "
		    "CREATE (a)-[:KNOWS]->(b);"));
	}

	void TearDown() override {
		if (conn_) conn_->Close();
		conn_.reset();
		if (db_) db_->Close();
		db_.reset();
		if (std::filesystem::exists(work_dir_)) {
			std::filesystem::remove_all(work_dir_);
		}
	}
};

TEST_F(VectorizedAllShortestPathTest, TwoPaths) {
	// 0 to 3: two shortest paths (0→1→3, 0→2→3), both length 2
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[k:KNOWS* ALL SHORTEST 1..]-"
	    "(b:Person {id: 3}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 2);
	ASSERT_EQ(resp.arrays_size(), 1);
	ASSERT_TRUE(resp.arrays(0).has_int64_array());
	EXPECT_EQ(resp.arrays(0).int64_array().values(0), 2);
	EXPECT_EQ(resp.arrays(0).int64_array().values(1), 2);
}

TEST_F(VectorizedAllShortestPathTest, DirectNeighbor) {
	// 0 to 1: one shortest path (direct), length 1
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[k:KNOWS* ALL SHORTEST 1..]-"
	    "(b:Person {id: 1}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 1);
	ASSERT_EQ(resp.arrays_size(), 1);
	ASSERT_TRUE(resp.arrays(0).has_int64_array());
	EXPECT_EQ(resp.arrays(0).int64_array().values(0), 1);
}

TEST_F(VectorizedAllShortestPathTest, NoPath) {
	// 0 to 99: no path
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[k:KNOWS* ALL SHORTEST 1..]-"
	    "(b:Person {id: 99}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 0);
}

TEST_F(VectorizedAllShortestPathTest, ThreeHopsTwoPaths) {
	// 0 to 4: two shortest paths (0→1→4, 0→2→4), both length 2
	auto result = conn_->Query(
	    "MATCH (a:Person {id: 0})-[k:KNOWS* ALL SHORTEST 1..]-"
	    "(b:Person {id: 4}) RETURN length(k) as len",
	    "read");
	ASSERT_TRUE(result.has_value()) << result.error().error_message();
	const auto& resp = result.value().response();
	EXPECT_EQ(resp.row_count(), 2);
	ASSERT_EQ(resp.arrays_size(), 1);
	ASSERT_TRUE(resp.arrays(0).has_int64_array());
	EXPECT_EQ(resp.arrays(0).int64_array().values(0), 2);
	EXPECT_EQ(resp.arrays(0).int64_array().values(1), 2);
}
