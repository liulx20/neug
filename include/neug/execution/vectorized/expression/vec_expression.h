#pragma once
#include <memory>
#include <string>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/core/graph_vector.h"

namespace neug {
class StorageReadInterface;
}

namespace neug::execution::vec {

enum class CompareOp : uint8_t { kEq, kNe, kLt, kLe, kGt, kGe };
enum class ArithOp : uint8_t { kAdd, kSub, kMul, kDiv, kMod };
enum class StringFuncOp : uint8_t { kStartsWith, kEndsWith, kContains };

struct VecExecContext {
	const execution::ParamsMap* params = nullptr;
	const StorageReadInterface* graph = nullptr;
};

// ============================================================
// Base
// ============================================================

class VecExpression {
 public:
	virtual ~VecExpression() = default;
	virtual DataType result_type() const = 0;
	virtual void Evaluate(const GraphDataChunk& chunk, size_t count,
	                      GraphVector& result,
	                      const VecExecContext& ctx) = 0;
};

// ============================================================
// Leaf expressions
// ============================================================

class VecColumnRefExpr : public VecExpression {
 public:
	VecColumnRefExpr(int column_tag, DataType type);
	DataType result_type() const override { return type_; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	int column_tag_;
	DataType type_;
};

class VecConstantExpr : public VecExpression {
 public:
	explicit VecConstantExpr(execution::Value constant);
	DataType result_type() const override { return constant_.type(); }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;
	const execution::Value& constant() const { return constant_; }

 private:
	execution::Value constant_;
};

class VecParamExpr : public VecExpression {
 public:
	VecParamExpr(std::string name, int index, DataType type);
	DataType result_type() const override { return type_; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::string name_;
	int index_;
	DataType type_;
};

// ============================================================
// Comparison
// ============================================================

class VecComparisonExpr : public VecExpression {
 public:
	VecComparisonExpr(std::unique_ptr<VecExpression> lhs,
	                  CompareOp op,
	                  std::unique_ptr<VecExpression> rhs);
	DataType result_type() const override {
		return DataType{DataTypeId::kBoolean};
	}
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> lhs_;
	std::unique_ptr<VecExpression> rhs_;
	CompareOp op_;
};

// ============================================================
// Boolean logic
// ============================================================

class VecBooleanAndExpr : public VecExpression {
 public:
	VecBooleanAndExpr(std::unique_ptr<VecExpression> lhs,
	                  std::unique_ptr<VecExpression> rhs);
	DataType result_type() const override {
		return DataType{DataTypeId::kBoolean};
	}
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> lhs_;
	std::unique_ptr<VecExpression> rhs_;
};

class VecBooleanOrExpr : public VecExpression {
 public:
	VecBooleanOrExpr(std::unique_ptr<VecExpression> lhs,
	                 std::unique_ptr<VecExpression> rhs);
	DataType result_type() const override {
		return DataType{DataTypeId::kBoolean};
	}
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> lhs_;
	std::unique_ptr<VecExpression> rhs_;
};

class VecBooleanNotExpr : public VecExpression {
 public:
	explicit VecBooleanNotExpr(std::unique_ptr<VecExpression> operand);
	DataType result_type() const override {
		return DataType{DataTypeId::kBoolean};
	}
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> operand_;
};

// ============================================================
// Arithmetic
// ============================================================

class VecArithExpr : public VecExpression {
 public:
	VecArithExpr(std::unique_ptr<VecExpression> lhs, ArithOp op,
	             std::unique_ptr<VecExpression> rhs);
	DataType result_type() const override { return result_type_; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> lhs_;
	std::unique_ptr<VecExpression> rhs_;
	ArithOp op_;
	DataType result_type_;
};

class VecUnaryMinusExpr : public VecExpression {
 public:
	explicit VecUnaryMinusExpr(std::unique_ptr<VecExpression> operand);
	DataType result_type() const override { return operand_->result_type(); }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> operand_;
};

// ============================================================
// Type cast
// ============================================================

class VecCastExpr : public VecExpression {
 public:
	VecCastExpr(std::unique_ptr<VecExpression> child, DataType target_type);
	DataType result_type() const override { return target_type_; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> child_;
	DataType target_type_;
};

// ============================================================
// Null checks
// ============================================================

class VecIsNullExpr : public VecExpression {
 public:
	VecIsNullExpr(std::unique_ptr<VecExpression> operand, bool negate);
	DataType result_type() const override {
		return DataType{DataTypeId::kBoolean};
	}
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> operand_;
	bool negate_;
};

// ============================================================
// String predicates
// ============================================================

class VecStringFuncExpr : public VecExpression {
 public:
	VecStringFuncExpr(std::unique_ptr<VecExpression> str_expr,
	                  std::unique_ptr<VecExpression> pattern_expr,
	                  StringFuncOp op);
	DataType result_type() const override {
		return DataType{DataTypeId::kBoolean};
	}
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> str_expr_;
	std::unique_ptr<VecExpression> pattern_expr_;
	StringFuncOp op_;
};

// ============================================================
// IN list
// ============================================================

class VecInListExpr : public VecExpression {
 public:
	VecInListExpr(std::unique_ptr<VecExpression> value_expr,
	              std::vector<execution::Value> list);
	DataType result_type() const override {
		return DataType{DataTypeId::kBoolean};
	}
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> value_expr_;
	std::vector<execution::Value> list_;
};

// ============================================================
// CASE WHEN
// ============================================================

class VecCaseWhenExpr : public VecExpression {
 public:
	using WhenThen = std::pair<std::unique_ptr<VecExpression>,
	                           std::unique_ptr<VecExpression>>;

	VecCaseWhenExpr(std::vector<WhenThen> when_thens,
	                std::unique_ptr<VecExpression> else_expr,
	                DataType result_type);
	DataType result_type() const override { return result_type_; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::vector<WhenThen> when_thens_;
	std::unique_ptr<VecExpression> else_expr_;
	DataType result_type_;
};

// ============================================================

class VecPathLengthExpr : public VecExpression {
 public:
	explicit VecPathLengthExpr(int tag) : tag_(tag) {}
	DataType result_type() const override { return DataType{DataTypeId::kInt64}; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	int tag_;
};

enum class ExtractInterval : uint8_t {
	kYear, kMonth, kDay, kHour, kMinute, kSecond, kMillisecond
};

class VecExtractExpr : public VecExpression {
 public:
	VecExtractExpr(std::unique_ptr<VecExpression> operand,
	               ExtractInterval interval);
	DataType result_type() const override { return DataType{DataTypeId::kInt64}; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> operand_;
	ExtractInterval interval_;
};

class VecListExtractExpr : public VecExpression {
 public:
	VecListExtractExpr(std::unique_ptr<VecExpression> list_expr,
	                   std::unique_ptr<VecExpression> index_expr,
	                   DataType elem_type);
	DataType result_type() const override { return elem_type_; }
	void Evaluate(const GraphDataChunk& chunk, size_t count,
	              GraphVector& result, const VecExecContext& ctx) override;

 private:
	std::unique_ptr<VecExpression> list_expr_;
	std::unique_ptr<VecExpression> index_expr_;
	DataType elem_type_;
};

}  // namespace neug::execution::vec
