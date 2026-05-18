#include "neug/execution/vectorized/expression/vec_expression.h"

#include <cassert>
#include <cstring>
#include <ctime>
#include <string>

#include "neug/execution/vectorized/core/list_vector_buffer.h"
#include "neug/utils/property/types.h"

namespace neug::execution::vec {

// ============================================================
// Null propagation helpers
// ============================================================

static void CopyValidity(const GraphVector& src, GraphVector& dst,
                         size_t count) {
	if (!src.validity().AllValid()) {
		for (size_t i = 0; i < count; i++) {
			if (!src.validity().IsValid(i)) {
				dst.validity().SetInvalid(i);
			}
		}
	}
}

static void PropagateNullsBinary(const GraphVector& lhs, const GraphVector& rhs,
                                 GraphVector& result, size_t count) {
	bool lhs_has_nulls = !lhs.validity().AllValid();
	bool rhs_has_nulls = !rhs.validity().AllValid();
	if (!lhs_has_nulls && !rhs_has_nulls) {
		return;
	}
	for (size_t i = 0; i < count; i++) {
		bool l_valid = lhs.validity().IsValid(i);
		bool r_valid = rhs.validity().IsValid(i);
		if (!l_valid || !r_valid) {
			result.validity().SetInvalid(i);
		}
	}
}

// ============================================================
// VecColumnRefExpr
// ============================================================

VecColumnRefExpr::VecColumnRefExpr(int column_tag, DataType type)
    : column_tag_(column_tag), type_(std::move(type)) {}

static void CopyVectorData(const GraphVector& src, GraphVector& dst,
                           DataTypeId type_id, size_t count) {
	size_t elem_size = GetTypeSize(type_id);
	std::memcpy(dst.GetData<char>(), src.GetData<char>(), elem_size * count);
}

void VecColumnRefExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                GraphVector& result,
                                const VecExecContext& ctx) {
	const auto& src = chunk.GetVectorByTag(column_tag_);
	DataTypeId tid = type_.id();

	if (tid == DataTypeId::kVarchar) {
		auto* src_data = StringVector::GetStringData(
		    const_cast<GraphVector&>(src));
		auto* dst_data = StringVector::GetStringData(result);
		for (size_t i = 0; i < count; i++) {
			dst_data[i] = StringVector::AddString(
			    result, src_data[i].GetString());
		}
	} else if (tid == DataTypeId::kVertex) {
		const vid_t* src_vids = VertexVector::GetVids(src);
		vid_t* dst_vids = VertexVector::GetVids(result);
		std::memcpy(dst_vids, src_vids, count * sizeof(vid_t));
		label_t* dst_labels = VertexVector::GetLabels(result);
		if (VertexVector::IsConstantLabel(src)) {
			label_t lbl = VertexVector::GetConstantLabel(src);
			for (size_t i = 0; i < count; i++) dst_labels[i] = lbl;
		} else {
			const label_t* src_labels = VertexVector::GetLabels(src);
			std::memcpy(dst_labels, src_labels, count * sizeof(label_t));
		}
	} else {
		CopyVectorData(src, result, tid, count);
	}
	CopyValidity(src, result, count);
}

// ============================================================
// VecConstantExpr
// ============================================================

VecConstantExpr::VecConstantExpr(execution::Value constant)
    : constant_(std::move(constant)) {}

template <typename T>
static void FillConstant(GraphVector& result, T value, size_t count) {
	auto* data = result.GetData<T>();
	for (size_t i = 0; i < count; i++) {
		data[i] = value;
	}
}

void VecConstantExpr::Evaluate(const GraphDataChunk&, size_t count,
                               GraphVector& result,
                               const VecExecContext&) {
	if (constant_.IsNull()) {
		result.validity().SetAllInvalid();
		return;
	}
	DataTypeId tid = constant_.type().id();
	switch (tid) {
	case DataTypeId::kBoolean:
		FillConstant(result, constant_.GetValue<bool>(), count);
		break;
	case DataTypeId::kInt32:
		FillConstant(result, constant_.GetValue<int32_t>(), count);
		break;
	case DataTypeId::kInt64:
		FillConstant(result, constant_.GetValue<int64_t>(), count);
		break;
	case DataTypeId::kUInt32:
		FillConstant(result, constant_.GetValue<uint32_t>(), count);
		break;
	case DataTypeId::kUInt64:
		FillConstant(result, constant_.GetValue<uint64_t>(), count);
		break;
	case DataTypeId::kFloat:
		FillConstant(result, constant_.GetValue<float>(), count);
		break;
	case DataTypeId::kDouble:
		FillConstant(result, constant_.GetValue<double>(), count);
		break;
	case DataTypeId::kVarchar: {
		std::string str_val = constant_.GetValue<std::string>();
		auto* str_data = StringVector::GetStringData(result);
		for (size_t i = 0; i < count; i++) {
			str_data[i] = StringVector::AddString(result, str_val);
		}
		break;
	}
	case DataTypeId::kList: {
		const auto& children = ListValue::GetChildren(constant_);
		size_t num_elems = children.size();
		auto& child_vec = ListVector::GetChild(result);
		auto* entries = ListVector::GetEntries(result);

		DataTypeId child_tid = DataTypeId::kUnknown;
		if (!children.empty()) {
			child_tid = children[0].type().id();
		}

		// Fill child vector with the list elements once
		for (size_t j = 0; j < num_elems; j++) {
			switch (child_tid) {
			case DataTypeId::kInt32:
				child_vec.GetData<int32_t>()[j] = children[j].GetValue<int32_t>();
				break;
			case DataTypeId::kInt64:
				child_vec.GetData<int64_t>()[j] = children[j].GetValue<int64_t>();
				break;
			case DataTypeId::kDouble:
				child_vec.GetData<double>()[j] = children[j].GetValue<double>();
				break;
			case DataTypeId::kVarchar: {
				auto* sd = StringVector::GetStringData(child_vec);
				sd[j] = StringVector::AddString(
				    child_vec, children[j].GetValue<std::string>());
				break;
			}
			default:
				break;
			}
		}

		// Every row gets the same list entry pointing to [0, num_elems)
		for (size_t i = 0; i < count; i++) {
			entries[i] = list_entry_t{0, num_elems};
		}
		break;
	}
	default:
		assert(false && "Unsupported type in VecConstantExpr");
		break;
	}
}

// ============================================================
// VecComparisonExpr (with null propagation)
// ============================================================

VecComparisonExpr::VecComparisonExpr(std::unique_ptr<VecExpression> lhs,
                                     CompareOp op,
                                     std::unique_ptr<VecExpression> rhs)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), op_(op) {}

template <typename T>
static bool Compare(CompareOp op, T lhs, T rhs) {
	switch (op) {
	case CompareOp::kEq:
		return lhs == rhs;
	case CompareOp::kNe:
		return lhs != rhs;
	case CompareOp::kLt:
		return lhs < rhs;
	case CompareOp::kLe:
		return lhs <= rhs;
	case CompareOp::kGt:
		return lhs > rhs;
	case CompareOp::kGe:
		return lhs >= rhs;
	}
	return false;
}

template <typename T>
static void CompareTyped(const GraphVector& lhs_vec,
                         const GraphVector& rhs_vec, CompareOp op,
                         size_t count, GraphVector& result) {
	const auto* l = lhs_vec.GetData<T>();
	const auto* r = rhs_vec.GetData<T>();
	auto* out = result.GetData<bool>();
	for (size_t i = 0; i < count; i++) {
		out[i] = Compare(op, l[i], r[i]);
	}
}

void VecComparisonExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                 GraphVector& result,
                                 const VecExecContext& ctx) {
	DataType lhs_type = lhs_->result_type();
	GraphVector lhs_vec{lhs_type};
	GraphVector rhs_vec{lhs_type};
	lhs_->Evaluate(chunk, count, lhs_vec, ctx);
	rhs_->Evaluate(chunk, count, rhs_vec, ctx);

	DataTypeId tid = lhs_type.id();
	switch (tid) {
	case DataTypeId::kBoolean:
		CompareTyped<bool>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kInt32:
		CompareTyped<int32_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kInt64:
		CompareTyped<int64_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kUInt32:
		CompareTyped<uint32_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kUInt64:
		CompareTyped<uint64_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kFloat:
		CompareTyped<float>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kDouble:
		CompareTyped<double>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kVarchar: {
		auto* l = StringVector::GetStringData(lhs_vec);
		auto* r = StringVector::GetStringData(rhs_vec);
		auto* out = result.GetData<bool>();
		for (size_t i = 0; i < count; i++) {
			std::string ls = l[i].GetString();
			std::string rs = r[i].GetString();
			out[i] = Compare(op_, ls, rs);
		}
		break;
	}
	case DataTypeId::kVertex: {
		const vid_t* l_vids = VertexVector::GetVids(lhs_vec);
		const vid_t* r_vids = VertexVector::GetVids(rhs_vec);
		auto* out = result.GetData<bool>();
		bool l_const_label = VertexVector::IsConstantLabel(lhs_vec);
		bool r_const_label = VertexVector::IsConstantLabel(rhs_vec);
		label_t l_clabel = l_const_label ? VertexVector::GetConstantLabel(lhs_vec) : 0;
		label_t r_clabel = r_const_label ? VertexVector::GetConstantLabel(rhs_vec) : 0;
		const label_t* l_labels = l_const_label ? nullptr : VertexVector::GetLabels(lhs_vec);
		const label_t* r_labels = r_const_label ? nullptr : VertexVector::GetLabels(rhs_vec);
		for (size_t i = 0; i < count; i++) {
			label_t ll = l_const_label ? l_clabel : l_labels[i];
			label_t rl = r_const_label ? r_clabel : r_labels[i];
			bool eq = (ll == rl && l_vids[i] == r_vids[i]);
			switch (op_) {
			case CompareOp::kEq: out[i] = eq; break;
			case CompareOp::kNe: out[i] = !eq; break;
			default: out[i] = false; break;
			}
		}
		break;
	}
	default:
		assert(false && "Unsupported type in VecComparisonExpr");
		break;
	}

	PropagateNullsBinary(lhs_vec, rhs_vec, result, count);
}

// ============================================================
// VecBooleanAndExpr (with SQL three-valued logic)
// ============================================================

VecBooleanAndExpr::VecBooleanAndExpr(std::unique_ptr<VecExpression> lhs,
                                     std::unique_ptr<VecExpression> rhs)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

void VecBooleanAndExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                 GraphVector& result,
                                 const VecExecContext& ctx) {
	DataType bool_type{DataTypeId::kBoolean};
	GraphVector lhs_vec{bool_type};
	GraphVector rhs_vec{bool_type};
	lhs_->Evaluate(chunk, count, lhs_vec, ctx);
	rhs_->Evaluate(chunk, count, rhs_vec, ctx);

	const auto* l = lhs_vec.GetData<bool>();
	const auto* r = rhs_vec.GetData<bool>();
	auto* out = result.GetData<bool>();

	bool lhs_has_nulls = !lhs_vec.validity().AllValid();
	bool rhs_has_nulls = !rhs_vec.validity().AllValid();

	if (!lhs_has_nulls && !rhs_has_nulls) {
		for (size_t i = 0; i < count; i++) {
			out[i] = l[i] && r[i];
		}
	} else {
		// SQL three-valued: NULL AND FALSE = FALSE, NULL AND TRUE = NULL
		for (size_t i = 0; i < count; i++) {
			bool l_valid = lhs_vec.validity().IsValid(i);
			bool r_valid = rhs_vec.validity().IsValid(i);
			if (l_valid && r_valid) {
				out[i] = l[i] && r[i];
			} else if (l_valid && !l[i]) {
				out[i] = false;
			} else if (r_valid && !r[i]) {
				out[i] = false;
			} else {
				out[i] = false;
				result.validity().SetInvalid(i);
			}
		}
	}
}

// ============================================================
// VecBooleanOrExpr (with SQL three-valued logic)
// ============================================================

VecBooleanOrExpr::VecBooleanOrExpr(std::unique_ptr<VecExpression> lhs,
                                   std::unique_ptr<VecExpression> rhs)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

void VecBooleanOrExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                GraphVector& result,
                                const VecExecContext& ctx) {
	DataType bool_type{DataTypeId::kBoolean};
	GraphVector lhs_vec{bool_type};
	GraphVector rhs_vec{bool_type};
	lhs_->Evaluate(chunk, count, lhs_vec, ctx);
	rhs_->Evaluate(chunk, count, rhs_vec, ctx);

	const auto* l = lhs_vec.GetData<bool>();
	const auto* r = rhs_vec.GetData<bool>();
	auto* out = result.GetData<bool>();

	bool lhs_has_nulls = !lhs_vec.validity().AllValid();
	bool rhs_has_nulls = !rhs_vec.validity().AllValid();

	if (!lhs_has_nulls && !rhs_has_nulls) {
		for (size_t i = 0; i < count; i++) {
			out[i] = l[i] || r[i];
		}
	} else {
		// SQL three-valued: NULL OR TRUE = TRUE, NULL OR FALSE = NULL
		for (size_t i = 0; i < count; i++) {
			bool l_valid = lhs_vec.validity().IsValid(i);
			bool r_valid = rhs_vec.validity().IsValid(i);
			if (l_valid && r_valid) {
				out[i] = l[i] || r[i];
			} else if (l_valid && l[i]) {
				out[i] = true;
			} else if (r_valid && r[i]) {
				out[i] = true;
			} else {
				out[i] = false;
				result.validity().SetInvalid(i);
			}
		}
	}
}

// ============================================================
// VecBooleanNotExpr (with null propagation)
// ============================================================

VecBooleanNotExpr::VecBooleanNotExpr(std::unique_ptr<VecExpression> operand)
    : operand_(std::move(operand)) {}

void VecBooleanNotExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                 GraphVector& result,
                                 const VecExecContext& ctx) {
	DataType bool_type{DataTypeId::kBoolean};
	GraphVector child_vec{bool_type};
	operand_->Evaluate(chunk, count, child_vec, ctx);

	const auto* src = child_vec.GetData<bool>();
	auto* out = result.GetData<bool>();
	for (size_t i = 0; i < count; i++) {
		out[i] = !src[i];
	}
	CopyValidity(child_vec, result, count);
}

// ============================================================
// VecArithExpr (with null propagation)
// ============================================================

VecArithExpr::VecArithExpr(std::unique_ptr<VecExpression> lhs, ArithOp op,
                           std::unique_ptr<VecExpression> rhs)
    : lhs_(std::move(lhs)),
      rhs_(std::move(rhs)),
      op_(op),
      result_type_(lhs_->result_type()) {}

template <typename T>
static T ApplyArith(ArithOp op, T lhs, T rhs) {
	switch (op) {
	case ArithOp::kAdd:
		return lhs + rhs;
	case ArithOp::kSub:
		return lhs - rhs;
	case ArithOp::kMul:
		return lhs * rhs;
	case ArithOp::kDiv:
		return lhs / rhs;
	case ArithOp::kMod:
		if constexpr (std::is_floating_point_v<T>) {
			return static_cast<T>(0);
		} else {
			return lhs % rhs;
		}
	}
	return T{};
}

template <>
float ApplyArith<float>(ArithOp op, float lhs, float rhs) {
	switch (op) {
	case ArithOp::kAdd:
		return lhs + rhs;
	case ArithOp::kSub:
		return lhs - rhs;
	case ArithOp::kMul:
		return lhs * rhs;
	case ArithOp::kDiv:
		return lhs / rhs;
	case ArithOp::kMod:
		return std::fmod(lhs, rhs);
	}
	return 0.0f;
}

template <>
double ApplyArith<double>(ArithOp op, double lhs, double rhs) {
	switch (op) {
	case ArithOp::kAdd:
		return lhs + rhs;
	case ArithOp::kSub:
		return lhs - rhs;
	case ArithOp::kMul:
		return lhs * rhs;
	case ArithOp::kDiv:
		return lhs / rhs;
	case ArithOp::kMod:
		return std::fmod(lhs, rhs);
	}
	return 0.0;
}

template <typename T>
static void ArithTyped(const GraphVector& lhs_vec, const GraphVector& rhs_vec,
                       ArithOp op, size_t count, GraphVector& result) {
	const auto* l = lhs_vec.GetData<T>();
	const auto* r = rhs_vec.GetData<T>();
	auto* out = result.GetData<T>();
	for (size_t i = 0; i < count; i++) {
		out[i] = ApplyArith(op, l[i], r[i]);
	}
}

void VecArithExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                            GraphVector& result,
                            const VecExecContext& ctx) {
	DataType child_type = lhs_->result_type();
	GraphVector lhs_vec{child_type};
	GraphVector rhs_vec{child_type};
	lhs_->Evaluate(chunk, count, lhs_vec, ctx);
	rhs_->Evaluate(chunk, count, rhs_vec, ctx);

	DataTypeId tid = child_type.id();
	switch (tid) {
	case DataTypeId::kInt32:
		ArithTyped<int32_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kInt64:
		ArithTyped<int64_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kUInt32:
		ArithTyped<uint32_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kUInt64:
		ArithTyped<uint64_t>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kFloat:
		ArithTyped<float>(lhs_vec, rhs_vec, op_, count, result);
		break;
	case DataTypeId::kDouble:
		ArithTyped<double>(lhs_vec, rhs_vec, op_, count, result);
		break;
	default:
		assert(false && "Unsupported type in VecArithExpr");
		break;
	}

	PropagateNullsBinary(lhs_vec, rhs_vec, result, count);
}

// ============================================================
// VecUnaryMinusExpr
// ============================================================

VecUnaryMinusExpr::VecUnaryMinusExpr(std::unique_ptr<VecExpression> operand)
    : operand_(std::move(operand)) {}

template <typename T>
static void NegateTyped(const GraphVector& src, size_t count,
                        GraphVector& result) {
	const auto* in = src.GetData<T>();
	auto* out = result.GetData<T>();
	for (size_t i = 0; i < count; i++) {
		out[i] = -in[i];
	}
}

void VecUnaryMinusExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                 GraphVector& result,
                                 const VecExecContext& ctx) {
	DataType child_type = operand_->result_type();
	GraphVector child_vec{child_type};
	operand_->Evaluate(chunk, count, child_vec, ctx);

	DataTypeId tid = child_type.id();
	switch (tid) {
	case DataTypeId::kInt32:
		NegateTyped<int32_t>(child_vec, count, result);
		break;
	case DataTypeId::kInt64:
		NegateTyped<int64_t>(child_vec, count, result);
		break;
	case DataTypeId::kFloat:
		NegateTyped<float>(child_vec, count, result);
		break;
	case DataTypeId::kDouble:
		NegateTyped<double>(child_vec, count, result);
		break;
	default:
		assert(false && "Unsupported type in VecUnaryMinusExpr");
		break;
	}

	CopyValidity(child_vec, result, count);
}

// ============================================================
// VecCastExpr
// ============================================================

VecCastExpr::VecCastExpr(std::unique_ptr<VecExpression> child,
                         DataType target_type)
    : child_(std::move(child)), target_type_(std::move(target_type)) {}

template <typename Src, typename Dst>
static void CastTyped(const GraphVector& src_vec, size_t count,
                      GraphVector& result) {
	const auto* in = src_vec.GetData<Src>();
	auto* out = result.GetData<Dst>();
	for (size_t i = 0; i < count; i++) {
		out[i] = static_cast<Dst>(in[i]);
	}
}

// Dispatch: for each (src_type, dst_type) pair, call CastTyped<Src,Dst>
#define CAST_CASE(src_tid, src_cpp, dst_tid, dst_cpp) \
	if (src_id == DataTypeId::src_tid && dst_id == DataTypeId::dst_tid) { \
		CastTyped<src_cpp, dst_cpp>(child_vec, count, result); \
		CopyValidity(child_vec, result, count); \
		return; \
	}

void VecCastExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                           GraphVector& result,
                           const VecExecContext& ctx) {
	DataType child_type = child_->result_type();
	GraphVector child_vec{child_type};
	child_->Evaluate(chunk, count, child_vec, ctx);

	DataTypeId src_id = child_type.id();
	DataTypeId dst_id = target_type_.id();

	if (src_id == dst_id) {
		CopyVectorData(child_vec, result, src_id, count);
		CopyValidity(child_vec, result, count);
		return;
	}

	// Integer promotions
	CAST_CASE(kInt32, int32_t, kInt64, int64_t)
	CAST_CASE(kInt32, int32_t, kFloat, float)
	CAST_CASE(kInt32, int32_t, kDouble, double)
	CAST_CASE(kInt64, int64_t, kDouble, double)
	CAST_CASE(kInt64, int64_t, kFloat, float)
	CAST_CASE(kUInt32, uint32_t, kUInt64, uint64_t)
	CAST_CASE(kUInt32, uint32_t, kInt64, int64_t)
	CAST_CASE(kUInt32, uint32_t, kDouble, double)
	CAST_CASE(kUInt64, uint64_t, kDouble, double)
	CAST_CASE(kFloat, float, kDouble, double)
	// Unsigned to signed
	CAST_CASE(kUInt32, uint32_t, kInt32, int32_t)
	CAST_CASE(kUInt64, uint64_t, kInt64, int64_t)
	// Signed to unsigned
	CAST_CASE(kInt32, int32_t, kUInt32, uint32_t)
	CAST_CASE(kInt64, int64_t, kUInt64, uint64_t)
	// Float to int
	CAST_CASE(kFloat, float, kInt32, int32_t)
	CAST_CASE(kFloat, float, kInt64, int64_t)
	CAST_CASE(kDouble, double, kInt32, int32_t)
	CAST_CASE(kDouble, double, kInt64, int64_t)
	CAST_CASE(kDouble, double, kFloat, float)

	assert(false && "Unsupported cast in VecCastExpr");
}

#undef CAST_CASE

// ============================================================
// VecIsNullExpr
// ============================================================

VecIsNullExpr::VecIsNullExpr(std::unique_ptr<VecExpression> operand,
                             bool negate)
    : operand_(std::move(operand)), negate_(negate) {}

void VecIsNullExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                             GraphVector& result,
                             const VecExecContext& ctx) {
	DataType child_type = operand_->result_type();
	GraphVector child_vec{child_type};
	operand_->Evaluate(chunk, count, child_vec, ctx);

	auto* out = result.GetData<bool>();
	if (child_vec.validity().AllValid()) {
		// All valid → IS NULL = all false, IS NOT NULL = all true
		bool val = negate_;
		for (size_t i = 0; i < count; i++) {
			out[i] = val;
		}
	} else {
		for (size_t i = 0; i < count; i++) {
			bool is_null = !child_vec.validity().IsValid(i);
			out[i] = negate_ ? !is_null : is_null;
		}
	}
}

// ============================================================
// VecStringFuncExpr
// ============================================================

VecStringFuncExpr::VecStringFuncExpr(std::unique_ptr<VecExpression> str_expr,
                                     std::unique_ptr<VecExpression> pattern_expr,
                                     StringFuncOp op)
    : str_expr_(std::move(str_expr)),
      pattern_expr_(std::move(pattern_expr)),
      op_(op) {}

static bool StringStartsWith(const std::string& str,
                             const std::string& prefix) {
	if (prefix.size() > str.size()) return false;
	return str.compare(0, prefix.size(), prefix) == 0;
}

static bool StringEndsWith(const std::string& str,
                           const std::string& suffix) {
	if (suffix.size() > str.size()) return false;
	return str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static bool StringContains(const std::string& str,
                           const std::string& substr) {
	return str.find(substr) != std::string::npos;
}

void VecStringFuncExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                 GraphVector& result,
                                 const VecExecContext& ctx) {
	DataType str_type{DataTypeId::kVarchar};
	GraphVector str_vec{str_type};
	GraphVector pat_vec{str_type};
	str_expr_->Evaluate(chunk, count, str_vec, ctx);
	pattern_expr_->Evaluate(chunk, count, pat_vec, ctx);

	auto* str_data = StringVector::GetStringData(str_vec);
	auto* pat_data = StringVector::GetStringData(pat_vec);
	auto* out = result.GetData<bool>();

	for (size_t i = 0; i < count; i++) {
		std::string s = str_data[i].GetString();
		std::string p = pat_data[i].GetString();
		switch (op_) {
		case StringFuncOp::kStartsWith:
			out[i] = StringStartsWith(s, p);
			break;
		case StringFuncOp::kEndsWith:
			out[i] = StringEndsWith(s, p);
			break;
		case StringFuncOp::kContains:
			out[i] = StringContains(s, p);
			break;
		}
	}

	PropagateNullsBinary(str_vec, pat_vec, result, count);
}

// ============================================================
// VecInListExpr
// ============================================================

VecInListExpr::VecInListExpr(std::unique_ptr<VecExpression> value_expr,
                             std::vector<execution::Value> list)
    : value_expr_(std::move(value_expr)), list_(std::move(list)) {}

template <typename T>
static void InListTyped(const GraphVector& val_vec,
                        const std::vector<execution::Value>& list,
                        size_t count, GraphVector& result) {
	const auto* data = val_vec.GetData<T>();
	auto* out = result.GetData<bool>();

	// Pre-extract typed values from the list
	std::vector<T> typed_list;
	typed_list.reserve(list.size());
	for (const auto& v : list) {
		if (!v.IsNull()) {
			typed_list.push_back(v.GetValue<T>());
		}
	}

	for (size_t i = 0; i < count; i++) {
		bool found = false;
		for (const auto& lv : typed_list) {
			if (data[i] == lv) {
				found = true;
				break;
			}
		}
		out[i] = found;
	}
}

void VecInListExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                             GraphVector& result,
                             const VecExecContext& ctx) {
	DataType val_type = value_expr_->result_type();
	GraphVector val_vec{val_type};
	value_expr_->Evaluate(chunk, count, val_vec, ctx);

	DataTypeId tid = val_type.id();
	switch (tid) {
	case DataTypeId::kInt32:
		InListTyped<int32_t>(val_vec, list_, count, result);
		break;
	case DataTypeId::kInt64:
		InListTyped<int64_t>(val_vec, list_, count, result);
		break;
	case DataTypeId::kUInt32:
		InListTyped<uint32_t>(val_vec, list_, count, result);
		break;
	case DataTypeId::kUInt64:
		InListTyped<uint64_t>(val_vec, list_, count, result);
		break;
	case DataTypeId::kFloat:
		InListTyped<float>(val_vec, list_, count, result);
		break;
	case DataTypeId::kDouble:
		InListTyped<double>(val_vec, list_, count, result);
		break;
	case DataTypeId::kVarchar: {
		auto* str_data = StringVector::GetStringData(val_vec);
		auto* out = result.GetData<bool>();
		std::vector<std::string> str_list;
		for (const auto& v : list_) {
			if (!v.IsNull()) {
				str_list.push_back(v.GetValue<std::string>());
			}
		}
		for (size_t i = 0; i < count; i++) {
			std::string s = str_data[i].GetString();
			bool found = false;
			for (const auto& lv : str_list) {
				if (s == lv) {
					found = true;
					break;
				}
			}
			out[i] = found;
		}
		break;
	}
	default:
		assert(false && "Unsupported type in VecInListExpr");
		break;
	}

	CopyValidity(val_vec, result, count);
}

// ============================================================
// VecCaseWhenExpr
// ============================================================

VecCaseWhenExpr::VecCaseWhenExpr(std::vector<WhenThen> when_thens,
                                 std::unique_ptr<VecExpression> else_expr,
                                 DataType result_type)
    : when_thens_(std::move(when_thens)),
      else_expr_(std::move(else_expr)),
      result_type_(std::move(result_type)) {}

void VecCaseWhenExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                               GraphVector& result,
                               const VecExecContext& ctx) {
	// Track which rows are already resolved
	std::vector<bool> resolved(count, false);

	DataType bool_type{DataTypeId::kBoolean};
	size_t elem_size = GetTypeSize(result_type_.id());
	bool is_string = (result_type_.id() == DataTypeId::kVarchar);

	for (auto& [when_expr, then_expr] : when_thens_) {
		GraphVector when_vec{bool_type};
		when_expr->Evaluate(chunk, count, when_vec, ctx);
		const auto* flags = when_vec.GetData<bool>();

		// Check if any unresolved row matches this WHEN
		bool any_match = false;
		for (size_t i = 0; i < count; i++) {
			if (!resolved[i] && when_vec.validity().IsValid(i) && flags[i]) {
				any_match = true;
				break;
			}
		}
		if (!any_match) continue;

		// Evaluate THEN for matching rows
		GraphVector then_vec{result_type_};
		then_expr->Evaluate(chunk, count, then_vec, ctx);

		if (is_string) {
			auto* then_data = StringVector::GetStringData(then_vec);
			auto* result_data = StringVector::GetStringData(result);
			for (size_t i = 0; i < count; i++) {
				if (!resolved[i] && when_vec.validity().IsValid(i) && flags[i]) {
					result_data[i] = StringVector::AddString(
					    result, then_data[i].GetString());
					if (!then_vec.validity().IsValid(i)) {
						result.validity().SetInvalid(i);
					}
					resolved[i] = true;
				}
			}
		} else {
			auto* then_data = then_vec.GetData<char>();
			auto* result_data = result.GetData<char>();
			for (size_t i = 0; i < count; i++) {
				if (!resolved[i] && when_vec.validity().IsValid(i) && flags[i]) {
					std::memcpy(result_data + i * elem_size,
					            then_data + i * elem_size, elem_size);
					if (!then_vec.validity().IsValid(i)) {
						result.validity().SetInvalid(i);
					}
					resolved[i] = true;
				}
			}
		}
	}

	// ELSE branch for unresolved rows
	bool has_unresolved = false;
	for (size_t i = 0; i < count; i++) {
		if (!resolved[i]) {
			has_unresolved = true;
			break;
		}
	}

	if (has_unresolved) {
		if (else_expr_) {
			GraphVector else_vec{result_type_};
			else_expr_->Evaluate(chunk, count, else_vec, ctx);

			if (is_string) {
				auto* else_data = StringVector::GetStringData(else_vec);
				auto* result_data = StringVector::GetStringData(result);
				for (size_t i = 0; i < count; i++) {
					if (!resolved[i]) {
						result_data[i] = StringVector::AddString(
						    result, else_data[i].GetString());
						if (!else_vec.validity().IsValid(i)) {
							result.validity().SetInvalid(i);
						}
					}
				}
			} else {
				auto* else_data = else_vec.GetData<char>();
				auto* result_data = result.GetData<char>();
				for (size_t i = 0; i < count; i++) {
					if (!resolved[i]) {
						std::memcpy(result_data + i * elem_size,
						            else_data + i * elem_size, elem_size);
						if (!else_vec.validity().IsValid(i)) {
							result.validity().SetInvalid(i);
						}
					}
				}
			}
		} else {
			// No ELSE → null for unresolved rows
			for (size_t i = 0; i < count; i++) {
				if (!resolved[i]) {
					result.validity().SetInvalid(i);
				}
			}
		}
	}
}

// ============================================================
// VecParamExpr
// ============================================================

VecParamExpr::VecParamExpr(std::string name, int index, DataType type)
    : name_(std::move(name)), index_(index), type_(std::move(type)) {}

void VecParamExpr::Evaluate(const GraphDataChunk&, size_t count,
                            GraphVector& result,
                            const VecExecContext& ctx) {
	assert(ctx.params != nullptr && "VecParamExpr requires params in context");
	auto it = ctx.params->find(name_);
	if (it == ctx.params->end()) {
		result.validity().SetAllInvalid();
		return;
	}
	const auto& val = it->second;
	if (val.IsNull()) {
		result.validity().SetAllInvalid();
		return;
	}
	DataTypeId tid = type_.id();
	switch (tid) {
	case DataTypeId::kBoolean:
		FillConstant(result, val.GetValue<bool>(), count);
		break;
	case DataTypeId::kInt32:
		FillConstant(result, val.GetValue<int32_t>(), count);
		break;
	case DataTypeId::kInt64:
		FillConstant(result, val.GetValue<int64_t>(), count);
		break;
	case DataTypeId::kUInt32:
		FillConstant(result, val.GetValue<uint32_t>(), count);
		break;
	case DataTypeId::kUInt64:
		FillConstant(result, val.GetValue<uint64_t>(), count);
		break;
	case DataTypeId::kFloat:
		FillConstant(result, val.GetValue<float>(), count);
		break;
	case DataTypeId::kDouble:
		FillConstant(result, val.GetValue<double>(), count);
		break;
	case DataTypeId::kVarchar: {
		std::string str_val = val.GetValue<std::string>();
		auto* str_data = StringVector::GetStringData(result);
		for (size_t i = 0; i < count; i++) {
			str_data[i] = StringVector::AddString(result, str_val);
		}
		break;
	}
	default:
		assert(false && "Unsupported type in VecParamExpr");
		break;
	}
}

void VecPathLengthExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                 GraphVector& result,
                                 const VecExecContext&) {
	result.Initialize(DataType{DataTypeId::kInt64}, count);
	int col_idx = chunk.FindColumnByTag(tag_);
	if (col_idx < 0) {
		auto* data = result.GetData<int64_t>();
		for (size_t i = 0; i < count; i++) data[i] = 0;
		return;
	}
	const auto& vec = chunk.GetVector(static_cast<size_t>(col_idx));
	auto* out = result.GetData<int64_t>();
	const bool has_sel = vec.IsDictionary();
	const SelectionVector* sel = has_sel ? &vec.sel() : nullptr;
	for (size_t i = 0; i < count; i++) {
		size_t idx = has_sel ? sel->GetIndex(i) : i;
		const auto& path = PathVector::GetPath(vec, idx);
		out[i] = path.is_null() ? 0 : static_cast<int64_t>(path.length());
	}
}

// ============================================================
// VecExtractExpr
// ============================================================

VecExtractExpr::VecExtractExpr(std::unique_ptr<VecExpression> operand,
                               ExtractInterval interval)
    : operand_(std::move(operand)), interval_(interval) {}

void VecExtractExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                              GraphVector& result, const VecExecContext& ctx) {
	GraphVector tmp(operand_->result_type(), count);
	operand_->Evaluate(chunk, count, tmp, ctx);

	result.Initialize(DataType{DataTypeId::kInt64}, count);
	auto* out = result.GetData<int64_t>();
	auto src_type = operand_->result_type().id();

	bool has_nulls = !tmp.validity().AllValid();
	for (size_t i = 0; i < count; i++) {
		if (has_nulls && !tmp.validity().IsValid(i)) {
			result.validity().SetInvalid(i);
			continue;
		}
		if (src_type == DataTypeId::kTimestampMs) {
			int64_t ms = tmp.GetData<int64_t>()[i];
			int64_t sec = ms / 1000;
			struct tm tm;
			gmtime_r(reinterpret_cast<time_t*>(&sec), &tm);
			switch (interval_) {
			case ExtractInterval::kYear:
				out[i] = tm.tm_year + 1900; break;
			case ExtractInterval::kMonth:
				out[i] = tm.tm_mon + 1; break;
			case ExtractInterval::kDay:
				out[i] = tm.tm_mday; break;
			case ExtractInterval::kHour:
				out[i] = tm.tm_hour; break;
			case ExtractInterval::kMinute:
				out[i] = tm.tm_min; break;
			case ExtractInterval::kSecond:
				out[i] = tm.tm_sec; break;
			case ExtractInterval::kMillisecond:
				out[i] = ms % 1000; break;
			}
		} else if (src_type == DataTypeId::kDate) {
			Date d;
			d.value.integer = static_cast<uint32_t>(tmp.GetData<int64_t>()[i]);
			switch (interval_) {
			case ExtractInterval::kYear:
				out[i] = d.year(); break;
			case ExtractInterval::kMonth:
				out[i] = d.month(); break;
			case ExtractInterval::kDay:
				out[i] = d.day(); break;
			case ExtractInterval::kHour:
				out[i] = d.hour(); break;
			default:
				out[i] = 0; break;
			}
		} else {
			out[i] = 0;
		}
	}
}

// ============================================================
// VecListExtractExpr
// ============================================================

VecListExtractExpr::VecListExtractExpr(
    std::unique_ptr<VecExpression> list_expr,
    std::unique_ptr<VecExpression> index_expr, DataType elem_type)
    : list_expr_(std::move(list_expr)),
      index_expr_(std::move(index_expr)),
      elem_type_(std::move(elem_type)) {}

void VecListExtractExpr::Evaluate(const GraphDataChunk& chunk, size_t count,
                                  GraphVector& result,
                                  const VecExecContext& ctx) {
	GraphVector list_vec(list_expr_->result_type(), count);
	list_expr_->Evaluate(chunk, count, list_vec, ctx);

	GraphVector idx_vec(DataType{DataTypeId::kInt64}, count);
	index_expr_->Evaluate(chunk, count, idx_vec, ctx);

	const auto* entries = ListVector::GetEntries(list_vec);
	const auto& child = ListVector::GetChild(list_vec);
	const auto* idx_data = idx_vec.GetData<int64_t>();

	switch (elem_type_.id()) {
	case DataTypeId::kInt64: {
		auto* out = result.GetData<int64_t>();
		const auto* cdata = child.GetData<int64_t>();
		for (size_t i = 0; i < count; i++) {
			int64_t idx = idx_data[i];
			if (idx >= 0 && static_cast<uint64_t>(idx) < entries[i].length) {
				out[i] = cdata[entries[i].offset + idx];
			} else {
				out[i] = 0;
			}
		}
		break;
	}
	case DataTypeId::kDouble: {
		auto* out = result.GetData<double>();
		const auto* cdata = child.GetData<double>();
		for (size_t i = 0; i < count; i++) {
			int64_t idx = idx_data[i];
			if (idx >= 0 && static_cast<uint64_t>(idx) < entries[i].length) {
				out[i] = cdata[entries[i].offset + idx];
			} else {
				out[i] = 0.0;
			}
		}
		break;
	}
	case DataTypeId::kVarchar: {
		auto* cdata = StringVector::GetStringData(
		    const_cast<GraphVector&>(child));
		auto* out = StringVector::GetStringData(result);
		for (size_t i = 0; i < count; i++) {
			int64_t idx = idx_data[i];
			if (idx >= 0 && static_cast<uint64_t>(idx) < entries[i].length) {
				out[i] = StringVector::AddString(
				    result, cdata[entries[i].offset + idx].GetString());
			} else {
				out[i] = StringVector::AddString(result, "");
			}
		}
		break;
	}
	default: {
		auto* out = result.GetData<int64_t>();
		for (size_t i = 0; i < count; i++) out[i] = 0;
		break;
	}
	}
}

}  // namespace neug::execution::vec
