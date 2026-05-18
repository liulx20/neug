#include "neug/execution/vectorized/ops/vec_select_operator.h"

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/core/selection_vector.h"

namespace neug::execution::vec {

VecSelectOperator::VecSelectOperator(int column_tag, CompareOp op,
                                     neug::execution::Value constant)
    : column_tag_(column_tag), op_(op), constant_(std::move(constant)) {}

std::unique_ptr<OperatorState> VecSelectOperator::GetOperatorState() const {
	return std::make_unique<OperatorState>();
}

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
static size_t SelectTyped(const GraphVector& vec, size_t count, CompareOp op,
                          T constant, SelectionVector& sel) {
	const auto* data = vec.GetData<T>();
	size_t selected = 0;
	for (size_t i = 0; i < count; i++) {
		if (Compare(op, data[i], constant)) {
			sel.SetIndex(selected++, static_cast<sel_t>(i));
		}
	}
	return selected;
}

OperatorResultType VecSelectOperator::Execute(GraphDataChunk& input,
                                               GraphDataChunk& output,
                                               OperatorState&,
                                                   const VecExecContext& ctx) {
	size_t count = input.size();
	if (count == 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	input.Flatten();

	int col_idx = input.FindColumnByTag(column_tag_);
	assert(col_idx >= 0);
	auto& vec = input.GetVector(static_cast<size_t>(col_idx));

	SelectionVector sel(count);
	size_t selected = 0;
	DataTypeId type_id = vec.type_id();

	switch (type_id) {
	case DataTypeId::kInt32:
		selected = SelectTyped<int32_t>(vec, count, op_,
		                                constant_.GetValue<int32_t>(), sel);
		break;
	case DataTypeId::kInt64:
		selected = SelectTyped<int64_t>(vec, count, op_,
		                                constant_.GetValue<int64_t>(), sel);
		break;
	case DataTypeId::kUInt32:
		selected = SelectTyped<uint32_t>(vec, count, op_,
		                                 constant_.GetValue<uint32_t>(), sel);
		break;
	case DataTypeId::kUInt64:
		selected = SelectTyped<uint64_t>(vec, count, op_,
		                                 constant_.GetValue<uint64_t>(), sel);
		break;
	case DataTypeId::kFloat:
		selected = SelectTyped<float>(vec, count, op_,
		                              constant_.GetValue<float>(), sel);
		break;
	case DataTypeId::kDouble:
		selected = SelectTyped<double>(vec, count, op_,
		                               constant_.GetValue<double>(), sel);
		break;
	default:
		assert(false && "Unsupported type in VecSelectOperator");
		break;
	}

	if (selected == 0) {
		output = std::move(input);
		output.SetCardinality(0);
	} else if (selected == count) {
		output = std::move(input);
	} else {
		input.Compact(sel, selected);
		output = std::move(input);
	}
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
