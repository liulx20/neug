#include "neug/execution/vectorized/ops/property_read_operator.h"

#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/column.h"

namespace neug::execution::vec {

PropertyReadOperator::PropertyReadOperator(int vertex_tag,
                                           std::string prop_name,
                                           int output_tag)
    : vertex_tag_(vertex_tag),
      prop_name_(std::move(prop_name)),
      output_tag_(output_tag) {}

std::unique_ptr<OperatorState> PropertyReadOperator::GetOperatorState() const {
	return std::make_unique<OperatorState>();
}

template <typename T>
static void ReadTypedProperty(const TypedRefColumn<T>& col,
                              const vid_t* vids, size_t count,
                              GraphVector& out_vec) {
	auto* data = out_vec.GetData<T>();
	for (size_t i = 0; i < count; i++) {
		data[i] = col.get_view(vids[i]);
	}
}

static void ReadStringProperty(const TypedRefColumn<std::string_view>& col,
                                const vid_t* vids, size_t count,
                                GraphVector& out_vec) {
	auto* str_data = StringVector::GetStringData(out_vec);
	for (size_t i = 0; i < count; i++) {
		std::string_view sv = col.get_view(vids[i]);
		str_data[i] = StringVector::AddString(out_vec, std::string(sv));
	}
}

OperatorResultType PropertyReadOperator::Execute(GraphDataChunk& input,
                                                  GraphDataChunk& output,
                                                  OperatorState&,
                                                   const VecExecContext& ctx) {
	size_t count = input.size();
	if (count == 0) {
		output = std::move(input);
		return OperatorResultType::kNeedMoreInput;
	}

	int col_idx = input.FindColumnByTag(vertex_tag_);
	assert(col_idx >= 0);
	auto& vertex_vec = input.GetVector(static_cast<size_t>(col_idx));
	if (vertex_vec.IsDictionary()) {
		Flatten(vertex_vec, count);
	}
	const vid_t* vids = VertexVector::GetVids(vertex_vec);

	label_t label;
	if (VertexVector::IsConstantLabel(vertex_vec)) {
		label = VertexVector::GetConstantLabel(vertex_vec);
	} else {
		label = VertexVector::GetLabels(vertex_vec)[0];
	}

	auto ref_col = ctx.graph->GetVertexPropColumn(label, prop_name_);
	assert(ref_col != nullptr);

	DataTypeId type_id = ref_col->type();
	DataType prop_type{type_id};
	GraphVector prop_vec{prop_type};

	switch (type_id) {
	case DataTypeId::kInt32:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<int32_t>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kInt64:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<int64_t>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kUInt32:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<uint32_t>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kUInt64:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<uint64_t>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kFloat:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<float>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kDouble:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<double>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kBoolean:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<bool>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kDate: {
		auto& col = static_cast<const TypedRefColumn<Date>&>(*ref_col);
		auto* data = prop_vec.GetData<int64_t>();
		for (size_t i = 0; i < count; i++) {
			Date d = col.get_view(vids[i]);
			data[i] = static_cast<int64_t>(d.value.integer);
		}
		break;
	}
	case DataTypeId::kTimestampMs:
		ReadTypedProperty(
		    static_cast<const TypedRefColumn<DateTime>&>(*ref_col), vids, count,
		    prop_vec);
		break;
	case DataTypeId::kVarchar:
		ReadStringProperty(
		    static_cast<const TypedRefColumn<std::string_view>&>(*ref_col),
		    vids, count, prop_vec);
		break;
	default:
		assert(false && "Unsupported property type in PropertyReadOperator");
		break;
	}

	input.AddColumn(output_tag_, std::move(prop_vec));
	output = std::move(input);
	return OperatorResultType::kNeedMoreInput;
}

}  // namespace neug::execution::vec
