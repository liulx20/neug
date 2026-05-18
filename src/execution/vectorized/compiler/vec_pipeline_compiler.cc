#include "neug/execution/vectorized/compiler/vec_pipeline_compiler.h"

#include <algorithm>
#include <stack>
#include <stdexcept>

#include "neug/common/extra_type_info.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/vectorized/ops/edge_expand_operator.h"
#include "neug/execution/vectorized/ops/get_v_filter_operator.h"
#include "neug/execution/vectorized/ops/tc_fuse_operator.h"
#include "neug/execution/vectorized/ops/dedup_operator.h"
#include "neug/execution/vectorized/ops/path_expand_operator.h"
#include "neug/execution/vectorized/ops/all_shortest_path_operator.h"
#include "neug/execution/vectorized/ops/shortest_path_operator.h"
#include "neug/execution/vectorized/ops/unfold_operator.h"
#include "neug/execution/vectorized/ops/union_source.h"
#include "neug/execution/vectorized/ops/intersect_operator.h"
#include "neug/execution/vectorized/ops/limit_operator.h"
#include "neug/execution/vectorized/ops/sort_sink.h"
#include "neug/execution/vectorized/ops/sort_source.h"
#include "neug/execution/vectorized/ops/topn_sink.h"
#include "neug/execution/vectorized/ops/hash_aggregate_sink.h"
#include "neug/execution/vectorized/ops/hash_aggregate_source.h"
#include "neug/execution/vectorized/ops/hash_join_build_sink.h"
#include "neug/execution/vectorized/ops/hash_join_probe_operator.h"
#include "neug/execution/vectorized/ops/property_read_operator.h"
#include "neug/execution/vectorized/ops/project_operator.h"
#include "neug/execution/vectorized/ops/result_sink.h"
#include "neug/execution/vectorized/ops/vec_expr_filter_operator.h"
#include "neug/execution/vectorized/ops/index_scan_source.h"
#include "neug/execution/vectorized/ops/materialize_sink.h"
#include "neug/execution/vectorized/ops/materialize_source.h"
#include "neug/execution/vectorized/ops/vertex_scan_source.h"
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"
#include "neug/execution/expression/special_predicates.h"
#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/storages/graph/schema.h"

#include "neug/generated/proto/plan/expr.pb.h"

namespace neug::execution::vec {

static bool SubPlanHasSource(const physical::PhysicalPlan& plan) {
	for (int i = 0; i < plan.plan_size(); i++) {
		if (plan.plan(i).opr().op_kind_case() ==
		    physical::PhysicalOpr_Operator::kScan)
			return true;
	}
	return false;
}

// ============================================================
// TC fuse pattern detection
// ============================================================

static bool tc_fusable_vec(const physical::PhysicalPlan& plan, int op_idx) {
	if (op_idx + 7 >= plan.plan_size()) return false;

	auto get_kind = [&](int i) {
		return plan.plan(i).opr().op_kind_case();
	};
	if (get_kind(op_idx) != physical::PhysicalOpr_Operator::kEdge ||
	    get_kind(op_idx + 1) != physical::PhysicalOpr_Operator::kVertex ||
	    get_kind(op_idx + 2) != physical::PhysicalOpr_Operator::kProject ||
	    get_kind(op_idx + 3) != physical::PhysicalOpr_Operator::kGroupBy ||
	    get_kind(op_idx + 4) != physical::PhysicalOpr_Operator::kProject ||
	    get_kind(op_idx + 5) != physical::PhysicalOpr_Operator::kEdge ||
	    get_kind(op_idx + 6) != physical::PhysicalOpr_Operator::kEdge ||
	    get_kind(op_idx + 7) != physical::PhysicalOpr_Operator::kSelect) {
		return false;
	}

	const auto& ee_opr0 = plan.plan(op_idx).opr().edge();
	if (ee_opr0.is_optional() || !ee_opr0.has_v_tag() || !ee_opr0.has_alias())
		return false;
	if (!ee_opr0.params().has_predicate()) return false;

	auto sp_pred =
	    execution::parse_sp_pred(ee_opr0.params().predicate());
	if (sp_pred != execution::SPPredicateType::kPropertyGT &&
	    sp_pred != execution::SPPredicateType::kPropertyLT)
		return false;
	const auto& op2 = ee_opr0.params().predicate().operators(2);
	if (op2.item_case() != common::ExprOpr::ItemCase::kParam) return false;

	int start_tag = ee_opr0.v_tag().value();
	auto dir0 = ee_opr0.direction();
	if (dir0 == physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH)
		return false;

	const auto& vertex = plan.plan(op_idx + 1).opr().vertex();
	int alias1 = vertex.has_alias() ? vertex.alias().value() : -1;

	const auto& group_by_opr = plan.plan(op_idx + 3).opr().group_by();
	if (group_by_opr.mappings_size() != 1 || group_by_opr.functions_size() != 1)
		return false;
	auto mapping = group_by_opr.mappings(0);
	if (!mapping.has_key() || mapping.key().tag().id() != start_tag)
		return false;
	const auto& func = group_by_opr.functions(0);
	if (func.aggregate() != physical::GroupBy_AggFunc::TO_SET) return false;
	if (func.vars_size() != 1 || !func.vars(0).has_tag() ||
	    func.vars(0).tag().id() != alias1 || func.vars(0).has_property())
		return false;
	int alias4 = func.alias().value();

	const auto& ee_opr1 = plan.plan(op_idx + 5).opr().edge();
	if (ee_opr1.is_optional() || !ee_opr1.has_v_tag())
		return false;
	if (ee_opr1.direction() ==
	    physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH)
		return false;
	if (ee_opr1.params().has_predicate()) return false;

	const auto& ee_opr2 = plan.plan(op_idx + 6).opr().edge();
	if (ee_opr2.is_optional() || !ee_opr2.has_v_tag() || !ee_opr2.has_alias())
		return false;
	if (ee_opr2.direction() ==
	    physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH)
		return false;
	if (ee_opr2.params().has_predicate()) return false;

	int alias7 = ee_opr2.alias().value();

	const auto& select_opr = plan.plan(op_idx + 7).opr().select();
	if (select_opr.predicate().operators_size() != 3) return false;
	auto& var = select_opr.predicate().operators(0);
	auto& within = select_opr.predicate().operators(1);
	auto& v_set = select_opr.predicate().operators(2);
	if (!var.has_var() || !var.var().has_tag() || var.var().has_property())
		return false;
	if (var.var().tag().id() != alias7) return false;
	if (within.item_case() != common::ExprOpr::ItemCase::kLogical ||
	    within.logical() != common::Logical::WITHIN)
		return false;
	if (!v_set.has_var() || !v_set.var().has_tag() || v_set.var().has_property())
		return false;
	if (v_set.var().tag().id() != alias4) return false;

	return true;
}

// ============================================================
// CanVectorize: static boundary check
// ============================================================

static bool HasUnsupportedExpression(const ::common::Expression& expr) {
	for (int i = 0; i < expr.operators_size(); ++i) {
		const auto& opr = expr.operators(i);
		if (opr.has_udf_func()) return true;
		if (opr.has_scalar_func()) {
			const auto& name = opr.scalar_func().unique_name();
			if (name.find("LIST_EXTRACT") == std::string::npos &&
			    name.find("CAST") == std::string::npos)
				return true;
		}
		if (opr.has_path_func()) return true;
		if (opr.has_var()) {
			const auto& var = opr.var();
			if (var.has_property() && var.property().has_id()) return true;
		}
		if (opr.has_logical() &&
		    opr.logical() == ::common::Logical::REGEX) return true;
	}
	return false;
}

bool VecPipelineCompiler::CanVectorize(const physical::PhysicalPlan& plan) {
	if (plan.has_flag()) {
		const auto& f = plan.flag();
		if (f.insert() || f.update() || f.schema() || f.batch() ||
		    f.create_temp_table() || f.checkpoint() || f.procedure_call())
			return false;
	}

	for (int i = 0; i < plan.plan_size(); ++i) {
		if (i + 7 < plan.plan_size() && tc_fusable_vec(plan, i)) {
			i += 7;
			continue;
		}
		const auto& op = plan.plan(i).opr();
		switch (op.op_kind_case()) {
		case physical::PhysicalOpr_Operator::kScan: {
			if (op.scan().scan_opt() != physical::Scan::VERTEX) return false;
			if (op.scan().has_idx_predicate()) {
				const auto& pred = op.scan().idx_predicate();
				if (pred.or_predicates_size() != 1) return false;
				if (pred.or_predicates(0).predicates_size() != 1) return false;
				const auto& triplet = pred.or_predicates(0).predicates(0);
				if (triplet.value_case() !=
				        algebra::IndexPredicate_Triplet::kConst &&
				    triplet.value_case() !=
				        algebra::IndexPredicate_Triplet::kParam)
					return false;
				if (triplet.cmp() != common::Logical::EQ &&
				    triplet.cmp() != common::Logical::WITHIN)
					return false;
			}
			if (op.scan().has_params() && op.scan().params().has_predicate()) {
				if (HasUnsupportedExpression(op.scan().params().predicate()))
					return false;
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kSelect: {
			if (HasUnsupportedExpression(op.select().predicate()))
				return false;
			break;
		}
		case physical::PhysicalOpr_Operator::kProject: {
			for (int j = 0; j < op.project().mappings_size(); ++j) {
				if (HasUnsupportedExpression(op.project().mappings(j).expr()))
					return false;
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kEdge: {
			const auto& edge = op.edge();
			if (edge.expand_opt() ==
			    physical::EdgeExpand_ExpandOpt_EDGE) {
				if (i + 1 >= plan.plan_size()) return false;
				if (!plan.plan(i + 1).opr().has_vertex()) return false;
				i++;
			} else if (edge.expand_opt() !=
			           physical::EdgeExpand_ExpandOpt_VERTEX) {
				return false;
			}
			if (edge.has_params() && edge.params().has_predicate()) {
				if (HasUnsupportedExpression(edge.params().predicate()))
					return false;
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kVertex: {
			const auto& v = op.vertex();
			if (v.has_params() && v.params().has_predicate()) {
				if (HasUnsupportedExpression(v.params().predicate()))
					return false;
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kJoin: {
			const auto& join = op.join();
			if (!CanVectorize(join.left_plan())) return false;
			if (!CanVectorize(join.right_plan())) return false;
			break;
		}
		case physical::PhysicalOpr_Operator::kGroupBy:
			break;
		case physical::PhysicalOpr_Operator::kIntersect: {
			const auto& inter = op.intersect();
			for (int j = 0; j < inter.sub_plans_size(); j++) {
				const auto& sp = inter.sub_plans(j);
				if (sp.plan_size() < 1) return false;
				if (!sp.plan(0).opr().has_edge()) return false;
				const auto& edge = sp.plan(0).opr().edge();
				if (edge.has_params() && edge.params().has_predicate())
					return false;
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kDedup:
			break;
		case physical::PhysicalOpr_Operator::kUnfold:
			break;
		case physical::PhysicalOpr_Operator::kPath: {
			const auto& path = op.path();
			if (path.path_opt() ==
			        physical::PathExpand_PathOpt_ANY_SHORTEST ||
			    path.path_opt() ==
			        physical::PathExpand_PathOpt_ALL_SHORTEST) {
				if (i + 1 >= plan.plan_size()) return false;
				const auto& next_op = plan.plan(i + 1).opr();
				if (!next_op.has_vertex()) return false;
				if (next_op.vertex().opt() != physical::GetV::OTHER)
					return false;
				if (path.has_condition()) return false;
				if (path.is_optional()) return false;
				i++;
				break;
			}
			if (path.result_opt() !=
			    physical::PathExpand_ResultOpt_END_V)
				return false;
			if (path.path_opt() !=
			    physical::PathExpand_PathOpt_ARBITRARY)
				return false;
			if (path.has_condition()) return false;
			if (path.is_optional()) return false;
			break;
		}
		case physical::PhysicalOpr_Operator::kUnion: {
			const auto& u = op.union_();
			for (int j = 0; j < u.sub_plans_size(); j++) {
				if (!CanVectorize(u.sub_plans(j))) return false;
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kOrderBy: {
			const auto& ob = op.order_by();
			for (int j = 0; j < ob.pairs_size(); j++) {
				const auto& pair = ob.pairs(j);
				if (!pair.has_key()) return false;
				if (pair.key().has_property()) return false;
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kLimit:
			break;
		case physical::PhysicalOpr_Operator::kSink:
		case physical::PhysicalOpr_Operator::kRoot:
			break;
		default:
			return false;
		}
	}
	return true;
}

VecOutputInfo VecPipelineCompiler::GetOutputInfo() const {
	return {output_tags_, output_types_};
}

// ============================================================
// Proto Value → execution::Value
// ============================================================

static execution::Value ConvertProtoValue(const ::common::Value& v) {
	switch (v.item_case()) {
	case ::common::Value::kBoolean:
		return execution::Value::BOOLEAN(v.boolean());
	case ::common::Value::kI32:
		return execution::Value::INT32(v.i32());
	case ::common::Value::kI64:
		return execution::Value::INT64(v.i64());
	case ::common::Value::kF64:
		return execution::Value::DOUBLE(v.f64());
	case ::common::Value::kF32:
		return execution::Value::FLOAT(v.f32());
	case ::common::Value::kStr:
		return execution::Value::STRING(v.str());
	case ::common::Value::kU32:
		return execution::Value::UINT32(v.u32());
	case ::common::Value::kU64:
		return execution::Value::UINT64(v.u64());
	case ::common::Value::kNone:
		return execution::Value(DataType::SQLNULL);
	default:
		throw std::runtime_error("Unsupported proto value type: " +
		                         std::to_string(static_cast<int>(v.item_case())));
	}
}


// ============================================================
// Shunting-yard: Expression compilation
// ============================================================

static int get_priority(const ::common::ExprOpr& opr) {
	switch (opr.item_case()) {
	case ::common::ExprOpr::kBrace:
		return 17;
	case ::common::ExprOpr::kExtract:
		return 2;
	case ::common::ExprOpr::kLogical: {
		switch (opr.logical()) {
		case ::common::Logical::AND:
			return 11;
		case ::common::Logical::OR:
			return 12;
		case ::common::Logical::NOT:
		case ::common::Logical::WITHIN:
		case ::common::Logical::WITHOUT:
		case ::common::Logical::ISNULL:
			return 2;
		case ::common::Logical::EQ:
		case ::common::Logical::NE:
			return 7;
		case ::common::Logical::GE:
		case ::common::Logical::GT:
		case ::common::Logical::LT:
		case ::common::Logical::LE:
			return 6;
		case ::common::Logical::STARTSWITH:
		case ::common::Logical::ENDSWITH:
			return 6;
		default:
			return 16;
		}
	}
	case ::common::ExprOpr::kArith: {
		switch (opr.arith()) {
		case ::common::Arithmetic::ADD:
		case ::common::Arithmetic::SUB:
			return 4;
		case ::common::Arithmetic::MUL:
		case ::common::Arithmetic::DIV:
		case ::common::Arithmetic::MOD:
			return 3;
		default:
			return 16;
		}
	}
	default:
		return 16;
	}
}

static CompareOp LogicalToCompareOp(::common::Logical l) {
	switch (l) {
	case ::common::Logical::EQ:
		return CompareOp::kEq;
	case ::common::Logical::NE:
		return CompareOp::kNe;
	case ::common::Logical::LT:
		return CompareOp::kLt;
	case ::common::Logical::LE:
		return CompareOp::kLe;
	case ::common::Logical::GT:
		return CompareOp::kGt;
	case ::common::Logical::GE:
		return CompareOp::kGe;
	default:
		throw std::runtime_error("Not a comparison op");
	}
}

static ArithOp ProtoArithToArithOp(::common::Arithmetic a) {
	switch (a) {
	case ::common::Arithmetic::ADD:
		return ArithOp::kAdd;
	case ::common::Arithmetic::SUB:
		return ArithOp::kSub;
	case ::common::Arithmetic::MUL:
		return ArithOp::kMul;
	case ::common::Arithmetic::DIV:
		return ArithOp::kDiv;
	case ::common::Arithmetic::MOD:
		return ArithOp::kMod;
	default:
		throw std::runtime_error("Unsupported arithmetic op");
	}
}

static bool IsComparisonLogical(::common::Logical l) {
	return l == ::common::Logical::EQ || l == ::common::Logical::NE ||
	       l == ::common::Logical::LT || l == ::common::Logical::LE ||
	       l == ::common::Logical::GT || l == ::common::Logical::GE;
}

static DataType PromoteType(DataType a, DataType b) {
	if (a.id() == b.id()) return a;
	if (a.id() == DataTypeId::kDouble || b.id() == DataTypeId::kDouble)
		return DataType{DataTypeId::kDouble};
	if (a.id() == DataTypeId::kFloat || b.id() == DataTypeId::kFloat)
		return DataType{DataTypeId::kDouble};
	if (a.id() == DataTypeId::kInt64 || b.id() == DataTypeId::kInt64)
		return DataType{DataTypeId::kInt64};
	if (a.id() == DataTypeId::kUInt64 || b.id() == DataTypeId::kUInt64)
		return DataType{DataTypeId::kInt64};
	if (a.id() == DataTypeId::kInt32 || b.id() == DataTypeId::kInt32)
		return DataType{DataTypeId::kInt32};
	return a;
}

static std::unique_ptr<VecExpression> MaybeCast(
    std::unique_ptr<VecExpression> expr, DataType target) {
	if (expr->result_type().id() == target.id()) return expr;
	return std::make_unique<VecCastExpr>(std::move(expr), target);
}

std::unique_ptr<VecExpression> VecPipelineCompiler::CompileExpression(
    const ::common::Expression& expr) {
	std::stack<::common::ExprOpr> opr_stack;
	std::stack<::common::ExprOpr> opr_stack2;

	const auto& oprs = expr.operators();
	for (auto it = oprs.rbegin(); it != oprs.rend(); ++it) {
		switch (it->item_case()) {
		case ::common::ExprOpr::kBrace: {
			auto brace = it->brace();
			if (brace == ::common::ExprOpr::LEFT_BRACE) {
				while (!opr_stack.empty() &&
				       opr_stack.top().item_case() != ::common::ExprOpr::kBrace) {
					opr_stack2.push(opr_stack.top());
					opr_stack.pop();
				}
				if (!opr_stack.empty()) opr_stack.pop();
			} else {
				opr_stack.emplace(*it);
			}
			break;
		}
		case ::common::ExprOpr::kArith:
		case ::common::ExprOpr::kLogical: {
			if (it->item_case() == ::common::ExprOpr::kLogical &&
			    (it->logical() == ::common::Logical::NOT ||
			     it->logical() == ::common::Logical::ISNULL)) {
				opr_stack2.push(*it);
				break;
			}
			while (!opr_stack.empty() &&
			       get_priority(opr_stack.top()) <= get_priority(*it)) {
				opr_stack2.push(opr_stack.top());
				opr_stack.pop();
			}
			opr_stack.push(*it);
			break;
		}
		case ::common::ExprOpr::kConst:
		case ::common::ExprOpr::kVar:
		case ::common::ExprOpr::kParam:
		case ::common::ExprOpr::kCase:
		case ::common::ExprOpr::kToList:
		case ::common::ExprOpr::kScalarFunc:
		case ::common::ExprOpr::kExtract: {
			opr_stack2.push(*it);
			break;
		}
		default:
			throw std::runtime_error(
			    "Unsupported ExprOpr in vectorized compilation (case=" +
			    std::to_string(it->item_case()) + "): " +
			    it->DebugString());
		}
	}
	while (!opr_stack.empty()) {
		opr_stack2.push(opr_stack.top());
		opr_stack.pop();
	}

	// Build expression tree from postfix stack
	std::function<std::unique_ptr<VecExpression>()> build =
	    [&]() -> std::unique_ptr<VecExpression> {
		if (opr_stack2.empty()) return nullptr;
		auto opr = opr_stack2.top();
		opr_stack2.pop();

		switch (opr.item_case()) {
		case ::common::ExprOpr::kConst: {
			auto val = ConvertProtoValue(opr.const_());
			return std::make_unique<VecConstantExpr>(std::move(val));
		}
		case ::common::ExprOpr::kVar: {
			const auto& var = opr.var();
			int tag = var.has_tag() ? var.tag().id() : -1;
			if (var.has_property() && var.property().has_key()) {
				std::string prop_name = var.property().key().name();
				EnsurePropertyMaterialized(tag, prop_name);
				int prop_tag = GetPropTag(tag, prop_name);
				DataType type = var.has_node_type()
				                    ? parse_from_ir_data_type(var.node_type())
				                    : GetPropType(tag, prop_name);
				return std::make_unique<VecColumnRefExpr>(prop_tag, type);
			} else if (var.has_property() && var.property().has_len()) {
				int resolved_tag = tag >= 0 ? tag : head_tag_;
				DataTypeId col_type = DataTypeId::kInt64;
				for (size_t j = 0; j < output_tags_.size(); j++) {
					if (output_tags_[j] == resolved_tag) {
						col_type = output_types_[j].id();
						break;
					}
				}
				if (col_type == DataTypeId::kPath) {
					return std::make_unique<VecPathLengthExpr>(resolved_tag);
				}
				return std::make_unique<VecColumnRefExpr>(
				    tag, DataType{DataTypeId::kInt64});
			} else if (var.has_property() && var.property().has_id()) {
				throw std::runtime_error(
				    "ID property access not yet supported in vectorized engine");
			} else {
				// Direct tag reference (e.g., vertex column)
				DataType type = var.has_node_type()
				                    ? parse_from_ir_data_type(var.node_type())
				                    : DataType{DataTypeId::kUnknown};
				return std::make_unique<VecColumnRefExpr>(tag, type);
			}
		}
		case ::common::ExprOpr::kParam: {
			const auto& param = opr.param();
			DataType type = param.has_data_type()
			                    ? parse_from_ir_data_type(param.data_type())
			                    : DataType{DataTypeId::kInt64};
			return std::make_unique<VecParamExpr>(
			    param.name(), param.index(), type);
		}
		case ::common::ExprOpr::kLogical: {
			auto logical = opr.logical();
			if (logical == ::common::Logical::NOT) {
				auto operand = build();
				return std::make_unique<VecBooleanNotExpr>(std::move(operand));
			}
			if (logical == ::common::Logical::ISNULL) {
				auto operand = build();
				return std::make_unique<VecIsNullExpr>(std::move(operand), false);
			}
			if (IsComparisonLogical(logical)) {
				auto lhs = build();
				auto rhs = build();
				DataType promoted =
				    PromoteType(lhs->result_type(), rhs->result_type());
				lhs = MaybeCast(std::move(lhs), promoted);
				rhs = MaybeCast(std::move(rhs), promoted);
				return std::make_unique<VecComparisonExpr>(
				    std::move(lhs), LogicalToCompareOp(logical), std::move(rhs));
			}
			if (logical == ::common::Logical::AND) {
				auto lhs = build();
				auto rhs = build();
				return std::make_unique<VecBooleanAndExpr>(std::move(lhs),
				                                          std::move(rhs));
			}
			if (logical == ::common::Logical::OR) {
				auto lhs = build();
				auto rhs = build();
				return std::make_unique<VecBooleanOrExpr>(std::move(lhs),
				                                         std::move(rhs));
			}
			if (logical == ::common::Logical::WITHIN) {
				auto lhs = build();
				auto rhs = build();
				auto* const_rhs = dynamic_cast<VecConstantExpr*>(rhs.get());
				if (const_rhs &&
				    const_rhs->constant().type().id() == DataTypeId::kList) {
					const auto& list_children =
					    execution::ListValue::GetChildren(const_rhs->constant());
					std::vector<execution::Value> items(list_children.begin(),
					                                   list_children.end());
					return std::make_unique<VecInListExpr>(std::move(lhs),
					                                      std::move(items));
				}
				throw std::runtime_error(
				    "WITHIN with non-constant list not supported");
			}
			if (logical == ::common::Logical::STARTSWITH) {
				auto lhs = build();
				auto rhs = build();
				return std::make_unique<VecStringFuncExpr>(
				    std::move(lhs), std::move(rhs), StringFuncOp::kStartsWith);
			}
			if (logical == ::common::Logical::ENDSWITH) {
				auto lhs = build();
				auto rhs = build();
				return std::make_unique<VecStringFuncExpr>(
				    std::move(lhs), std::move(rhs), StringFuncOp::kEndsWith);
			}
			throw std::runtime_error("Unsupported logical op: " +
			                         std::to_string(static_cast<int>(logical)));
		}
		case ::common::ExprOpr::kArith: {
			auto lhs = build();
			auto rhs = build();
			DataType promoted =
			    PromoteType(lhs->result_type(), rhs->result_type());
			lhs = MaybeCast(std::move(lhs), promoted);
			rhs = MaybeCast(std::move(rhs), promoted);
			return std::make_unique<VecArithExpr>(
			    std::move(lhs), ProtoArithToArithOp(opr.arith()), std::move(rhs));
		}
		case ::common::ExprOpr::kCase: {
			const auto& case_opr = opr.case_();
			std::vector<VecCaseWhenExpr::WhenThen> when_thens;
			DataType result_type{DataTypeId::kUnknown};
			for (int i = 0; i < case_opr.when_then_expressions_size(); ++i) {
				auto when_expr = CompileExpression(
				    case_opr.when_then_expressions(i).when_expression());
				auto then_expr = CompileExpression(
				    case_opr.when_then_expressions(i).then_result_expression());
				if (then_expr->result_type().id() != DataTypeId::kNull &&
				    then_expr->result_type().id() != DataTypeId::kUnknown) {
					result_type = then_expr->result_type();
				}
				when_thens.emplace_back(std::move(when_expr),
				                        std::move(then_expr));
			}
			std::unique_ptr<VecExpression> else_expr = nullptr;
			if (case_opr.has_else_result_expression()) {
				else_expr =
				    CompileExpression(case_opr.else_result_expression());
				if (else_expr->result_type().id() != DataTypeId::kNull &&
				    else_expr->result_type().id() != DataTypeId::kUnknown) {
					result_type = else_expr->result_type();
				}
			}
			return std::make_unique<VecCaseWhenExpr>(
			    std::move(when_thens), std::move(else_expr), result_type);
		}
		case ::common::ExprOpr::kToList: {
			// A list of constant expressions used as IN-list argument
			const auto& list_fields = opr.to_list().fields();
			// First expression is already on the stack (the value being checked)
			// Actually in WITHIN, format is: value WITHIN list
			// Here we extract list constants
			std::vector<execution::Value> values;
			for (int i = 0; i < list_fields.size(); ++i) {
				const auto& field_expr = list_fields[i];
				if (field_expr.operators_size() > 0 &&
				    field_expr.operators(0).has_const_()) {
					values.push_back(
					    ConvertProtoValue(field_expr.operators(0).const_()));
				}
			}
			// This is the list part of a WITHIN expression
			// We return a special marker — but actually WITHIN is handled differently
			// In the proto format: [value] [WITHIN] [toList{...}]
			// The shunting-yard puts toList as an operand, then WITHIN pops lhs+rhs
			// So we need to handle WITHIN+toList together
			// Let's create a VecConstantExpr with a list value
			if (!values.empty()) {
				return std::make_unique<VecConstantExpr>(
				    execution::Value::LIST(values[0].type(), std::move(values)));
			}
			return std::make_unique<VecConstantExpr>(
			    execution::Value(DataType::SQLNULL));
		}
		case ::common::ExprOpr::kScalarFunc: {
			const auto& func = opr.scalar_func();
			const auto& name = func.unique_name();
			if (name.find("LIST_EXTRACT") != std::string::npos) {
				auto list_expr = CompileExpression(func.parameters(0));
				auto index_expr = CompileExpression(func.parameters(1));
				DataType elem_type{DataTypeId::kUnknown};
				if (list_expr->result_type().id() == DataTypeId::kList) {
					auto* info = dynamic_cast<const ListTypeInfo*>(
					    list_expr->result_type().RawExtraTypeInfo());
					if (info) elem_type = info->child_type;
				}
				if (opr.has_node_type()) {
					elem_type = parse_from_ir_data_type(opr.node_type());
				}
				return std::make_unique<VecListExtractExpr>(
				    std::move(list_expr), std::move(index_expr), elem_type);
			}
			if (name.find("CAST") != std::string::npos) {
				auto child = CompileExpression(func.parameters(0));
				DataType target{DataTypeId::kVarchar};
				if (opr.has_node_type()) {
					target = parse_from_ir_data_type(opr.node_type());
				}
				return std::make_unique<VecCastExpr>(std::move(child), target);
			}
			throw std::runtime_error(
			    "Unsupported scalar function: " + name);
		}
		case ::common::ExprOpr::kExtract: {
			auto operand = build();
			ExtractInterval interval;
			switch (opr.extract().interval()) {
			case ::common::Extract::YEAR:
				interval = ExtractInterval::kYear; break;
			case ::common::Extract::MONTH:
				interval = ExtractInterval::kMonth; break;
			case ::common::Extract::DAY:
				interval = ExtractInterval::kDay; break;
			case ::common::Extract::HOUR:
				interval = ExtractInterval::kHour; break;
			case ::common::Extract::MINUTE:
				interval = ExtractInterval::kMinute; break;
			case ::common::Extract::SECOND:
				interval = ExtractInterval::kSecond; break;
			case ::common::Extract::MILLISECOND:
				interval = ExtractInterval::kMillisecond; break;
			default:
				throw std::runtime_error("Unsupported extract interval");
			}
			return std::make_unique<VecExtractExpr>(
			    std::move(operand), interval);
		}
		default:
			throw std::runtime_error(
			    "Unsupported ExprOpr in build phase: " + opr.DebugString());
		}
	};

	auto result = build();
	return result;
}

// ============================================================
// Property management
// ============================================================

void VecPipelineCompiler::EnsurePropertyMaterialized(
    int vertex_tag, const std::string& prop_name) {
	if (vertex_tag == -1) vertex_tag = head_tag_;
	auto key = std::make_pair(vertex_tag, prop_name);
	if (prop_tag_map_.find(key) != prop_tag_map_.end()) return;

	int prop_tag = AllocatePropTag(vertex_tag, prop_name);
	operators_.push_back(std::make_unique<PropertyReadOperator>(
	    vertex_tag, prop_name, prop_tag));
}

int VecPipelineCompiler::GetPropTag(int vertex_tag,
                                    const std::string& prop_name) const {
	if (vertex_tag == -1) vertex_tag = head_tag_;
	auto key = std::make_pair(vertex_tag, prop_name);
	auto it = prop_tag_map_.find(key);
	if (it != prop_tag_map_.end()) return it->second;
	return -1;
}

int VecPipelineCompiler::AllocatePropTag(int vertex_tag,
                                         const std::string& prop_name) {
	auto key = std::make_pair(vertex_tag, prop_name);
	auto it = prop_tag_map_.find(key);
	if (it != prop_tag_map_.end()) return it->second;
	int tag = next_prop_tag_++;
	prop_tag_map_[key] = tag;
	return tag;
}

DataType VecPipelineCompiler::GetPropType(
    int vertex_tag, const std::string& prop_name) const {
	if (vertex_tag == -1) vertex_tag = head_tag_;
	auto key = std::make_pair(vertex_tag, prop_name);
	auto it = prop_type_map_.find(key);
	if (it != prop_type_map_.end()) return it->second;
	return DataType{DataTypeId::kUnknown};
}

std::vector<VecPipelineCompiler::PropRef>
VecPipelineCompiler::ExtractReferencedProperties(
    const ::common::Expression& expr) {
	std::vector<PropRef> refs;
	for (int i = 0; i < expr.operators_size(); ++i) {
		const auto& opr = expr.operators(i);
		if (opr.has_var()) {
			const auto& var = opr.var();
			int tag = var.has_tag() ? var.tag().id() : -1;
			if (var.has_property() && var.property().has_key()) {
				refs.push_back({tag, var.property().key().name()});
			}
		}
		if (opr.has_case_()) {
			const auto& case_expr = opr.case_();
			for (int j = 0; j < case_expr.when_then_expressions_size(); ++j) {
				auto when_refs = ExtractReferencedProperties(
				    case_expr.when_then_expressions(j).when_expression());
				auto then_refs = ExtractReferencedProperties(
				    case_expr.when_then_expressions(j).then_result_expression());
				refs.insert(refs.end(), when_refs.begin(), when_refs.end());
				refs.insert(refs.end(), then_refs.begin(), then_refs.end());
			}
			if (case_expr.has_else_result_expression()) {
				auto else_refs = ExtractReferencedProperties(
				    case_expr.else_result_expression());
				refs.insert(refs.end(), else_refs.begin(), else_refs.end());
			}
		}
	}
	return refs;
}

// ============================================================
// Operator compilation
// ============================================================

VecPipelineCompiler::VecPipelineCompiler(const Schema& schema,
                                         const physical::PhysicalPlan& plan)
    : schema_(schema), plan_(plan) {}

void VecPipelineCompiler::CompileScan(const physical::Scan& scan,
                                      const physical::PhysicalOpr& opr) {
	int alias = scan.has_alias() ? scan.alias().value() : 0;
	std::vector<label_t> labels;
	if (scan.has_params()) {
		for (const auto& table : scan.params().tables()) {
			labels.push_back(table.id());
		}
	}

	if (scan.has_idx_predicate()) {
		const auto& triplet =
		    scan.idx_predicate().or_predicates(0).predicates(0);

		if (triplet.value_case() == algebra::IndexPredicate_Triplet::kParam) {
			const auto& param = triplet.param();
			DataType param_type = param.has_data_type()
			    ? parse_from_ir_data_type(param.data_type())
			    : DataType{DataTypeId::kInt64};
			source_ = std::make_unique<IndexScanSource>(
			    param.name(), param_type, labels, alias);
		} else {
			const auto& val = triplet.const_();
			std::vector<Property> pks;
			if (val.item_case() == common::Value::kI32) {
				Property p;
				p.set_int32(val.i32());
				pks.push_back(p);
			} else if (val.item_case() == common::Value::kI64) {
				Property p;
				p.set_int64(val.i64());
				pks.push_back(p);
			} else if (val.item_case() == common::Value::kStr) {
				Property p;
				p.set_string_view(val.str());
				pks.push_back(p);
			} else if (val.item_case() == common::Value::kI64Array) {
				for (int j = 0; j < val.i64_array().item_size(); j++) {
					Property p;
					p.set_int64(val.i64_array().item(j));
					pks.push_back(p);
				}
			} else if (val.item_case() == common::Value::kI32Array) {
				for (int j = 0; j < val.i32_array().item_size(); j++) {
					Property p;
					p.set_int32(val.i32_array().item(j));
					pks.push_back(p);
				}
			}
			source_ = std::make_unique<IndexScanSource>(
			    std::move(pks), labels, alias);
		}
	} else {
		source_ = std::make_unique<VertexScanSource>(labels, alias);
	}

	head_tag_ = alias;

	if (scan.has_params() && scan.params().has_predicate()) {
		auto props = ExtractReferencedProperties(scan.params().predicate());
		for (auto& ref : props) {
			EnsurePropertyMaterialized(ref.vertex_tag, ref.prop_name);
		}
		auto pred = CompileExpression(scan.params().predicate());
		operators_.push_back(
		    std::make_unique<VecExprFilterOperator>(std::move(pred)));
	}
}

void VecPipelineCompiler::CompileSelect(const algebra::Select& select) {
	auto props = ExtractReferencedProperties(select.predicate());
	for (auto& ref : props) {
		EnsurePropertyMaterialized(ref.vertex_tag, ref.prop_name);
	}
	auto pred = CompileExpression(select.predicate());
	operators_.push_back(
	    std::make_unique<VecExprFilterOperator>(std::move(pred)));
}

void VecPipelineCompiler::CompileEdgeExpand(const physical::EdgeExpand& edge,
                                            const physical::PhysicalOpr& opr,
                                            int output_alias_override) {
	int v_tag = edge.has_v_tag() ? edge.v_tag().value() : head_tag_;
	int edge_alias = edge.has_alias() ? edge.alias().value() : -1;
	int output_alias = output_alias_override != -2
	    ? output_alias_override : edge_alias;
	execution::Direction dir = parse_direction(edge.direction());
	auto labels = parse_label_triplets(opr.meta_data(0));
	bool is_optional = edge.is_optional();

	std::unique_ptr<VecExpression> pred;
	std::vector<EdgePropInfo> edge_props;

	if (edge.has_params() && edge.params().has_predicate()) {
		auto refs = ExtractReferencedProperties(edge.params().predicate());
		for (auto& ref : refs) {
			bool is_edge_prop = false;
			DataType prop_type{DataTypeId::kUnknown};
			for (auto& triplet : labels) {
				auto names = schema_.get_edge_property_names(
				    triplet.src_label, triplet.dst_label,
				    triplet.edge_label);
				auto types = schema_.get_edge_properties(
				    triplet.src_label, triplet.dst_label,
				    triplet.edge_label);
				for (size_t j = 0; j < names.size(); j++) {
					if (names[j] == ref.prop_name) {
						prop_type = types[j];
						is_edge_prop = true;
						break;
					}
				}
				if (is_edge_prop) break;
			}
			if (!is_edge_prop) continue;

			int tag = ref.vertex_tag;
			if (tag == -1) tag = head_tag_;
			auto key = std::make_pair(tag, ref.prop_name);
			if (prop_tag_map_.find(key) != prop_tag_map_.end()) continue;

			int prop_tag = AllocatePropTag(tag, ref.prop_name);
			prop_type_map_[key] = prop_type;
			edge_props.push_back({ref.prop_name, prop_tag, prop_type});
		}

		pred = CompileExpression(edge.params().predicate());
	}

	operators_.push_back(std::make_unique<EdgeExpandOperator>(
	    v_tag, output_alias, std::move(labels), dir, is_optional,
	    std::move(pred), std::move(edge_props)));
	head_tag_ = output_alias;
}

void VecPipelineCompiler::CompileGetV(const physical::GetV& get_v,
                                      const physical::PhysicalOpr& opr) {
	int alias = get_v.has_alias() ? get_v.alias().value() : -1;
	if (alias != -1) {
		head_tag_ = alias;
	}

	if (get_v.has_params() && get_v.params().tables_size() > 0) {
		std::vector<label_t> allowed_labels;
		for (const auto& table : get_v.params().tables()) {
			allowed_labels.push_back(static_cast<label_t>(table.id()));
		}
		int target_tag = (alias != -1) ? alias : head_tag_;
		operators_.push_back(std::make_unique<GetVFilterOperator>(
		    target_tag, std::move(allowed_labels)));
	}

	if (get_v.has_params() && get_v.params().has_predicate()) {
		auto props = ExtractReferencedProperties(get_v.params().predicate());
		for (auto& ref : props) {
			EnsurePropertyMaterialized(ref.vertex_tag, ref.prop_name);
		}
		auto pred = CompileExpression(get_v.params().predicate());
		operators_.push_back(
		    std::make_unique<VecExprFilterOperator>(std::move(pred)));
	}
}

void VecPipelineCompiler::CompileIntersect(
    const physical::Intersect& intersect,
    const physical::PhysicalOpr& opr) {
	std::vector<IntersectSubPlan> sub_plans;
	for (int i = 0; i < intersect.sub_plans_size(); i++) {
		const auto& sp = intersect.sub_plans(i);
		const auto& edge = sp.plan(0).opr().edge();
		IntersectSubPlan plan;
		plan.v_tag = edge.has_v_tag() ? edge.v_tag().value() : head_tag_;
		plan.dir = parse_direction(edge.direction());
		plan.labels = parse_label_triplets(sp.plan(0).meta_data(0));
		sub_plans.push_back(std::move(plan));
	}
	int key = intersect.key();
	operators_.push_back(std::make_unique<IntersectOperator>(
	    std::move(sub_plans), key));
	head_tag_ = key;
}

void VecPipelineCompiler::CompileJoin(const physical::Join& join,
                                      const physical::PhysicalOpr& opr) {
	bool right_has_source = SubPlanHasSource(join.right_plan());
	bool left_has_source = SubPlanHasSource(join.left_plan());
	bool needs_materialize = !right_has_source || !left_has_source;

	// When sub-plans reference outer variables (no Scan), we materialize
	// the current pipeline's output so sub-plans can read from it.
	size_t mat_stage_idx = 0;
	if (needs_materialize) {
		RowLayout mat_layout;
		mat_layout.Initialize(output_tags_, output_types_);
		FlushCurrentStage(
		    std::make_unique<MaterializeSink>(std::move(mat_layout)));
		mat_stage_idx = stages_.size() - 1;
	}

	// 1. Compile right (build) side
	VecPipelineCompiler build_compiler(schema_, join.right_plan());
	auto build_pipeline = build_compiler.Compile();

	auto build_output = build_compiler.GetOutputInfo();
	std::vector<int> build_tags = build_output.tags;
	std::vector<DataType> build_types = build_output.types;

	if (build_tags.empty()) {
		for (int i = 0; i < join.right_plan().plan_size(); i++) {
			const auto& op = join.right_plan().plan(i).opr();
			if (op.has_scan() && op.scan().has_alias()) {
				build_tags.push_back(op.scan().alias().value());
				build_types.push_back(DataType(DataTypeId::kVertex));
			}
			if (op.has_edge() && op.edge().has_alias()) {
				build_tags.push_back(op.edge().alias().value());
				build_types.push_back(DataType(DataTypeId::kVertex));
			}
			if (op.has_vertex() && op.vertex().has_alias()) {
				build_tags.push_back(op.vertex().alias().value());
				build_types.push_back(DataType(DataTypeId::kVertex));
			}
		}
	}

	RowLayout layout;
	layout.Initialize(build_tags, build_types);

	std::vector<int> build_key_col_indices;
	for (int i = 0; i < join.right_keys_size(); i++) {
		int key_tag = join.right_keys(i).tag().id();
		int col_idx = layout.FindColumnByTag(key_tag);
		if (col_idx >= 0) {
			build_key_col_indices.push_back(col_idx);
		}
	}

	// Replace last stage's sink with HashJoinBuildSink
	build_pipeline.stages.back().sink =
	    std::make_unique<HashJoinBuildSink>(layout, build_key_col_indices);

	if (!right_has_source) {
		// Wire MaterializeSource as the build stage's source
		CompiledVecPipeline::StageLink mat_link;
		mat_link.factory = [mat_stage_idx](
		    CompiledVecPipeline::StageLink::SinkStates& states)
		    -> std::unique_ptr<IVecSource> {
			auto& ms = static_cast<MaterializeSink::State&>(
			    *states[mat_stage_idx]);
			return std::make_unique<MaterializeSource>(
			    &ms.data, ms.total_rows);
		};
		stage_links_.push_back(std::move(mat_link));
	}

	// Merge build stages into our pipeline
	MergeSubPipeline(std::move(build_pipeline));

	// 2. Compile left (probe) side
	VecPipelineCompiler left_compiler(schema_, join.left_plan());
	auto left_pipeline = left_compiler.Compile();

	// 3. Determine probe key tags and build output columns
	std::vector<int> probe_key_tags;
	for (int i = 0; i < join.left_keys_size(); i++) {
		probe_key_tags.push_back(join.left_keys(i).tag().id());
	}

	execution::JoinKind jk = parse_join_kind(join.join_kind());
	std::vector<int> build_output_col_indices;
	if (jk == execution::JoinKind::kInnerJoin ||
	    jk == execution::JoinKind::kLeftOuterJoin ||
	    jk == execution::JoinKind::kTimesJoin) {
		for (size_t i = 0; i < layout.columns.size(); i++) {
			bool is_key = false;
			for (int ki : build_key_col_indices) {
				if (ki == static_cast<int>(i)) {
					is_key = true;
					break;
				}
			}
			if (!is_key) {
				build_output_col_indices.push_back(static_cast<int>(i));
			}
		}
	}

	// 4. Create probe operator (late-bound, hash table set at runtime)
	auto probe_op = std::make_unique<HashJoinProbeOperator>(
	    nullptr, nullptr, jk,
	    std::move(probe_key_tags), std::move(build_output_col_indices));
	auto* probe_ptr = probe_op.get();

	// Append probe operator to left pipeline's last stage
	left_pipeline.stages.back().operators.push_back(std::move(probe_op));

	// 5. Add stage link: bind build hash table to probe operator at runtime
	// For source-less left plan, also provide MaterializeSource
	CompiledVecPipeline::StageLink link;
	size_t build_state_offset = stages_.size() - 1;
	if (!left_has_source) {
		link.factory = [mat_stage_idx](
		    CompiledVecPipeline::StageLink::SinkStates& states)
		    -> std::unique_ptr<IVecSource> {
			auto& ms = static_cast<MaterializeSink::State&>(
			    *states[mat_stage_idx]);
			return std::make_unique<MaterializeSource>(
			    &ms.data, ms.total_rows);
		};
	}
	link.bind_callback = [probe_ptr, build_state_offset](
	    CompiledVecPipeline::StageLink::SinkStates& states) {
		auto& state = static_cast<HashJoinBuildSink::State&>(
		    *states[build_state_offset]);
		probe_ptr->BindBuildState(&state.hash_table, &state.tuples);
	};
	stage_links_.push_back(std::move(link));

	// Merge left pipeline stages
	MergeSubPipeline(std::move(left_pipeline));

	// 6. Pop the last merged stage back into working state so
	// subsequent operators (Project, OrderBy, etc.) extend it naturally
	source_ = std::move(stages_.back().source);
	operators_ = std::move(stages_.back().operators);
	stages_.pop_back();

	if (left_has_source) {
		head_tag_ = left_compiler.head_tag_;
	}
}

void VecPipelineCompiler::CompileGroupBy(const physical::GroupBy& group_by,
                                         const physical::PhysicalOpr& opr) {
	// 1. Parse key mappings
	std::vector<int> key_tags;
	std::vector<int> key_aliases;
	std::vector<DataType> key_types;

	for (int i = 0; i < group_by.mappings_size(); i++) {
		const auto& mapping = group_by.mappings(i);
		int tag = mapping.key().has_tag() ? mapping.key().tag().id() : -1;
		int alias = mapping.has_alias() ? mapping.alias().value() : tag;

		// Determine type: if it's a property, materialize it
		DataType type{DataTypeId::kVertex};
		if (mapping.key().has_property() && mapping.key().property().has_key()) {
			std::string prop_name = mapping.key().property().key().name();
			EnsurePropertyMaterialized(tag, prop_name);
			int prop_tag = GetPropTag(tag, prop_name);
			type = GetPropType(tag, prop_name);
			tag = prop_tag;
		} else if (mapping.key().has_node_type()) {
			type = parse_from_ir_data_type(mapping.key().node_type());
		}

		key_tags.push_back(tag);
		key_aliases.push_back(alias);
		key_types.push_back(type);
	}

	// 2. Parse aggregate functions
	std::vector<AggFuncDef> agg_funcs;
	for (int i = 0; i < group_by.functions_size(); i++) {
		const auto& func = group_by.functions(i);
		AggrKind kind = parse_aggregate(func.aggregate());
		int alias = func.has_alias() ? func.alias().value() : -1;

		int input_tag = -1;
		DataType input_type{DataTypeId::kVertex};

		if (func.vars_size() > 0) {
			const auto& var = func.vars(0);
			input_tag = var.has_tag() ? var.tag().id() : -1;

			if (var.has_property() && var.property().has_key()) {
				std::string prop_name = var.property().key().name();
				EnsurePropertyMaterialized(input_tag, prop_name);
				input_tag = GetPropTag(input_tag, prop_name);
				input_type = GetPropType(
				    var.has_tag() ? var.tag().id() : -1, prop_name);
			} else if (var.has_node_type()) {
				input_type = parse_from_ir_data_type(var.node_type());
			}
		}

		agg_funcs.push_back({kind, input_tag, input_type, alias});
	}

	// 3. Build RowLayout for keys
	RowLayout key_layout;
	if (!key_tags.empty()) {
		key_layout.Initialize(key_tags, key_types);
	}

	// 4. Create aggregate sink and flush current stage
	auto agg_sink = std::make_unique<HashAggregateSink>(
	    key_layout, key_tags, agg_funcs);
	FlushCurrentStage(std::move(agg_sink));

	// 5. Add stage link: create HashAggregateSource from completed sink
	CompiledVecPipeline::StageLink link;
	link.factory = [](CompiledVecPipeline::StageLink::SinkStates& states) -> std::unique_ptr<IVecSource> {
		auto& state = static_cast<HashAggregateSink::State&>(*states.back());
		return std::make_unique<HashAggregateSource>(&state.hash_table);
	};
	stage_links_.push_back(std::move(link));

	// 6. Start fresh — source will be created at runtime by stage link
	source_ = nullptr;
	operators_.clear();

	// 7. Update output info
	output_tags_.clear();
	output_types_.clear();
	for (size_t i = 0; i < key_aliases.size(); i++) {
		output_tags_.push_back(key_aliases[i]);
		output_types_.push_back(key_types[i]);
	}
	for (auto& func : agg_funcs) {
		output_tags_.push_back(func.output_alias);
		switch (func.kind) {
		case AggrKind::kCount:
		case AggrKind::kCountDistinct:
			output_types_.push_back(DataType{DataTypeId::kInt64});
			break;
		case AggrKind::kSum:
		case AggrKind::kAvg:
			output_types_.push_back(DataType{DataTypeId::kDouble});
			break;
		case AggrKind::kMin:
		case AggrKind::kMax:
		case AggrKind::kFirst:
			output_types_.push_back(func.input_type);
			break;
		case AggrKind::kToList:
		case AggrKind::kToSet: {
			DataType child_type = func.input_type;
			if (child_type.id() != DataTypeId::kVarchar &&
			    child_type.id() != DataTypeId::kDouble &&
			    child_type.id() != DataTypeId::kFloat) {
				child_type = DataType{DataTypeId::kInt64};
			}
			output_types_.push_back(DataType(
			    DataTypeId::kList,
			    std::make_shared<ListTypeInfo>(child_type)));
			break;
		}
		default:
			output_types_.push_back(DataType{DataTypeId::kInt64});
			break;
		}
	}

}

void VecPipelineCompiler::CompileDedup(const algebra::Dedup& dedup,
                                       const physical::PhysicalOpr& opr) {
	std::vector<int> key_tags;
	std::vector<DataType> key_types;
	for (int i = 0; i < dedup.keys_size(); i++) {
		const auto& key = dedup.keys(i);
		int tag = key.has_tag() ? key.tag().id() : -1;
		key_tags.push_back(tag);

		DataType dt{DataTypeId::kInt64};
		for (size_t j = 0; j < output_tags_.size(); j++) {
			if (output_tags_[j] == tag) {
				dt = output_types_[j];
				break;
			}
		}
		key_types.push_back(dt);
	}
	operators_.push_back(
	    std::make_unique<DedupOperator>(std::move(key_tags), std::move(key_types)));
}

void VecPipelineCompiler::CompileUnion(const physical::Union& union_op,
                                       const physical::PhysicalOpr& opr) {
	// Check if any sub-plan lacks a source (references outer variables)
	bool any_sourceless = false;
	for (int i = 0; i < union_op.sub_plans_size(); i++) {
		if (!SubPlanHasSource(union_op.sub_plans(i))) {
			any_sourceless = true;
			break;
		}
	}

	// If any sub-plan needs outer data, materialize current pipeline
	size_t mat_stage_idx = 0;
	if (any_sourceless) {
		RowLayout mat_layout;
		mat_layout.Initialize(output_tags_, output_types_);
		FlushCurrentStage(
		    std::make_unique<MaterializeSink>(std::move(mat_layout)));
		mat_stage_idx = stages_.size() - 1;
	}

	std::vector<std::unique_ptr<UnionSubPipeline>> sub_pipelines;
	std::vector<MaterializeSource*> unbound_sources;

	for (int i = 0; i < union_op.sub_plans_size(); i++) {
		auto compiler = std::make_unique<VecPipelineCompiler>(
		    schema_, union_op.sub_plans(i));
		auto sub_compiled = compiler->Compile();

		auto sp = std::make_unique<UnionSubPipeline>();

		if (sub_compiled.stages.size() > 1) {
			// Multi-stage sub-plan (e.g., has OrderBy/GroupBy).
			// If first stage has no source, inject an unbound MaterializeSource.
			if (!sub_compiled.stages[0].source) {
				auto mat_src = std::make_unique<MaterializeSource>();
				unbound_sources.push_back(mat_src.get());
				sub_compiled.stages[0].source = std::move(mat_src);
			}
			sp->full_pipeline =
			    std::make_unique<CompiledVecPipeline>(std::move(sub_compiled));
		} else {
			if (sub_compiled.stages[0].source) {
				sp->source = std::move(sub_compiled.stages[0].source);
			} else {
				auto mat_src = std::make_unique<MaterializeSource>();
				unbound_sources.push_back(mat_src.get());
				sp->source = std::move(mat_src);
			}
			sp->operators = std::move(sub_compiled.stages[0].operators);
		}
		sub_pipelines.push_back(std::move(sp));

		if (i == 0) {
			auto info = compiler->GetOutputInfo();
			output_tags_ = info.tags;
			output_types_ = info.types;
		}

		union_sub_compilers_.push_back(std::move(compiler));
	}

	source_ = std::make_unique<UnionSource>(std::move(sub_pipelines));
	operators_.clear();

	// Wire MaterializeSources to the materialize state at runtime
	if (any_sourceless && !unbound_sources.empty()) {
		CompiledVecPipeline::StageLink link;
		link.bind_callback = [mat_stage_idx, unbound_sources](
		    CompiledVecPipeline::StageLink::SinkStates& states) {
			auto& ms = static_cast<MaterializeSink::State&>(
			    *states[mat_stage_idx]);
			for (auto* src : unbound_sources) {
				src->Bind(&ms.data, ms.total_rows);
			}
		};
		stage_links_.push_back(std::move(link));
	}
}

void VecPipelineCompiler::CompileUnfold(const physical::Unfold& unfold,
                                        const physical::PhysicalOpr& opr) {
	int alias = unfold.alias().value();
	const auto& input_expr = unfold.input_expr();

	// Determine input tag (must be a simple var reference to a list column)
	int input_tag = -1;
	if (input_expr.operators_size() == 1 && input_expr.operators(0).has_var() &&
	    !input_expr.operators(0).var().has_property()) {
		input_tag = input_expr.operators(0).var().tag().id();
	} else if (unfold.has_tag()) {
		input_tag = unfold.tag().value();
	}

	// Look up the list column type to get element type
	DataType elem_type{DataTypeId::kInt64};
	for (size_t i = 0; i < output_tags_.size(); i++) {
		if (output_tags_[i] == input_tag) {
			if (output_types_[i].id() == DataTypeId::kList) {
				elem_type = ListType::GetChildType(output_types_[i]);
			} else {
				elem_type = output_types_[i];
			}
			break;
		}
	}

	operators_.push_back(
	    std::make_unique<UnfoldOperator>(input_tag, alias, elem_type));

	// Update output: remove the list column, add the unfolded element column
	std::vector<int> new_tags;
	std::vector<DataType> new_types;
	for (size_t i = 0; i < output_tags_.size(); i++) {
		if (output_tags_[i] != input_tag) {
			new_tags.push_back(output_tags_[i]);
			new_types.push_back(output_types_[i]);
		}
	}
	new_tags.push_back(alias);
	new_types.push_back(elem_type);
	output_tags_ = std::move(new_tags);
	output_types_ = std::move(new_types);
}

void VecPipelineCompiler::CompilePathExpand(
    const physical::PathExpand& path, const physical::PhysicalOpr& opr,
    int plan_idx) {
	if (path.path_opt() ==
	    physical::PathExpand_PathOpt_ANY_SHORTEST) {
		CompileShortestPath(path, opr, plan_idx);
		return;
	}
	if (path.path_opt() ==
	    physical::PathExpand_PathOpt_ALL_SHORTEST) {
		CompileAllShortestPath(path, opr, plan_idx);
		return;
	}

	int start_tag = path.has_start_tag() ? path.start_tag().value() : head_tag_;
	auto labels = parse_label_triplets(opr.meta_data(0));
	execution::Direction dir =
	    parse_direction(path.base().edge_expand().direction());
	int hop_lower = path.hop_range().lower();
	int hop_upper = path.hop_range().upper();

	// The following GetV provides the output alias
	int alias = -1;
	if (plan_idx + 1 < plan_.plan_size()) {
		const auto& next = plan_.plan(plan_idx + 1).opr();
		if (next.has_vertex() && next.vertex().has_alias()) {
			alias = next.vertex().alias().value();
		}
	}

	operators_.push_back(std::make_unique<PathExpandOperator>(
	    start_tag, alias, std::move(labels), dir, hop_lower, hop_upper));

	output_tags_.push_back(alias);
	output_types_.push_back(DataType(DataTypeId::kVertex));
	head_tag_ = alias;
}

void VecPipelineCompiler::CompileShortestPath(
    const physical::PathExpand& path, const physical::PhysicalOpr& opr,
    int plan_idx) {
	int start_tag = path.has_start_tag() ? path.start_tag().value() : head_tag_;
	auto labels = parse_label_triplets(opr.meta_data(0));
	assert(!labels.empty());
	int hop_lower = path.hop_range().lower();
	int hop_upper = path.hop_range().upper();
	int path_alias = path.has_alias() ? path.alias().value() : -1;

	const auto& get_v = plan_.plan(plan_idx + 1).opr().vertex();
	int v_alias = get_v.has_alias() ? get_v.alias().value() : -1;

	std::string dest_param_name;
	int64_t dest_const = -1;
	bool use_param = true;

	if (get_v.has_params() && get_v.params().has_predicate()) {
		const auto& pred = get_v.params().predicate();
		if (pred.operators_size() >= 3) {
			const auto& val_opr = pred.operators(2);
			if (val_opr.has_param()) {
				dest_param_name = val_opr.param().name();
				use_param = true;
			} else if (val_opr.has_const_()) {
				dest_const = val_opr.const_().i64();
				use_param = false;
			}
		}
	}

	operators_.push_back(std::make_unique<ShortestPathOperator>(
	    start_tag, v_alias, path_alias, labels[0], hop_lower, hop_upper,
	    std::move(dest_param_name), dest_const, use_param));

	output_tags_.push_back(v_alias);
	output_types_.push_back(DataType(DataTypeId::kVertex));
	if (path_alias != -1) {
		output_tags_.push_back(path_alias);
		output_types_.push_back(DataType(DataTypeId::kInt64));
	}
	head_tag_ = v_alias;
}

void VecPipelineCompiler::CompileAllShortestPath(
    const physical::PathExpand& path, const physical::PhysicalOpr& opr,
    int plan_idx) {
	int start_tag = path.has_start_tag() ? path.start_tag().value() : head_tag_;
	auto labels = parse_label_triplets(opr.meta_data(0));
	assert(!labels.empty());
	int hop_lower = path.hop_range().lower();
	int hop_upper = path.hop_range().upper();
	int path_alias = path.has_alias() ? path.alias().value() : -1;

	const auto& get_v = plan_.plan(plan_idx + 1).opr().vertex();
	int v_alias = get_v.has_alias() ? get_v.alias().value() : -1;

	std::string dest_param_name;
	int64_t dest_const = -1;
	bool use_param = true;

	if (get_v.has_params() && get_v.params().has_predicate()) {
		const auto& pred = get_v.params().predicate();
		if (pred.operators_size() >= 3) {
			const auto& val_opr = pred.operators(2);
			if (val_opr.has_param()) {
				dest_param_name = val_opr.param().name();
				use_param = true;
			} else if (val_opr.has_const_()) {
				dest_const = val_opr.const_().i64();
				use_param = false;
			}
		}
	}

	operators_.push_back(std::make_unique<AllShortestPathOperator>(
	    start_tag, v_alias, path_alias, labels[0], hop_lower, hop_upper,
	    std::move(dest_param_name), dest_const, use_param));

	output_tags_.push_back(v_alias);
	output_types_.push_back(DataType(DataTypeId::kVertex));
	if (path_alias != -1) {
		output_tags_.push_back(path_alias);
		output_types_.push_back(DataType(DataTypeId::kPath));
	}
	head_tag_ = v_alias;
}

void VecPipelineCompiler::CompileOrderBy(const algebra::OrderBy& order_by,
                                         const physical::PhysicalOpr& opr) {
	// 1. Build RowLayout from all current output columns
	RowLayout layout;
	layout.Initialize(output_tags_, output_types_);

	// 2. Parse ordering pairs → SortKeyDefs
	std::vector<SortKeyDef> keys;
	for (int i = 0; i < order_by.pairs_size(); i++) {
		const auto& pair = order_by.pairs(i);
		int tag = pair.key().tag().id();

		int col_idx = layout.FindColumnByTag(tag);
		if (col_idx < 0) continue;

		DataTypeId type = layout.columns[col_idx].type.id();
		bool ascending =
		    (pair.order() == algebra::OrderBy_OrderingPair_Order_ASC);
		keys.push_back({col_idx, type, ascending});
	}

	// 3. Determine if Top-N
	bool is_topn = order_by.has_limit() && order_by.limit().upper() > 0;
	size_t limit_val = is_topn ? static_cast<size_t>(order_by.limit().upper())
	                           : 0;

	// 4. Create sort sink and flush current stage
	std::unique_ptr<IVecSink> sort_sink;
	if (is_topn) {
		sort_sink = std::make_unique<TopNSink>(layout, keys, limit_val);
	} else {
		sort_sink = std::make_unique<SortSink>(layout, keys);
	}
	FlushCurrentStage(std::move(sort_sink));

	// 5. Add stage link: create SortSource from completed sink state
	CompiledVecPipeline::StageLink link;
	link.factory = [is_topn](CompiledVecPipeline::StageLink::SinkStates& states)
	    -> std::unique_ptr<IVecSource> {
		if (is_topn) {
			auto& state = static_cast<TopNSink::State&>(*states.back());
			return std::make_unique<SortSource>(&state.data, &state.heap);
		} else {
			auto& state = static_cast<SortSink::State&>(*states.back());
			return std::make_unique<SortSource>(
			    &state.data, &state.sorted_indices);
		}
	};
	stage_links_.push_back(std::move(link));

	// 6. Start fresh — source will be created at runtime by stage link
	source_ = nullptr;
	operators_.clear();
}

void VecPipelineCompiler::CompileLimit(const algebra::Limit& limit,
                                       const physical::PhysicalOpr& opr) {
	size_t offset = 0;
	size_t count = 0;
	if (limit.has_range()) {
		offset = static_cast<size_t>(limit.range().lower());
		count = static_cast<size_t>(limit.range().upper());
	}
	if (count > 0) {
		operators_.push_back(
		    std::make_unique<LimitOperator>(offset, count));
	}
}

void VecPipelineCompiler::CompileProject(const physical::Project& project,
                                         const physical::PhysicalOpr& opr) {
	std::vector<ExprProjection> projections;
	std::vector<int> proj_tags;
	std::vector<DataType> proj_types;
	bool has_expr = false;

	for (int i = 0; i < project.mappings_size(); ++i) {
		const auto& mapping = project.mappings(i);
		int alias = mapping.has_alias() ? mapping.alias().value() : -1;
		const auto& expr = mapping.expr();

		// Materialize all referenced properties
		auto props = ExtractReferencedProperties(expr);
		for (auto& ref : props) {
			EnsurePropertyMaterialized(ref.vertex_tag, ref.prop_name);
		}

		if (expr.operators_size() == 1 && expr.operators(0).has_var() &&
		    !(expr.operators(0).var().has_property() &&
		      expr.operators(0).var().property().has_len())) {
			// Simple variable reference (not length() which needs expression eval)
			const auto& var = expr.operators(0).var();
			int tag = var.has_tag() ? var.tag().id() : -1;
			DataType type{DataTypeId::kUnknown};
			if (var.has_property() && var.property().has_key()) {
				std::string prop_name = var.property().key().name();
				int prop_tag = GetPropTag(tag, prop_name);
				type = var.has_node_type()
				           ? parse_from_ir_data_type(var.node_type())
				           : GetPropType(tag, prop_name);
				tag = prop_tag;
			} else if (var.has_node_type()) {
				type = parse_from_ir_data_type(var.node_type());
			}
			int dst = alias >= 0 ? alias : tag;
			projections.push_back({nullptr, tag, dst, type});
			proj_tags.push_back(dst);
			proj_types.push_back(type);
		} else {
			// Complex expression
			auto compiled = CompileExpression(expr);
			DataType type = compiled->result_type();
			projections.push_back({std::move(compiled), -1, alias, type});
			proj_tags.push_back(alias);
			proj_types.push_back(type);
			has_expr = true;
		}
	}

	if (projections.empty()) return;

	if (!has_expr) {
		// All simple refs — use lightweight ProjectOperator
		std::vector<int> src_tags, dst_tags;
		for (auto& p : projections) {
			src_tags.push_back(p.src_tag);
			dst_tags.push_back(p.dst_tag);
		}
		if (!project.is_append()) {
			output_tags_ = proj_tags;
			output_types_ = proj_types;
			operators_.push_back(std::make_unique<ProjectOperator>(
			    std::move(src_tags), std::move(dst_tags)));
		}
	} else {
		// Has expressions — use ExprProjectOperator
		operators_.push_back(std::make_unique<ExprProjectOperator>(
		    std::move(projections), project.is_append()));
		if (!project.is_append()) {
			output_tags_ = proj_tags;
			output_types_ = proj_types;
		} else {
			for (size_t i = 0; i < proj_tags.size(); i++) {
				output_tags_.push_back(proj_tags[i]);
				output_types_.push_back(proj_types[i]);
			}
		}
	}
}

void VecPipelineCompiler::CompileSink(const physical::Sink& sink) {
	if (sink.tags_size() > 0) {
		output_tags_.clear();
		output_types_.clear();
		for (int i = 0; i < sink.tags_size(); ++i) {
			const auto& opt_tag = sink.tags(i);
			int tag = opt_tag.has_tag() ? opt_tag.tag().value() : -1;
			output_tags_.push_back(tag);
			auto key_it = std::find_if(
			    prop_tag_map_.begin(), prop_tag_map_.end(),
			    [tag](const auto& p) { return p.second == tag; });
			if (key_it != prop_tag_map_.end()) {
				auto type_it = prop_type_map_.find(key_it->first);
				if (type_it != prop_type_map_.end()) {
					output_types_.push_back(type_it->second);
				} else {
					output_types_.push_back(DataType{DataTypeId::kUnknown});
				}
			} else {
				output_types_.push_back(DataType{DataTypeId::kUnknown});
			}
		}
	}
	// If sink.tags is empty, output_tags_/output_types_ are set by CompileProject
}

void VecPipelineCompiler::CompileTCFuse(int start_idx) {
	const auto& ee_opr0 = plan_.plan(start_idx).opr().edge();
	const auto& ee_opr1 = plan_.plan(start_idx + 5).opr().edge();
	const auto& ee_opr2 = plan_.plan(start_idx + 6).opr().edge();

	int input_tag = ee_opr0.v_tag().value();
	int alias1 = ee_opr1.has_alias() ? ee_opr1.alias().value() : -1;
	int alias2 = ee_opr2.alias().value();

	auto labels0 = parse_label_triplets(plan_.plan(start_idx).meta_data(0));
	auto labels1 = parse_label_triplets(plan_.plan(start_idx + 5).meta_data(0));
	auto labels2 = parse_label_triplets(plan_.plan(start_idx + 6).meta_data(0));

	if (labels0.size() != 1 || labels1.size() != 1 || labels2.size() != 1) {
		throw std::runtime_error("TC fuse requires exactly one label triplet per edge");
	}

	auto dir0 = parse_direction(ee_opr0.direction());
	auto dir1 = parse_direction(ee_opr1.direction());
	auto dir2 = parse_direction(ee_opr2.direction());

	std::array<std::tuple<label_t, label_t, label_t, execution::Direction>, 3>
	    labels;
	if (dir0 == execution::Direction::kOut) {
		labels[0] = {labels0[0].src_label, labels0[0].dst_label,
		             labels0[0].edge_label, dir0};
	} else {
		labels[0] = {labels0[0].dst_label, labels0[0].src_label,
		             labels0[0].edge_label, dir0};
	}
	if (dir1 == execution::Direction::kOut) {
		labels[1] = {labels1[0].src_label, labels1[0].dst_label,
		             labels1[0].edge_label, dir1};
	} else {
		labels[1] = {labels1[0].dst_label, labels1[0].src_label,
		             labels1[0].edge_label, dir1};
	}
	if (dir2 == execution::Direction::kOut) {
		labels[2] = {labels2[0].src_label, labels2[0].dst_label,
		             labels2[0].edge_label, dir2};
	} else {
		labels[2] = {labels2[0].dst_label, labels2[0].src_label,
		             labels2[0].edge_label, dir2};
	}

	bool is_lt = ee_opr0.params().predicate().operators(1).logical() ==
	             common::Logical::LT;
	std::string param_name =
	    ee_opr0.params().predicate().operators(2).param().name();

	auto properties0 = schema_.get_edge_properties(
	    labels0[0].src_label, labels0[0].dst_label, labels0[0].edge_label);
	DataTypeId ep_type = DataTypeId::kEmpty;
	if (!properties0.empty()) {
		ep_type = properties0[0].id();
	}

	operators_.push_back(std::make_unique<TCFuseOperator>(
	    input_tag, alias1, alias2, labels, is_lt,
	    std::move(param_name), ep_type));
	head_tag_ = alias2;
}

void VecPipelineCompiler::FlushCurrentStage(std::unique_ptr<IVecSink> sink) {
	PipelineStage stage;
	stage.source = std::move(source_);
	stage.operators = std::move(operators_);
	stage.sink = std::move(sink);
	stages_.push_back(std::move(stage));
	source_ = nullptr;
	operators_.clear();
}

void VecPipelineCompiler::MergeSubPipeline(CompiledVecPipeline sub) {
	for (auto& stage : sub.stages) {
		stages_.push_back(std::move(stage));
	}
	for (auto& link : sub.stage_links) {
		stage_links_.push_back(std::move(link));
	}
}

CompiledVecPipeline VecPipelineCompiler::Compile() {
	int opr_num = plan_.plan_size();
	for (int i = 0; i < opr_num; ++i) {
		if (i + 7 < opr_num && tc_fusable_vec(plan_, i)) {
			CompileTCFuse(i);
			i += 7;
			continue;
		}

		const auto& phys_opr = plan_.plan(i);
		const auto& op = phys_opr.opr();

		switch (op.op_kind_case()) {
		case physical::PhysicalOpr_Operator::kScan:
			CompileScan(op.scan(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kSelect:
			CompileSelect(op.select());
			break;
		case physical::PhysicalOpr_Operator::kProject:
			CompileProject(op.project(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kEdge: {
			if (op.edge().expand_opt() ==
			    physical::EdgeExpand_ExpandOpt_EDGE) {
				const auto& get_v = plan_.plan(i + 1).opr().vertex();
				int get_v_alias = get_v.has_alias()
				    ? get_v.alias().value() : -1;
				CompileEdgeExpand(op.edge(), phys_opr, get_v_alias);
				i++;
			} else {
				CompileEdgeExpand(op.edge(), phys_opr);
			}
			break;
		}
		case physical::PhysicalOpr_Operator::kVertex:
			CompileGetV(op.vertex(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kJoin:
			CompileJoin(op.join(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kIntersect:
			CompileIntersect(op.intersect(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kGroupBy:
			CompileGroupBy(op.group_by(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kDedup:
			CompileDedup(op.dedup(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kUnion:
			CompileUnion(op.union_(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kUnfold:
			CompileUnfold(op.unfold(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kPath:
			CompilePathExpand(op.path(), phys_opr, i);
			i++;  // skip the following GetV
			break;
		case physical::PhysicalOpr_Operator::kOrderBy:
			CompileOrderBy(op.order_by(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kLimit:
			CompileLimit(op.limit(), phys_opr);
			break;
		case physical::PhysicalOpr_Operator::kSink:
			CompileSink(op.sink());
			break;
		case physical::PhysicalOpr_Operator::kRoot:
			break;
		default:
			throw std::runtime_error(
			    "Unsupported operator in vectorized pipeline: " +
			    std::to_string(static_cast<int>(op.op_kind_case())));
		}
	}

	// Flush remaining source + operators as the final stage.
	// After a stage break (GroupBy, OrderBy), we have pending stage links
	// that need a target stage. Create one even if source is null — the
	// executor will use the stage link factory to create the source.
	if (stages_.size() <= stage_links_.size()) {
		PipelineStage final_stage;
		final_stage.source = std::move(source_);
		final_stage.operators = std::move(operators_);
		final_stage.sink = std::make_unique<ResultSink>();
		stages_.push_back(std::move(final_stage));
	} else if (source_) {
		PipelineStage final_stage;
		final_stage.source = std::move(source_);
		final_stage.operators = std::move(operators_);
		final_stage.sink = std::make_unique<ResultSink>();
		stages_.push_back(std::move(final_stage));
	} else if (!operators_.empty()) {
		if (!stages_.empty()) {
			auto& last = stages_.back();
			for (auto& op : operators_) {
				last.operators.push_back(std::move(op));
			}
			operators_.clear();
		} else {
			PipelineStage stage;
			stage.operators = std::move(operators_);
			stage.sink = std::make_unique<ResultSink>();
			stages_.push_back(std::move(stage));
		}
	}

	CompiledVecPipeline result;
	result.stages = std::move(stages_);
	result.stage_links = std::move(stage_links_);
	result.output_info = {output_tags_, output_types_};

	// Validate: every stage must have a source or a preceding stage link
	// with a factory that creates one. Stage 0 with null source is allowed
	// for sub-plans that will be externally sourced (e.g., Join/Union).
	bool plan_has_scan = false;
	for (int i = 0; i < plan_.plan_size(); i++) {
		if (plan_.plan(i).opr().op_kind_case() ==
		    physical::PhysicalOpr_Operator::kScan) {
			plan_has_scan = true;
			break;
		}
	}
	if (plan_has_scan) {
		for (size_t i = 0; i < result.stages.size(); i++) {
			if (!result.stages[i].source) {
				bool has_factory =
				    (i > 0 && i - 1 < result.stage_links.size() &&
				     result.stage_links[i - 1].factory);
				if (!has_factory) {
					throw std::runtime_error(
					    "Compiled pipeline has stage without source");
				}
			}
		}
	}

	return result;
}

}  // namespace neug::execution::vec
