#pragma once
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/vectorized/aggregate/aggregate_hash_table.h"
#include "neug/execution/vectorized/expression/vec_expression.h"
#include "neug/execution/vectorized/join/join_hash_table.h"
#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/compiled_vec_pipeline.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

#include "neug/generated/proto/plan/physical.pb.h"

namespace neug {
class Schema;
}

namespace neug::execution::vec {

class VecPipelineCompiler {
 public:
	VecPipelineCompiler(const Schema& schema,
	                    const physical::PhysicalPlan& plan);

	CompiledVecPipeline Compile();

	static bool CanVectorize(const physical::PhysicalPlan& plan);

	VecOutputInfo GetOutputInfo() const;

 private:
	void CompileScan(const physical::Scan& scan,
	                 const physical::PhysicalOpr& opr);
	void CompileSelect(const algebra::Select& select);
	void CompileProject(const physical::Project& project,
	                    const physical::PhysicalOpr& opr);
	void CompileEdgeExpand(const physical::EdgeExpand& edge,
	                       const physical::PhysicalOpr& opr,
	                       int output_alias_override = -2);
	void CompileGetV(const physical::GetV& get_v,
	                 const physical::PhysicalOpr& opr);
	void CompileJoin(const physical::Join& join,
	                 const physical::PhysicalOpr& opr);
	void CompileGroupBy(const physical::GroupBy& group_by,
	                    const physical::PhysicalOpr& opr);
	void CompileIntersect(const physical::Intersect& intersect,
	                      const physical::PhysicalOpr& opr);
	void CompileDedup(const algebra::Dedup& dedup,
	                  const physical::PhysicalOpr& opr);
	void CompileOrderBy(const algebra::OrderBy& order_by,
	                    const physical::PhysicalOpr& opr);
	void CompileLimit(const algebra::Limit& limit,
	                  const physical::PhysicalOpr& opr);
	void CompileUnion(const physical::Union& union_op,
	                  const physical::PhysicalOpr& opr);
	void CompileUnfold(const physical::Unfold& unfold,
	                   const physical::PhysicalOpr& opr);
	void CompilePathExpand(const physical::PathExpand& path,
	                       const physical::PhysicalOpr& opr, int plan_idx);
	void CompileShortestPath(const physical::PathExpand& path,
	                         const physical::PhysicalOpr& opr, int plan_idx);
	void CompileAllShortestPath(const physical::PathExpand& path,
	                            const physical::PhysicalOpr& opr, int plan_idx);
	void CompileSink(const physical::Sink& sink);
	void CompileTCFuse(int start_idx);

	std::unique_ptr<VecExpression> CompileExpression(
	    const ::common::Expression& expr);

	void EnsurePropertyMaterialized(int vertex_tag,
	                                const std::string& prop_name);

	struct PropRef {
		int vertex_tag;
		std::string prop_name;
	};
	std::vector<PropRef> ExtractReferencedProperties(
	    const ::common::Expression& expr);

	int GetPropTag(int vertex_tag, const std::string& prop_name) const;
	int AllocatePropTag(int vertex_tag, const std::string& prop_name);
	DataType GetPropType(int vertex_tag, const std::string& prop_name) const;

	void FlushCurrentStage(std::unique_ptr<IVecSink> sink);
	void MergeSubPipeline(CompiledVecPipeline sub);

	const Schema& schema_;
	const physical::PhysicalPlan& plan_;

	std::unique_ptr<IVecSource> source_;
	std::vector<std::unique_ptr<IVecOperator>> operators_;

	std::vector<PipelineStage> stages_;
	std::vector<CompiledVecPipeline::StageLink> stage_links_;

	int head_tag_ = -1;
	int next_prop_tag_ = 1000;
	std::map<std::pair<int, std::string>, int> prop_tag_map_;
	std::map<std::pair<int, std::string>, DataType> prop_type_map_;

	std::vector<int> output_tags_;
	std::vector<DataType> output_types_;

	std::vector<std::unique_ptr<VecPipelineCompiler>> union_sub_compilers_;
};

}  // namespace neug::execution::vec
