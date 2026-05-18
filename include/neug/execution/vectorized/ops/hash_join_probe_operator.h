#pragma once
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/join/join_hash_table.h"
#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class HashJoinProbeOperator : public IVecOperator {
 public:
	HashJoinProbeOperator(JoinHashTable* hash_table,
	                      TupleDataCollection* build_tuples,
	                      execution::JoinKind join_kind,
	                      std::vector<int> probe_key_tags,
	                      std::vector<int> build_output_col_indices);

	void BindBuildState(JoinHashTable* hash_table,
	                    TupleDataCollection* build_tuples) {
		hash_table_ = hash_table;
		build_tuples_ = build_tuples;
	}

	std::string GetName() const override { return "HashJoinProbeOperator"; }
	std::unique_ptr<OperatorState> GetOperatorState() const override;

	OperatorResultType Execute(GraphDataChunk& input, GraphDataChunk& output,
	                           OperatorState& state,
	                           const VecExecContext& ctx) override;

 private:
	OperatorResultType ExecuteInner(GraphDataChunk& input,
	                                GraphDataChunk& output,
	                                OperatorState& state);
	OperatorResultType ExecuteSemi(GraphDataChunk& input,
	                               GraphDataChunk& output,
	                               OperatorState& state);
	OperatorResultType ExecuteAnti(GraphDataChunk& input,
	                               GraphDataChunk& output,
	                               OperatorState& state);
	OperatorResultType ExecuteLeftOuter(GraphDataChunk& input,
	                                    GraphDataChunk& output,
	                                    OperatorState& state);

	JoinHashTable* hash_table_;
	TupleDataCollection* build_tuples_;
	execution::JoinKind join_kind_;
	std::vector<int> probe_key_tags_;
	std::vector<int> build_output_col_indices_;
};

}  // namespace neug::execution::vec
