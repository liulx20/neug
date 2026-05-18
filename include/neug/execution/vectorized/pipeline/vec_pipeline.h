#pragma once
#include <vector>

#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

struct VecExecContext;

struct VecPipeline {
	IVecSource* source = nullptr;
	std::vector<IVecOperator*> operators;
	IVecSink* sink = nullptr;
	const VecExecContext* ctx = nullptr;
};

}  // namespace neug::execution::vec
