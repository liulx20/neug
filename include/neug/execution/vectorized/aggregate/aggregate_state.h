#pragma once
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/common/types/graph_types.h"

namespace neug::execution::vec {

struct AggFuncDef {
	AggrKind kind;
	int input_tag;
	DataType input_type;
	int output_alias;
};

struct AggStateBase {
	virtual ~AggStateBase() = default;
};

struct CountState : AggStateBase {
	int64_t count = 0;
};

struct SumState : AggStateBase {
	double sum = 0.0;
	int64_t count = 0;
};

struct MinState : AggStateBase {
	double val_double = std::numeric_limits<double>::max();
	int64_t val_int = std::numeric_limits<int64_t>::max();
};

struct MaxState : AggStateBase {
	double val_double = std::numeric_limits<double>::lowest();
	int64_t val_int = std::numeric_limits<int64_t>::lowest();
};

struct FirstState : AggStateBase {
	bool has_first = false;
	int64_t first_int = 0;
	double first_double = 0.0;
	std::string first_string;
	vid_t first_vid = 0;
	label_t first_label = 0;
};

struct ListAccumState : AggStateBase {
	std::vector<int64_t> list_int;
	std::vector<double> list_double;
	std::vector<std::string> list_string;
};

inline std::unique_ptr<AggStateBase> CreateAggState(AggrKind kind) {
	switch (kind) {
	case AggrKind::kCount:
		return std::make_unique<CountState>();
	case AggrKind::kSum:
	case AggrKind::kAvg:
		return std::make_unique<SumState>();
	case AggrKind::kMin:
		return std::make_unique<MinState>();
	case AggrKind::kMax:
		return std::make_unique<MaxState>();
	case AggrKind::kFirst:
		return std::make_unique<FirstState>();
	case AggrKind::kCountDistinct:
	case AggrKind::kToList:
	case AggrKind::kToSet:
		return std::make_unique<ListAccumState>();
	default:
		return std::make_unique<CountState>();
	}
}

}  // namespace neug::execution::vec
