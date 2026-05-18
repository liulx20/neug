#pragma once
#include <cstddef>
#include <memory>
#include <string>

#include "neug/execution/vectorized/join/tuple_data.h"
#include "neug/execution/vectorized/pipeline/operator_base.h"

namespace neug::execution::vec {

class MaterializeSource : public IVecSource {
 public:
	MaterializeSource() : data_(nullptr), total_rows_(0) {}
	MaterializeSource(TupleDataCollection* data, size_t total_rows)
	    : data_(data), total_rows_(total_rows) {}

	void Bind(TupleDataCollection* data, size_t total_rows) {
		data_ = data;
		total_rows_ = total_rows;
	}

	std::string GetName() const override { return "MaterializeSource"; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(
	    const VecExecContext& ctx) const override;
	std::unique_ptr<LocalSourceState> GetLocalSourceState(
	    GlobalSourceState& gstate) const override;

	SourceResultType GetData(GraphDataChunk& chunk, GlobalSourceState& gstate,
	                         LocalSourceState& lstate) override;

 private:
	TupleDataCollection* data_;
	size_t total_rows_;
};

}  // namespace neug::execution::vec
