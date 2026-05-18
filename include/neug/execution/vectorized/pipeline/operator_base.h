#pragma once
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/vectorized/core/graph_data_chunk.h"

namespace neug::execution::vec {

struct VecExecContext;

enum class SourceResultType : uint8_t { kHaveMoreOutput, kFinished };

enum class OperatorResultType : uint8_t {
	kNeedMoreInput,
	kHaveMoreOutput,
	kFinished
};

enum class SinkResultType : uint8_t { kNeedMoreInput, kFinished };

struct GlobalSourceState {
	virtual ~GlobalSourceState() = default;
};
struct LocalSourceState {
	virtual ~LocalSourceState() = default;
};
struct OperatorState {
	virtual ~OperatorState() = default;
};
struct GlobalSinkState {
	virtual ~GlobalSinkState() = default;
};
struct LocalSinkState {
	virtual ~LocalSinkState() = default;
};

class IVecSource {
 public:
	virtual ~IVecSource() = default;
	virtual std::string GetName() const = 0;

	virtual std::unique_ptr<GlobalSourceState> GetGlobalSourceState(
	    const VecExecContext& ctx) const = 0;
	virtual std::unique_ptr<LocalSourceState> GetLocalSourceState(
	    GlobalSourceState& gstate) const = 0;

	virtual SourceResultType GetData(GraphDataChunk& chunk,
	                                 GlobalSourceState& gstate,
	                                 LocalSourceState& lstate) = 0;
};

class IVecOperator {
 public:
	virtual ~IVecOperator() = default;
	virtual std::string GetName() const = 0;

	virtual std::unique_ptr<OperatorState> GetOperatorState() const = 0;

	virtual OperatorResultType Execute(GraphDataChunk& input,
	                                   GraphDataChunk& output,
	                                   OperatorState& state,
	                                   const VecExecContext& ctx) = 0;
};

class IVecSink {
 public:
	virtual ~IVecSink() = default;
	virtual std::string GetName() const = 0;

	virtual std::unique_ptr<GlobalSinkState> GetGlobalSinkState() const = 0;
	virtual std::unique_ptr<LocalSinkState> GetLocalSinkState(
	    GlobalSinkState& gstate) const = 0;

	virtual SinkResultType Sink(GraphDataChunk& chunk,
	                            GlobalSinkState& gstate,
	                            LocalSinkState& lstate) = 0;

	virtual void Combine(GlobalSinkState& gstate, LocalSinkState& lstate) {}
	virtual void Finalize(GlobalSinkState& gstate) {}
};

}  // namespace neug::execution::vec
