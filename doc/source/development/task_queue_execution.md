# Task queue execution prototype

The execution layer builds subpipelines and their dependency graph. Operator
objects retain plan configuration; each execution owns separate state objects.
Operators do not receive a scheduler and cannot submit or wait for tasks.

```cpp
auto stream = pipeline.ExecuteScheduled(
    storage, stream_from_context(Context()), params, 4, nullptr);
auto result = materialize(std::move(stream));
```

This C++ API is opt-in and requires a read-only storage snapshot. Keep the plan,
snapshot and optional profiling timer alive until the output stream is consumed
or destroyed. There is no Python/SQL switch in this prototype.

## Plan, state and scheduling

| Component | Responsibility |
| --- | --- |
| `IOperator` | Plan configuration, input declarations and kernel selection |
| `OperatorState` | One execution's inputs, cursors and intermediate data |
| `PipelineBuilder` | Connect operator inputs and construct subpipeline boundaries |
| `PipelineFragment` | Output stream, prerequisite nodes and enclosing fork barriers |
| `PipelineGraph` | Dependency counts, ready nodes, completion notifications and failures |
| `TaskScheduler` | Worker pool and queue of runnable tasks |
| `ScheduledPipelineState` | Own the graph, worker pool and final output pipeline |

`IOperator::sub_pipelines()` declares child plans and how they consume input:

- `kMaterialized`: all child results are required before the consumer can run.
  Ordinary Join uses this mode.
- `kStreaming`: a single child stream feeds the operator. Primary-key Join uses
  this mode and can stay in the same pull pipeline as its child and downstream.
- `kSequential`: child streams are consumed one after another on demand. Union
  uses this mode.

`Eval` receives `OperatorInputs`, prepared by the builder. It has no scheduler or
execution-graph argument. Custom operator implementations must update their
signature and explicitly opt in through `supports_task_execution()`; plugins
implementing this interface must be rebuilt.

State classes such as `SourceState`, `LimitState`, `UnionState`, `JoinState` and
`PipelineOperatorState` are separate from operator definitions and `Eval`.
`Stream<ContextChunk>` owns them through `OperatorState`.

For example, `JoinOpr` holds Join parameters and child plans. Its independent
`JoinState` holds input streams and completion progress, and invokes the existing
Join kernel. It has no child-task phases, futures, queue or dependency counters.
The execution layer owns common-input caches and materialized branch buffers.

## Ordinary Join graph

The builder splits a materialized fork into these stages:

```text
Input pipeline -> shared input buffer
                         |
                +--------+--------+
                |                 |
          left pipeline     right pipeline
                |                 |
          left buffer       right buffer
                +--------+--------+
                         |
                  Join -> downstream
```

The common input is evaluated once and retains chunk boundaries. Each branch
has an independent reader cursor over the prepared input. Shared columns are
read-only; kernels create replacement columns when changing values.

Nested materialized forks are recursively expanded into the same DAG. A branch
collector depends on its own nested prerequisites. The builder is driven by
input declarations, not operator names or Join-specific scheduler calls.

The external consumer runs the coordinator on first `Next()`:

1. Mark the ancestors needed by the output pipeline.
2. Enqueue nodes with no unfinished dependencies.
3. Receive worker completion notifications and unlock successor nodes.
4. Once output prerequisites finish, enqueue one pull of the output pipeline.

Subsequent `Next()` calls enqueue further output pulls without rerunning the
completed graph. At most one output pull is in flight for a stream.

Workers execute ready tasks and return. They never wait for child futures,
recursively run queued dependencies, or call the graph coordinator. Nested Joins
therefore work with one worker without special work-helping logic.

## Demand and failures

Union's sequential child group stays inside one pull pipeline. An unconsumed
branch is neither initialized nor read, so downstream early termination does
not trigger errors from an unused branch. Joins inside such a sequential group
currently execute synchronously inside that group; extracting conditional
subgraphs while retaining this demand behavior is future work.

An operator such as Scan can declare that it does not consume its incoming data
stream. The builder prunes those unused data dependencies while retaining any
mandatory enclosing-fork barrier. Thus a Join's common input still completes
before either branch begins, even when a branch starts with a fresh scan.

A failed prerequisite does not unlock its consumers. The coordinator stops
submitting new nodes and waits for every submitted task to finish before
returning the error. Both task exceptions and coordinator-side failures drain
in-flight work. Errors and EOF remain terminal through `Stream::Next()`.

The worker pool starts on first demand. Thread-start failures also pass through
`Next()`'s error boundary. Destroying a partially consumed result joins the pool
without starting further output pulls. There is no mid-call cancellation or
preemption of a running synchronous kernel.

## Scope

This prototype parallelizes independent materialized child pipelines. It does
not yet partition scans or hash tables:

- Linear Scan/Filter/Project chains run on a worker as synchronous pull chains.
- Join still materializes both sides and uses the existing one-shot kernel.
  Parallel build/probe requires its own shared and worker-local Join state.
- Aggregation and sorting remain blocking kernels within a pipeline task.
- Dedup and aggregation have no hash exchange or parallel merge phase.
- The graph has no byte-based memory budget, spill support or cross-query
  admission control. Every execution has its own worker pool.
- Write/admin operators, exports, arbitrary procedures and GDS operations do not
  opt in to scheduled execution.

Each child pipeline has its own profiling state. A scheduled Join's local time
covers its kernel; branch preparation is recorded on its child pipelines.
Concurrent child durations must not be summed to interpret query wall time.

## Validation

`TaskSchedulerTest.*` checks real worker overlap, DAG execution with one worker,
failure propagation and draining, actual builder-generated fork concurrency,
one-time common-input preparation, unused sequential branches, pruning replaced
upstream inputs, empty-result metadata, and nested Join results/profiling at
1/2/4 workers. Two simultaneous executions reuse the same Join plan with
independent inputs and states.

The ordinary execution suite and Python streaming/query tests validate the
synchronous path after moving child-flow construction into the execution layer.
