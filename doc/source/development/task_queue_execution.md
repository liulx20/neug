# Task queue execution prototype

This prototype adds an explicit execution-state interface and a task queue for
read-only pipelines. It is opt-in through the C++ execution API:

```cpp
auto stream = pipeline.ExecuteScheduled(
    storage, stream_from_context(Context()), params, 4, nullptr);
auto result = materialize(std::move(stream));
```

`storage` must be a read-only snapshot. Keep the pipeline, storage and optional
profiling timer alive until the returned stream is consumed or destroyed. The
normal `Execute` / `ExecuteStream` API continues to execute synchronously.
There is no Python/SQL configuration switch in this prototype.

## Execution states

`Stream<T>` owns a `StreamState<T>`. `OperatorState` is the specialization for
`ContextChunk`; its `Next()` returns `result<optional<ContextChunk>>`.
`IOperator::Eval` now receives an explicit `TaskScheduler*`. Custom operator
implementations must update that signature and explicitly opt in to scheduled
execution; plugins implementing this interface must be rebuilt.

Operator configuration remains in the cached plan. Each execution constructs
separate state objects:

| State | Owned execution data |
| --- | --- |
| `PipelineOperatorState` | Producer stream, operator name and profiling references |
| `ScheduledPipelineState` | Task scheduler and output stream |
| `SourceState` | Input dependency, reader state, supplier and initialization flag |
| `LimitState` | Input stream, remaining skip/count and completion flag |
| `UnionState` | Cached common input, current branch stream and branch index |
| `JoinState` | Input, cached common input, branch results, child timers and phase |
| `MapState` | Input stream and transform function |
| `GenerateState` | Producer function and completion flag |
| `DeferredState` | Input stream, initializer and initialized output stream |
| `BatchState` | Batches and current index |

Callbacks can still express scalar kernels and initialization functions. These
state objects make the main input cursors and execution phases explicit; this
prototype does not eliminate every closure from every operator. A stream remains
a single-consumer object. Sharing a state pointer does not permit concurrent
`Next()` calls on that state.

## Tasks and dependencies

The result consumer submits one output-pull task and waits for it. That task
executes the synchronous pull chain on a worker. There is at most one output-pull
task in flight for that stream, so output remains ordered and there is no
background chunk prefetch after downstream stops requesting data.

An ordinary Join has four phases:

1. Consume and cache the common input, retaining chunk boundaries.
2. Execute the left and right branch pipelines with independent streams and
   timers. Queue the left branch; the current task executes the right branch.
3. Once both branches complete, invoke the existing Join kernel.
4. Return the joined chunk and then EOF.

With multiple workers, the two branches can overlap. A worker waiting for a
queued child task can execute another queued task. This allows nested Joins to
make progress even with one worker; workers do not all block waiting for queued
children. Tasks cannot wait cyclically on the same stream.

Both branches finish before the Join returns, including on an error path.
Exceptions are retained until sibling tasks are joined. Stream errors and EOF
remain terminal. Destroying a partially consumed result joins the worker pool;
it does not start additional pulls. A long-running `Next()` has no mid-call
cancellation or preemption in this version.

Union retains sequential branch consumption and propagates the scheduler into
nested pipelines. It does not buffer both output branches for parallel Union.
Common-input columns are shared between branch inputs; supported kernels must
read shared column contents and create replacement columns when modifying data.

## Supported scope and limitations

Built-in retrieval operators and file sources opt in to task execution.
Join/Union eligibility also checks their child pipelines. Write/admin operators,
exports, arbitrary procedure calls and GDS operations do not opt in. A read-only
storage interface alone is not sufficient to authorize task execution.

This is branch parallelism, not partitioned data parallelism. In particular:

- Scan is not split into independent scan ranges.
- Filter/Project in one linear pipeline execute on the same worker.
- Join still materializes both branch results and invokes the existing kernel.
  Hash-table build and probe are not parallelized.
- Dedup, aggregation and sorting have no hash exchange or parallel merge stage.
- The task queue has no byte-based memory budget or spill support. Work is
  submitted at pipeline-pull and Join-branch boundaries, not once per input row.
- PROFILE keeps separate branch timers. Concurrent child times may overlap and
  must not be summed to interpret query wall time.
- Every scheduled execution starts its own worker pool on first demand; admission control and
  sharing a pool across queries are future work.

Parallel Hash Join will require explicit build/probe states and a compiler /
executor agreement on build side and partitioning. That change is separate from
concurrently producing the two input relations.

## Validation

`TaskSchedulerTest.*` covers actual worker overlap, nested dependencies with one
worker, joining siblings after exceptions, demand-driven ordered pulls, terminal
errors, operator eligibility, empty-result metadata, and real nested Join
execution with a multi-chunk common input and profiling at 1/2/4 workers.

The ordinary execution tests and Python streaming/query tests also exercise the
synchronous path after introducing explicit states.
