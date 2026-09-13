# Task queue execution prototype

The execution layer builds subpipelines and their dependency graph. Operator
objects retain plan configuration; each execution owns separate state objects.
Operators do not receive a scheduler and cannot submit or wait for tasks.

```cpp
auto stream = pipeline.ExecuteStream(
    storage, stream_from_context(Context()), params, nullptr, 4);
auto result = materialize(std::move(stream));
```

`ExecuteStream` is the single execution entry point for reads, writes, DDL and
other commands. `Execute` materializes that stream for existing result consumers.
Every execution uses the same pipeline builder, dependency graph and task runner.
There is no separate scheduled API or per-operator admission switch.

The default worker count is one. Read-only executions may request more workers;
writable storage is constrained to one worker even when a higher count is
requested, preserving transaction sequencing. This enables worker execution of
writes without introducing concurrent mutation within one transaction. Keep the
plan, storage and optional profiling timer alive until the stream is consumed or
destroyed. The worker pool is joined before the completed execution returns to
its transaction owner. There is no Python/SQL worker-count switch yet.

## Plan, state and scheduling

| Component | Responsibility |
| --- | --- |
| `IOperator` | Plan configuration, input declarations and kernel selection |
| `OperatorState` | One execution's inputs, cursors and intermediate data |
| `PipelineBuilder` | Connect operator inputs and construct subpipeline boundaries |
| `PipelineFragment` | Output stream, prerequisite nodes, fork barriers and current morsel step |
| `MorselPipelineState` | Shared source allocation, local readers, transforms and bounded output waves |
| `PipelineGraph` | Dependency counts, ready nodes, completion notifications and failures |
| `TaskScheduler` | Worker pool and queue of runnable tasks |
| `PipelineExecutionState` | Own the graph, worker pool and final output pipeline |

`IOperator::sub_pipelines()` declares child plans and how they consume input:

- `kMaterialized`: all child results are required before the consumer can run.
- `kBuildProbe`: input 1 feeds a blocking build phase; input 0 streams through
  the probe phase after build completion. Ordinary Join uses this mode.
- `kStreaming`: a single child stream feeds the operator. Primary-key Join uses
  this mode and can stay in the same pull pipeline as its child and downstream.
- `kSequential`: child streams are consumed one after another on demand. Union
  uses this mode.

`Eval(storage, params, inputs, timer)` receives one `OperatorInputs` object
prepared by the builder. Ordinary operators use `TakeSingle()`, variadic
operators such as Union use `TakeAll()`, and build/probe operators use
`TakeBuildProbe()` with named `probe` and `build` streams. Taking inputs
transfers their ownership; fixed-arity access checks the number of inputs.
There is no separate upstream/branches pair in the interface.

`BuildProbeOperator` provides the state factory contract used by the builder.
Join only implements
that factory and declares its child plans. The builder uses `probe_plan()` and
`build_plan()` to connect the scheduled phases. It owns scheduling and dependency
tracking; operators receive neither a scheduler nor an execution graph.

Custom operator implementations use the unified `Eval` signature. Plugins
implementing this C++ interface must be rebuilt.

State classes such as `SourceState`, `LimitState`, `UnionState`, `JoinState` and
`PipelineOperatorState` are separate from operator definitions and `Eval`.
`Stream<ContextChunk>` owns them through `OperatorState`.

For example, `JoinOpr` holds Join parameters and child plans. Its independent
`JoinState` holds its input streams and a reusable `JoinTable`. The builder calls
`PrepareBuild()`, schedules `BuildPartition()` tasks, then calls `FinalizeBuild()`.
After publication, `ProbeChunk()` reads the immutable table from local pipeline
tasks. `Next()` provides chunk-wise probing when the probe input is behind a
global boundary. It has no futures, queue or dependency
counters.
The execution layer owns common-input caches and materialized branch buffers.

## Ordinary Join graph

The builder schedules the right build before the left probe pipeline:

```text
Input pipeline -> shared input buffer
                         |
                   right pipeline
                         |
                 materialized right rows
                         | prepare
                parallel local hash tables
                         | finalize / merge
                  immutable hash table
                         | build complete
                left source range allocation
                         | multiple workers
                local transforms -> Join probe -> downstream
```

The right input is materialized once. Build tasks use disjoint contiguous row
ranges and independent hash tables. A finalizer merges those tables in range
order before unlocking probe work. Its published table is reused for every left
chunk. Table merging is currently serial.
Inner, left outer, semi and anti joins use right-side hash lookup. Cartesian
Join retains the right rows without hashing; primary-key Join remains a separate
lookup implementation. Probe results follow left row order, with duplicate
matches in right row order. This can change incidental output order from the old
size-dependent inner-join kernel; queries requiring order must use `ORDER BY`.
A full right table can consume more memory than the old prefiltered table when
few right keys match the left input.

The common input is evaluated once and retains chunk boundaries. Each branch
has an independent reader cursor over the prepared input. Shared columns are
read-only; kernels create replacement columns when changing values.

Nested build/probe and materialized forks are recursively expanded into the
same DAG. The left pipeline's prerequisites depend on the right build node. The builder is driven by
input declarations, not operator names or Join-specific scheduler calls.

The external consumer runs the coordinator on first `Next()`:

1. Mark the ancestors needed by the output pipeline.
2. Enqueue nodes with no unfinished dependencies.
3. Receive worker completion notifications and unlock successor nodes.
4. Once output prerequisites finish, enqueue one pull of the output pipeline.

Subsequent `Next()` calls enqueue further output pulls without rerunning the
completed graph. At most one output pull is in flight for a stream.

A pipeline-step coordinator may itself run on a worker. When it waits for its
finite batch of morsel tasks, it helps execute queued work. Tasks submitted from
a worker go to the front of the queue, ahead of unrelated ready graph nodes.
This allows the same execution flow to make progress with one worker and keeps
serial transaction work ordered. Only the execution runtime uses this mechanism;
SQL operators do not submit or wait for tasks. Nested Join build phases remain
connected by dependency edges.

## Demand and failures

Union creates each child execution only when that branch is demanded. The child
uses the same builder, graph and execution state, with ready tasks executed
inline on the current worker. This avoids creating a nested pool or blocking a
worker on its own pool, and preserves the enclosing transaction's single-worker
constraint. Nested Joins retain their build/probe dependencies in this local
graph. An unconsumed branch is neither initialized nor read, so early termination
does not trigger errors or side effects from an unused branch. Conditional
graphs are not yet distributed across the enclosing worker pool.

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

## Morsel pipelines

`pipeline_behavior()` declares a source, a chunk-local transform, or a global
boundary. The default is global, so an operator must explicitly opt into local
execution. The builder groups a source and adjacent local transforms into one
`MorselPipelineState`:

```text
shared source: Pick() -> range 0, range 1, ...
                         | dynamically claimed
worker-local reader -> Filter -> Project -> optional Join probe
                         | completed range outputs
               ordered, bounded output wave
                         |
                global Limit / aggregate / sort
```

Scan allocates ranges of physical vertex positions, including deleted positions;
its readers check visibility and return only live rows. Ranges contain up to
4096 positions and readers produce chunks of up to 1024 rows/positions. The
allocation cursor is protected by the step, while each logical worker lane has
its own reader and predicate binding. Filter and ordinary Project run in the
same task for that range. Operator plan objects remain shared and immutable.
Per-morsel transform state is constructed separately for each task's work.

DataSource wraps the existing supplier in `ChunkMorselSource`. Reading/decoding
supplier batches is serialized during allocation, then ranges of decoded rows
can be processed by different workers. This does not yet provide parallel file
reading or CSV decoding. Incoming execution dependencies are consumed before
starting source allocation.

Each wave submits one task per worker. A task handles at most two morsels (one
with a single worker), claiming the next available range after finishing its
current range. Workers are not permanently assigned to a subpipeline. Completed
outputs are ordered by allocation sequence; all submitted tasks finish before
that wave is exposed. The next wave starts only when its outputs are demanded.
Thus an early Limit can avoid later waves, but may still cause bounded read-ahead
within the current wave. Output expansion, such as duplicate Join matches, is
not bounded by the input range size.

Source exhaustion and completion are distinct: `Finalize()` runs only after
allocation reaches EOF and all in-flight ranges complete successfully. Errors
are delivered in allocation order through `Next()`; a failed wave drains its
submitted tasks and does not finalize successfully. Early termination does not
call the source's successful-completion finalizer.

Ordinary Join attaches its probe kernel to the left morsel step when possible.
If the left input ends at a global boundary or reads a shared materialized input,
probing remains a single stream consumer. Global Limit, DISTINCT, aggregation,
sort/TopK and fused expansion-count operators are not cloned per worker.
Primary-key Join currently ends a morsel step.

## Scope and profiling

- Scan/Filter/Project and eligible Join probe chains partition actual input data.
  Index scans and other source types have not all been converted to range sources.
- Join builds local hash tables concurrently and merges them serially. There is
  no hash exchange, partition-local probe routing or spill implementation.
- Aggregation, dedup and sorting retain their global execution kernels.
- Conditional Union child graphs remain inline, preserving unused-branch laziness.
- The graph has no byte-based memory budget or cross-query admission control.
  Every execution has its own worker pool; writes use the same flow with one worker.
- `Stream` remains the result/error interface and a bridge between local kernels.
  Tasks run ranges through a pipeline segment, rather than scheduling each `Next()`
  or each operator as an individual task. This is not compiled kernel fusion.

Each worker lane has independent profiling counters. The step merges counters
only after all wave tasks complete, without merging or modifying the plan's child
timer tree from worker tasks. Join build time includes consuming the right input,
local table construction and finalization; child timers also record their own
work. Sum of concurrent task durations is not query wall time. Read-ahead can
make upstream row counts exceed the rows ultimately consumed by a Limit.

## Validation

`TaskSchedulerTest.*` checks real worker overlap, DAG execution with one worker,
failure propagation and draining, actual builder-generated fork concurrency,
one-time common-input preparation, unused sequential branches, pruning replaced
upstream inputs, empty-result metadata, and nested Join results/profiling at
1/2/4 workers. Two simultaneous executions reuse the same Join plan with
independent inputs and states.

The ordinary execution suite and Python streaming, query, DDL and transaction
tests validate the unified path, including commit and rollback behavior.
Additional scheduler tests cover task order and worker identity with writable
storage, and nested Join inside an unconsumed-tail sequential group on one worker.

`HashJoinTest.*` compares joins with a nested-loop oracle across duplicate keys,
empty inputs, both size relationships, multiple chunks and repeated probes of
the same table. `JoinBuildsOnceAndPullsOnlyOneProbeChunk` checks that the real
operator consumes the build input once, pulls one left chunk per result, and
reports a later left-input error only when that chunk is requested.

`MorselExecutionTest.*` checks dynamic range allocation and real worker overlap,
ordered local transformations, a single global Limit, deferred errors, source
finalization, and real partitioned Join build/probe with profiling.
`HashJoinTest.ParallelBuildFinalizesBeforeConcurrentProbe` checks the publication
barrier and concurrent probing against a nested-loop oracle. Python regressions
exercise scans over 5003 rows, deleted-row visibility, global ordering/dedup and
joins across range boundaries.
