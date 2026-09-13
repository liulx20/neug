# Task queue execution prototype

The execution layer builds subpipelines and their dependency graph. Operator
objects retain plan configuration; each execution owns separate state objects.
Operators do not receive a scheduler and cannot submit or wait for tasks.

```cpp
auto reader = pipeline.ExecuteReader(storage, Context(), params, nullptr, 4);
auto result = materialize(std::move(reader));
```

`ExecuteReader` is the single execution entry point for reads, writes, DDL and
other commands. `Execute` materializes that reader for existing result consumers.
Every execution uses the same pipeline builder, dependency graph and task runner.
There is no separate scheduled API or per-operator admission switch.

The low-level reader defaults to one worker. The normal AP query entry point
uses database `max_thread_num` by default; TP defaults to one execution worker
per query. Both accept a per-query override capped by database capacity;
writable storage is constrained to one worker even when a higher count is
requested, preserving transaction sequencing. This enables worker execution of
writes without introducing concurrent mutation within one transaction. Keep the
plan, storage and optional profiling timer alive until the reader is consumed or
destroyed. The worker pool is joined before the completed execution returns to
its transaction owner. Python callers can set the count without changing a
cached plan or subsequent calls:

```python
conn.execute("MATCH (n) RETURN n", num_threads=4)
```

Zero selects the execution-mode default, negative values are rejected, and writes
remain serial. Explicit transactions use the same setting (a writable transaction
still uses one worker even for a read statement). The C++ `Connection::Query`
accepts `num_threads` as its final argument. No SQL `SET` syntax is introduced.
See [the latency baseline](task_queue_baseline.md) for measurements and limitations.

## Plan, state and scheduling

| Component | Responsibility |
| --- | --- |
| `IOperator` | Plan configuration, input declarations and kernel selection |
| `OperatorState` | One execution's inputs, cursors and intermediate data |
| `PipelineBuilder` | Connect operator inputs and construct subpipeline boundaries |
| `PipelineFragment` | Output task, result columns, fork barriers and current segment |
| `MorselPipelineTask` | Shared source allocation, local readers, transforms and bounded ordered task completion |
| `QueueExecution` | Dependency gates, ready queue, one-slot mailboxes, completion events and failures |
| `TaskScheduler` | Worker pool and queue of runnable tasks |
| `QueryResultReader` | External result access and ownership of the execution |

`IOperator::sub_pipelines()` declares child plans and how they consume input:

- `kMaterialized`: all child results are required before the consumer can run.
- `kBuildProbe`: input 1 feeds a blocking build phase; input 0 streams through
  the probe phase after build completion. Ordinary Join uses this mode.
- `kStreaming`: a single child pipeline feeds the operator. Primary-key Join uses
  this mode.
- `kSequential`: child outputs are consumed one after another on demand. Union
  uses this mode.

`CreateState(storage, params, timer)` creates a `Kernel` owning one execution's
`OperatorState`. Its interface is deliberately independent of input transport:

```cpp
KernelResult Process(ContextChunk chunk); // zero or more output chunks
KernelResult Finalize();                  // called once when input completes
bool Finished() const;                    // early stop, for example Limit
```

The driver supplies input and forwards output. An operator cannot read its
upstream, execute child plans, submit work or wait for tasks. Ordinary transforms
use `make_chunk_kernel`; global kernels accumulate input through `Process` and
produce results in `Finalize`. Commands that must stabilize edge pointers retain
batch boundaries until finalization. Source operators have a separate
`CreateMorselSource` factory. Join's independent build state receives prepared
right-side data and exposes build/finalize/probe kernels to the builder.

`KernelChain` runs adjacent operators with an iterative loop. `LinearPipelineTask`
keeps global states across batches, forwards completion in pipeline order and
stops upstream work when a kernel finishes early. A morsel task uses the same
kernel driver for its local segment. No per-operator Stream or recursive
`Next()` chain is constructed. `Eval`, `OperatorInputs`, `map_chunks`,
`defer_stream`, `reduce_stream`, `generate_chunk` and `prepend_chunk` have been
removed. Plugins implementing the C++ operator interface must be rebuilt.

There is no generic `Stream`, `StreamState`, stream conversion helper or
boundary-level pull chain. `QueryResultReader` is a concrete external handle;
`Next()` requests a result chunk and drives completion events until data, EOF or
an error is available. Only this caller waits. Workers execute ready kernel,
morsel or build tasks and post a completion notification; they never call
another pipeline's `Next()` or wait for work in the same pool.

Each boundary has a one-slot mailbox. A consumer waiting for input registers
with its producer and suspends. Publishing a chunk or completing a dependency
puts the waiting consumer on the ready queue. Consuming a chunk does not
prefetch the next one: another upstream task runs only when the downstream
requests more data. Completed work within a kernel invocation or morsel
can retain multiple chunks, so the mailbox capacity is not a byte memory limit.
The coordinator runs on the thread calling `Next()`; no additional coordinator
thread or polling scan over the task graph is required.

Union's `ConcatTask` demands only its current branch. Later branches are part of
the same graph and pool, but their execution state and suppliers are not
initialized until demanded. Join's shared-input and build buffers are explicit
`BufferTask` barriers. `ReplayTask` keeps an independent cursor over a completed
buffer and publishes batches through its mailbox. Ordinary Join probes a
published table through `ProbeChunk` in both local and global segments.

A successful end is distinct from a failure. Errors remain terminal on the
reader. On error, destruction or a downstream early stop, no additional source
work is requested. All submitted tasks are drained before execution-owned state
is destroyed or the transaction owner receives EOF/error. Running storage calls
are not forcibly interrupted. A source's successful finalizer is not called
merely because the consumer stopped early.

COPY's insert state receives chunks from the driver. For each chunk it passes a
finite `BatchChunkSupplier` to the storage API, and accumulates result cardinality
until `Finalize`. The supplier maps columns but never calls into the execution
pipeline. This increases storage API call frequency compared with passing one
supplier for the entire input, while preserving bounded input buffering and the
transaction's existing commit/rollback boundary. All writes still use one worker.

## Batch merging

Global input kernels, Join build preparation, Context flattening and result
column serialization use `OrderedBatchAccumulator`. Level i retains a consecutive
group of 2^i input batches. Adding a batch merges occupied levels from low to
high, always keeping older rows on the left. Completion combines the occupied
levels from high to low. Empty typed batches remain part of this sequence.

This replaces repeated full-prefix copying with O(N log K) row-copy work for
N rows in K batches (assuming linear-cost column concatenation). Single-batch
input keeps column ownership unchanged. Row order, nulls, sparse aliases and
head alignment are preserved for batches sharing the same schema. The operation
still materializes all rows where required; it is not a memory budget, spill
implementation or parallel aggregate. See the baseline's before/after results.

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

## Demand and failures

On `Next()`, the external consumer requests the output task. Missing input or
unfinished gates register a waiter and enqueue the required upstream task.
Workers run only ready work. Completion events resume the waiting tasks; the
coordinator sleeps on a condition variable when no task can advance. The graph
is built once, and subsequent calls resume its execution state.

Union branches are built into the same graph but activated sequentially. An
unused branch's operator state and supplier are never initialized. Nested Joins
retain their build/probe gates and can use the enclosing pool, including when
that pool has only one worker. There is no nested executor or worker-side wait.

An operator such as Scan can declare that it does not consume incoming data.
The builder prunes unused data dependencies while retaining mandatory enclosing
fork barriers. Thus a Join's common input completes before either branch starts,
even if a branch begins with a fresh scan. Materialized write branches also have
explicit ordering gates, preserving plan order with the single write worker.

A failed task does not unlock its consumers. The coordinator stops submitting
new work and drains all submitted tasks before returning the error. The pool
starts on first demand, so thread-start errors also pass through the reader's
error boundary. Destruction stops future demand and joins the pool; it does not
preempt a running synchronous kernel.

## Morsel pipelines

`pipeline_behavior()` declares a source, a chunk-local transform, or a global
boundary. The default is global, so an operator must explicitly opt into local
execution. The builder groups a source and adjacent local transforms into one
`MorselPipelineTask`:

```text
shared source: Pick() -> range 0, range 1, ...
                         | dynamically claimed
worker-local reader -> Filter -> Project -> optional Join probe
                         | completed range outputs
               ordered, bounded completed ranges
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

Each source submits at most W range tasks for W workers. Completion publishes
that lane's result and profiling counters to the coordinator and wakes the
source task. Results are indexed by allocation sequence. The next contiguous
result can be delivered immediately, even while another range is still running;
a slow earlier range still holds back later results to preserve row order.

Active tasks and completed results share W slots. Delivering a range frees a
slot; while serving downstream demand the source can refill it so workers overlap
with downstream processing. The currently delivered range is retained separately,
giving at most W+1 source ranges in flight or buffered. The one-worker case
retains exact demand-driven reading without speculative refill. Completion
callbacks never submit replacement work, so pausing consumption cannot create
unbounded read-ahead. A task handles one morsel and each lane owns its reader.

The limit counts ranges, not bytes or output rows. One range may produce several
chunks or a large Join result. Global kernels and explicit replay/build buffers
still retain their required inputs independently of this limit.

Source exhaustion and completion are distinct. `Finalize()` runs only when EOF
has been observed, all allocated ranges have completed successfully and their
ordered outputs have been delivered. A failed range stops allocation; errors
are delivered in allocation order through `Next()`. Error/EOF handling and reader
destruction stop new work and drain submitted tasks. Cancellation is checked
before allocation and between reader chunks; it does not interrupt a blocking
storage call. Early termination does not call the source's successful finalizer.

Ordinary Join attaches its probe kernel to the left morsel step when possible.
If the left input ends at a global boundary or reads a shared materialized input,
probing remains a single task segment. Global Limit, DISTINCT, aggregation,
sort/TopK and fused expansion-count operators are not cloned per worker.
Primary-key Join currently ends a morsel step.

## Scope and profiling

- Scan/Filter/Project and eligible Join probe chains partition actual input data.
  Index scans and other source types have not all been converted to range sources.
- Join builds local hash tables concurrently and merges them serially. There is
  no hash exchange, partition-local probe routing or spill implementation.
- Aggregation, dedup and sorting retain their global execution kernels.
- Conditional Union branches use the same ready queue and worker pool, preserving
  unused-branch laziness.
- The graph has no byte-based memory budget or cross-query admission control.
  Every execution has its own worker pool; writes use the same flow with one worker.
- `QueryResultReader::Next()` is the external result/error interface. Internal
  boundaries exchange chunks through mailboxes. Tasks run a kernel segment or
  source range; local operator fusion is an interpreted loop, not compiled code.

Each worker lane has independent profiling counters. Completion events merge
counters on the coordinator after that individual task finishes, including
during cancellation draining. Worker tasks never modify the plan's timer tree. Join build time includes merging the collected right rows, local table
construction and finalization; right-side pipeline tasks record their own work
without charging queue waits to the Join. Sum of concurrent task durations is not query wall time. Read-ahead can
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
the same table. `JoinStateReceivesBuildDataAndReusesPublishedTable` checks build publication and
repeated probing without any operator-owned input reader.

`MorselExecutionTest.*` checks dynamic range allocation and real worker overlap,
ordered local transformations, a single global Limit, deferred errors, source
finalization, and real partitioned Join build/probe with profiling.
`HashJoinTest.ParallelBuildFinalizesBeforeConcurrentProbe` checks the publication
barrier and concurrent probing against a nested-loop oracle. Python regressions
exercise scans over 5003 rows, deleted-row visibility, global ordering/dedup and
joins across range boundaries.

`DeepPipelineRunsKernelsIterativelyAndFinalizesOnce` executes 4096 consecutive
kernels over multiple chunks and verifies processing order and one completion
per state. COPY tests cover late input errors and rollback across batches;
export tests verify column order across the Context-based extension ABI boundary.

`PausedConsumerDoesNotRefillCompletedSlots` verifies bounded source read-ahead and
no work after consumer cancellation. `FailureWaitsForOtherSubmittedBranch`
holds one branch in flight while its sibling fails and verifies that the error
is not returned before draining. `LimitDoesNotDemandNextUnionBranch` checks that
normal EOF after a Limit does not initialize the unused Union tail, with both
one and four workers.

`DeliversBeforeLaterRangeAndRefillsBoundedSlots` holds a later range blocked and
verifies delivery through a downstream operator and reuse of the freed lane.
`SlowFirstRangeBoundsOutOfOrderResults` blocks the first range and verifies that
completed later ranges cannot grow beyond the slot budget.
`EarlyStopDrainsLaterRangeWithoutFinalizingSource` verifies that Limit exposes
its row before the slow range finishes, then drains it before returning EOF.

`ErrorStopsRefillAndDrainsBlockedRange` holds a later task blocked while an earlier
one fails, and verifies terminal error propagation, draining and no successful
source finalization.
