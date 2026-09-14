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

Global input kernels, Join build preparation, Context flattening and general
result column serialization use `OrderedBatchAccumulator`. Level i retains a consecutive
group of 2^i input batches. Adding a batch merges occupied levels from low to
high, always keeping older rows on the left. Completion combines the occupied
levels from high to low. Empty typed batches remain part of this sequence.

This replaces repeated full-prefix copying with O(N log K) row-copy work for
N rows in K batches (assuming linear-cost column concatenation). Single-batch
input keeps column ownership unchanged. Row order, nulls, sparse aliases and
head alignment are preserved for batches sharing the same schema. The operation
still materializes all rows where required; it is not a memory budget, spill
implementation or parallel aggregate. See the baseline's before/after results.


Primitive result serialization bypasses this merge for multiple native value
chunks. Boolean, signed/unsigned 32/64-bit integers, float/double and strings
append directly into one reserved protobuf array per output column. Validity
bits are concatenated by row index, including boundaries not divisible by eight;
a nullable chunk makes the full output validity bitmap explicit. Chunk order,
empty typed chunks, sparse/repeated output aliases and the wire representation
are preserved. All backing columns are checked before values are written.
Native list chunks also bypass parent-column concatenation: the serializer
writes global offsets and validity while selecting each chunk's referenced child
elements in logical row order. Primitive and nested-list children use the same
chunk serializer recursively. Other child types are rebuilt once through their
column builder before serialization. This preserves sliced/repeated elements,
NULL versus empty lists, and the collected representation's nullable metadata.
Single chunks use the existing direct serializer. Other root types or column
representations still use the general column accumulator and serializer.
See [representative GroupBy validation](groupby_workload_validation.md).

This removes intermediate primitive-column concatenation, not result buffering:
`materialize` still retains output chunks, and the response arrays still contain
the complete result. Serialization remains on the consumer thread. Operator
scheduling and pipeline boundaries are unchanged.

## Ordinary Join graph

The builder schedules the right build before the left probe pipeline:

```text
Input pipeline -> shared input buffer
                         |
                   right pipeline
                         |
                bounded input batch slots
                         | parallel partition tasks
                ordered per-bucket build tasks
                         | finalize / publish
                  immutable hash partitions
                         | build complete
                left source range allocation
                         | multiple workers
                local transforms -> Join probe -> downstream
```

Right-side chunks enter at most `worker_count` pending build slots. Workers
partition different chunks concurrently, encoding generic keys once. For each
hash bucket, the coordinator submits at most one append task at a time, in input
batch order. A null partition result skips append jobs while advancing every
bucket's input sequence. Join uses this for redundant empty chunks; it retains a
first typed empty chunk when necessary for output metadata. Different buckets
advance independently. A slot is released only
after all buckets have appended it; when slots fill, the build task stops asking
for upstream input. Upstream sources retain their own bounded read-ahead.
This limits pending batches, not bytes or retained hash-table data.

Tables retain immutable original chunks and store `(chunk, row)` references.
There is no concatenation of the entire right input before building. Finalization
runs only after upstream EOF and every append completes; it publishes the tables
without merging them. Probe workers hash each key to its bucket and gather only
matched build rows, restoring left-input and duplicate-match order. When all
matches come from one chunk, gathering directly selects rows from that chunk
without grouping by chunk or constructing a restoring permutation. Selected
output rows spanning multiple chunks may still be copied and merged. Partition tasks and append tasks use
the same executor pool, with no worker waiting for another task inside a kernel.
Skewed keys can still concentrate appends in one bucket. With one worker the same
protocol executes serially.

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

Ordinary edge expansion and GetV now declare chunk-local behavior, like Filter
and Project. This includes edge/neighbor output, specialized edge and neighbor
predicates, general predicates, optional expansion, and per-input degree output.
The immutable operator configuration is shared; each invocation binds its own
predicate and owns traversal temporaries, selection vectors, and result columns.
The common morsel executor runs these kernels on independent input ranges and
preserves each range's row mapping and duplicate edges. No operator creates its
own threads. A high-degree vertex is still processed within one input range;
this is not parallel traversal of a single adjacency list.

Dedicated shortest-path operators and the global ExpandCount fusion remain
outside this chunk-local graph expansion coverage. Ordinary GetV and the seven EdgeExpand variants in
`edge.cc` are enabled; write parallelism is not broadened.

`PathExpandVOpr`, `PathExpandOpr`, and `PathExpandOprWithPred` also use the
chunk-local executor. Frontiers, path builders and input-row offsets belong to
each invocation. SIMPLE/TRAIL checks operate on the current path; the generic
ANY_SHORTEST branch creates visited state per root. Predicates are bound per
invocation. There is no cross-chunk visited set or deduplication. A single root's
search is not split among workers. Chunking may change traversal output order
without ORDER BY.

PathColumn now supports concatenation, required when multiple result chunks
contain complete paths or a downstream operator collects path columns. It appends
path references in chunk order and preserves NULL metadata, including typed empty
columns and zero-hop paths. It does not reconstruct paths or change their contents.

Path expansion validation uses 5,003 vertices with cycles, self-loops, parallel
edges and isolated vertices. At 1/2/4 workers it checks zero-to-two-hop endpoints,
repeated roots produced by an earlier expansion, complete vertex/edge sequences,
and predicate-filtered paths against independently enumerated walks. PROFILE
asserts each of the three enabled implementations executes. The path-column unit
test also checks NULL and zero-hop paths, empty columns, and concatenation in both
orders. The final build passes 190 C++ tests and 382 selected Python tests,
including the existing path suite (34 skipped, 20 deselected). The 47 result-reader,
scheduler and morsel tests pass 20 repetitions. Dedicated shortest-path operators
are not enabled by this change, and no path-workload speedup is claimed.

`IntersectOprMultip`, `IntersectWithEdgeOpr` and `TCOpr` also declare chunk-local
behavior. Intersect builds intersection sets per input row and binds predicates
per invocation. The TC fusion enumerates matching neighbor pairs for each root;
its neighbor set, builders and offsets are local. It does not update a shared
triangle counter. Its optimized LT/GT loops now advance the source-row offset
when a root has no qualifying neighbors, preventing subsequent matches from
being attached to an earlier input row.

The triangle integration test checks 1/2/4 workers against independently generated
rows, including edge properties, and PROFILE confirms both Intersect operators.
The filtered triangle queries currently retain an unfused plan, so a C++ test
constructs the actual TCOpr directly. It verifies the optimized storage branch,
both LT/GT predicates, and correct input-row mapping for 6,000 repeated roots
with empty roots preceding matches at 1/2/4 workers. The rebuilt engine passes
191 C++ tests and 385 selected Python tests (34 skipped, 20 deselected); the 48
result-reader, scheduler and morsel tests pass 20 repetitions. These checks cover
correctness; they do not establish a triangle-workload speedup.

The graph integration test uses 5,003 vertices, parallel edges and isolated
vertices, and checks 1/2/4 workers against independently generated expected rows.
It covers outgoing/incoming/undirected traversal, edge and neighbor predicates,
optional NULL rows, two-hop multiplicity, degree counts, vertex-only output,
and downstream ORDER BY/SKIP/LIMIT. PROFILE verifies an expansion operator is
present. The rebuilt engine passes 189 C++ and 371 embedded Python tests
(28 skipped, 20 deselected); 54 concurrency-related tests pass 20 repetitions.
This validation establishes correctness coverage, not a graph-workload speedup.

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
the builder now starts a range step for probe and subsequent chunk-local kernels.
Global Limit, DISTINCT, aggregation, sort/TopK and fused expansion-count operators
are still not cloned per worker.
Primary-key Join currently ends a morsel step.

## Parallel consumption of intermediate results

For read-only execution with multiple workers, the builder starts a range step
when chunk-local kernels or Join probe follow a non-range segment. It reuses the
existing morsel scheduler, ordered completion slots, cancellation and local
profiling counters. Input column aliases are carried into each range kernel.

A shared replay buffer gives each branch its own range source and chunk cursor.
Workers in that branch claim different ranges; another branch independently
visits all the same rows. Sources are created only after the branch's dependency
gates complete, including Join build publication. Empty or unused branches retain
the existing demand behavior.

For an ordinary upstream mailbox, the coordinator acquires one chunk on demand
and exposes 4,096-row ranges (read in batches up to 1,024 rows). After every range
of that chunk is delivered, it acquires the next chunk. It does not add an eager
whole-input materialization barrier. Empty typed chunks are delivered too. Worker
readers are released before advancing to the next input chunk. Limit and errors
stop requesting additional input and drain submitted work through the same
executor cancellation path.

This currently parallelizes ranges within one mailbox chunk; it does not overlap
range execution across several small mailbox chunks. Shared replay buffers can
supply ranges across chunk boundaries. Small inputs may offer too little work
for several workers. Sorting remains serial; partitioned Dedup and ordinary
GroupBy are described below. Writes still use one worker in the same graph. Range slicing copies selected columns;
there are no zero-copy slices or new byte-based memory limits here.

## Partitioned Dedup

Dedup, ordinary GroupBy and Join build share `PartitionPipelineTask` in the executor. Each query
owns a separate `PartitionState`; the operator plan holds only key aliases.
The task keeps at most W incoming batches in its partition/append slots for W
workers. Workers remove repeated keys within each batch and hash the remaining
keys into W partitions. Each partition has its own seen set, consumes batches in
input order and can advance independently of the other partitions. There is no
shared seen-set lock and no scheduling code inside the Dedup operator.

After all batches have been appended, finalization reconstructs first-occurrence
input order across partitions and batches. For single-column keys it then invokes the existing Dedup
helper on the surviving candidates. Composite keys are already unique after
partition processing and do not run a second global Dedup pass. This preserves the existing sorted order for
ordinary non-null scalar columns and first-occurrence order for composite or
nullable keys. The final result is still materialized as one chunk, and final
normalization runs on one worker. With one partition, selected rows are already
in input order, so finalization skips the redundant merge and reshuffle. A downstream range segment can consume that
result in parallel.

Pre-reduction is selective. For sortable scalar types, each batch samples up to
64 keys; if more than half are distinct, it keeps the batch without hashing all
rows. This avoids replacing an efficient scalar sort with an expensive hash
build for nearly unique inputs. Mixed reduced and retained batches still pass
through the same task and final helper, which enforces global uniqueness.
The threshold is a local heuristic, not a compiler cardinality estimate. Signed
and unsigned 32/64-bit keys now use native integer hash sets in sampling, local
reduction and partition reduction; NULL has a separate optional-key identity.
Other encoded paths reuse their byte buffer across rows instead of allocating a
new buffer every time.

Single-column floating-point, edge and other types whose native equality has not
been matched to key encoding retain all candidates. For example, nullable and
non-null floating columns can treat signed zero differently, and edge column
helpers can account for property identity. Composite keys keep the existing
encoded-key equality. Sampling changes work performed, never query semantics.

The queue bound covers pending batches, not total memory. Seen sets and surviving
candidates grow with distinct-key count; retained batches may contain duplicates.
There is no spill, byte budget or incremental downstream DISTINCT output. Scalar
high-cardinality inputs still pay some sampling and scheduling overhead.

## Partitioned GroupBy aggregation

Eligible GroupBy plans use the same `PartitionPipelineTask` and its W bounded
batch slots. `GroupByOpr` owns immutable key mappings and aggregate descriptors;
each execution creates a separate `GroupByState`. Batches can enter that state
in two forms: locally reduced keys and partial aggregate columns, or original
rows routed directly to partition buckets. Both forms update the same partition
group tables and aggregate columns in input order. There is no input replay,
second scheduler or additional query setting.

Local reduction starts enabled. A complete locally reduced batch measures its
actual group count; at least one group per four input rows selects direct routing
for subsequent batches. While direct routing is selected, every 32nd batch does
local reduction again to detect changed distributions. While local reduction is
selected, every batch updates this observation. This is a heuristic calibrated
on the integer COUNT/SUM diagnostic, not a general cost model for all key types
and aggregates. It avoids using a small prefix as a cardinality estimate.
Ungrouped aggregates always use partial states, including empty input.

Direct batches retain their source columns until all partition buckets have been
consumed. Buckets carry row selections, and each partition consumes original
values directly into its own aggregate arrays. This avoids local group tables,
partial aggregate arrays and local key reshuffling. Scalar INT32/INT64 keys use
native hashing for both routing modes; direct batches need no encoded signatures.
For raw batches backed by native INT32/INT64 ValueColumns, partition lookup and
retained-key writes use typed access instead of constructing a generic Value per row. Other column
representations and locally reduced batches continue through their existing
virtual accessors; reduced batches skip the added runtime type dispatch. NULL and
integer zero may share a bucket but remain separate groups. General
keys retain the existing encoded-key equivalence and fixed null bitmap.

Strategy hints are execution-owned relaxed atomics. Concurrent local batches
can update the hint in completion order, so task timing can affect which mode a
later batch uses. The partition lanes still process batches in input order, but GroupBy output
rows have no implicit ordering guarantee. Floating SUM/AVG association can also vary
with mode selection; bitwise reproducibility is not promised.

COUNT stores a non-null count (COUNT(*) counts every row). SUM retains the input
numeric width. AVG stores a double sum and a non-null count, merging both before
division. MIN/MAX retain the selected value and whether any non-null value has
been observed. A group containing only NULL has COUNT/SUM = 0 and MIN/MAX/AVG =
NULL, matching the existing helper. Ungrouped empty input returns one row even
when upstream produces EOF without a chunk; grouped empty input returns no rows.

Each partition finalizes its own key and aggregate columns into a result chunk.
GroupBy does not record first-appearance positions or globally merge groups by
those positions. Empty partitions are omitted; grouped empty input retains one
typed empty chunk, and ungrouped empty input emits one aggregate row. Output
heads are cleared to match the collected GroupBy helper.

After partition updates complete, the executor asks the execution state for
independent finalization work. For GroupBy with at least 4,096 groups per configured
partition on average, it queues one finalizer per partition; smaller results stay
inline to amortize scheduling overhead. This threshold is a granularity heuristic.
Each finalizer writes only its partition's result. The publication task waits for
all finalizers, transfers the completed chunks, and releases build state. Status
errors and exceptions use the same failure/drain mechanism as build tasks.
There is no heap, global row-reference array, or copying into a merged key column. The result consumer and downstream
operators receive multiple chunks. Explicit ORDER BY remains responsible for
sorting; LIMIT without ORDER BY can select different groups with different worker
counts. List/FIRST and other ineligible aggregates still use their existing global
kernel; the ordering of aggregate contents is not changed by this optimization.
The scheduler, demand, cancellation and profiling mechanisms remain shared with
Join and Dedup; no worker recursively reads another operator.

The current eligibility rules are:

- COUNT(*) and COUNT of one already projected column.
- SUM/AVG over signed/unsigned 32/64-bit integers, float and double.
- MIN/MAX over integers, boolean, strings, dates, timestamps and intervals.

If any function is ineligible, the whole GroupBy uses its existing global kernel
in the same task executor. DISTINCT aggregates, list/set collection, FIRST,
multiple-variable aggregates, unprojected property expressions and floating
MIN/MAX are not converted yet. Floating MIN/MAX needs special handling because
NaN can make selection depend on input order even within a batch.

Floating SUM/AVG changes addition association, including with one worker when
there are multiple batches. Results are not guaranteed bit-identical to the
collected helper or across worker counts. Ordinary data may differ in the last
bits; cancellation, large magnitudes, overflow and non-finite values can produce
larger differences. Integer SUM uses unsigned addition and bit-preserving
conversion for fixed-width wrapping, avoiding signed-overflow undefined behavior
in new partial-state code; it does not add checked-overflow errors or widen types.

Partial columns allocate only fields used by their aggregate. COUNT needs a
count array, SUM a value array, AVG a sum and count, and MIN/MAX values and counts.
For COUNT plus SUM(INT64), these array payloads total 16 bytes per group rather
than 48; this excludes group tables, keys, vector capacity and result buffers.

The slot limit is not a total memory budget. Group tables and result state grow
with distinct groups, and batches with nearly unique keys gain little from local
aggregation. There is no spill, skew repartitioning or byte limit. Direct batches
retain all source columns until their buckets are consumed, so the bounded batch
count is not a bound on bytes. A single hot group benefits from local reduction,
but its partials are merged by one partition lane.

## Scope and profiling

- Scan/Filter/Project and eligible Join probe chains partition actual input data.
  Index scans and other source types have not all been converted to range sources.
- Join partitions incoming chunks concurrently and incrementally builds hash
  buckets through bounded batch slots. It retains build chunks until query
  completion; there is no spill. Probe can now resume range execution after
  global boundaries or from shared replay buffers.
- Ordinary GroupBy routes raw rows or locally reduced aggregate states into
  independent hash partitions, using observed batch compression to choose. Unsupported aggregate combinations and sorting
  retain global kernels. Dedup performs parallel pre-reduction and a final
  serial normalization.
- Conditional Union branches use the same ready queue and worker pool, preserving
  unused-branch laziness.
- The graph has no byte-based memory budget or cross-query admission control.
  Every execution has its own worker pool; writes use the same flow with one worker.
- `QueryResultReader::Next()` is the external result/error interface. Internal
  boundaries exchange chunks through mailboxes. Tasks run a kernel segment or
  source range; local operator fusion is an interpreted loop, not compiled code.

Each worker lane has independent profiling counters. Completion events merge
counters on the coordinator after that individual task finishes, including
during cancellation draining. Worker tasks never modify the plan's timer tree. Join build time includes chunk partitioning, incremental table
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

`IncrementalBuildBoundsPendingInputAndDrainsFailure` forces out-of-order
partition completion and holds one bucket while another advances. It verifies
bounded upstream consumption, input-order appends, the probe barrier and draining
on a build error. `IncrementalChunksArePartitionedConcurrentlyAndKeepMatchOrder`
checks multiple retained chunks against the nested-loop oracle, including
Cartesian, inner, outer, semi and anti joins.

`MaterializedGlobalOutputRunsOrderedParallelRanges` checks both a global collected
result and repeated mailbox chunks with an empty typed chunk between them.
`SharedReplayBranchesOwnParallelCursors` checks parallel consumption of small
shared chunks without lost rows or cross-branch mutation.
`JoinProbesMaterializedInputThenKeepsParallelTransforms` checks shared build/probe
input followed by concurrent transforms. These tests hold the first worker until
a second range executes. `ParallelIntermediateLimitLeavesNextBranchUnused` checks
early termination without initializing the next sequential branch.

`ParallelDedupTest.*` compares exact output against the established collected
Dedup helper at 1/2/4 workers, including nullable/composite keys, mixed reduced
and retained batches, typed empties, vertex labels, scalar signed zero and edge
property identity. Direct state tests run partitioning concurrently and apply
each batch's independent buckets concurrently in input order. The common bounded
partition scheduler remains covered by the existing build ordering/error tests.

`ParallelGroupByTest.*` compares grouped/ungrouped results at 1/2/4 workers with
the collected helper, covering nullable and composite keys, string extrema,
INT32 result width, empty/all-null groups, partial AVG counts and grouping-key-based result equality. Additional tests cover no-batch EOF, fixed-width overflow across
concurrently built partials and unchanged global handling of DISTINCT/list and
floating MIN/MAX. Eligible cases also compare forced raw, forced partial and
adaptive modes at 1/2/4 partitions with concurrent bucket consumption. A changing
70-batch distribution exercises both mode transitions, NULL identity, AVG counts,
output head and complete group membership. Native key tests distinguish NULL, zero,
negative values and both INT32/INT64 extrema across batches. Floating comparisons use tolerance, not
bitwise identity.
