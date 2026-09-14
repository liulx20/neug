# Embedded task queue latency baseline

The normal query path now forwards `num_threads` to the task executor. This
baseline measures that path, including a new pool per query, execution, result
materialization and native serialization. Python row iteration is excluded.
It compares worker counts within this implementation, not against main or the
old stream executor. It is a local baseline, not a throughput or production-load
claim.

## Reproduce

From the repository root, with the Python binding built and available:

```sh
PYTHONPATH=tools/python_bind python tools/benchmarks/task_queue.py --rows 100000 --repeats 7 --output /tmp/task_queue_100k.json
PYTHONPATH=tools/python_bind python tools/benchmarks/task_queue.py --rows 1000000 --repeats 5 --output /tmp/task_queue_1m.json
```

The script creates an isolated temporary database from deterministic CSV data.
Each worker count gets a warm-up execution of the same cached query; sorted
result values are checked for equality across counts. Timed runs are interleaved
in a reproducible shuffled order and also check row counts. No PROFILE runs are
included in timings. Input creation and loading are excluded. Raw samples,
queries and result checksums are stored alongside this document.

## Measurements

Measured on 2026-09-13, macOS ARM64, 10 physical/logical CPUs, Release build,
`NEUG_NATIVE_ARCH=OFF`. All tests used `max_thread_num=4` and explicit 1/2/4
worker overrides. Other benchmarks/builds were not run concurrently.

Median milliseconds per `execute()` call:

| Rows | Query | 1 worker | 2 workers | 4 workers | 1-to-4 speedup |
| --- | --- | ---: | ---: | ---: | ---: |
| 100k | filter_project | 12.498 | 9.686 | 8.066 | 1.55x |
| 100k | hash_join | 34.194 | 27.717 | 24.694 | 1.38x |
| 100k | group_sum | 24.608 | 23.125 | 22.648 | 1.09x |
| 100k | limit_10 | 0.205 | 0.322 | 0.347 | 0.59x |
| 1m | filter_project | 752.238 | 718.833 | 699.892 | 1.07x |
| 1m | hash_join | 2272.726 | 2192.011 | 2164.780 | 1.05x |
| 1m | group_sum | 2132.984 | 2149.169 | 2153.476 | 0.99x |
| 1m | limit_10 | 0.191 | 0.272 | 0.313 | 0.61x |

Raw data: [100k rows](benchmarks/task_queue_100k.json),
[1m rows](benchmarks/task_queue_1m.json).

## Original baseline interpretation and next work

At 100k rows, filtering/projection and the non-primary-key equality Join benefit
from more workers. At 1m rows the gains shrink sharply; the global aggregate
shows no gain, while short LIMIT queries pay more dispatch/read-ahead overhead.
Increasing worker count alone does not address the dominant serial costs.

Code inspection identifies repeated copying at two boundaries:

- `Sink::sink_results` merges each output column sequentially across chunks.
  `ValueColumn::union_col` copies both its accumulated left side and new right
  side. With similarly sized chunks, the accumulated rows are copied repeatedly.
- `make_global_kernel::Process` similarly merges each input into all prior rows
  before aggregation or another global operator executes.

This gives a concrete explanation consistent with the superlinear size scaling,
but the baseline does not isolate the fraction of time spent in each component.
A focused profile and a before/after benchmark should validate a change to these
merge paths before attributing the slowdown to task scheduling.

Next priorities are to eliminate repeated full-prefix copying, then replace
wave-wide publication with bounded incremental completion. Parallel aggregation,
shared pools/admission control and byte-based memory budgets remain separate
work. The original baseline commit changed no wave scheduling or column merging
behavior; the follow-up below addresses the latter.

## Follow-up: balanced batch merging

On the same machine and build settings, the same benchmark script was rerun
after replacing prefix merges with ordered, balanced batch accumulation.
The original implementation is commit `7a04177f`. Repeats and query workloads
are unchanged (7 at 100k rows, 5 at 1m rows); no builds or tests ran concurrently.
Every query's result row count and SHA-256 checksum match the original baseline
as well as the other worker counts. These local timings are not a cross-machine
performance guarantee.

Median milliseconds per call:

| Rows | Query | Before: 1 worker | After: 1 worker | Before: 4 workers | After: 4 workers | 4-worker improvement |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 100k | filter_project | 12.498 | 8.083 | 8.066 | 3.604 | 2.24x |
| 100k | hash_join | 34.194 | 18.122 | 24.694 | 8.270 | 2.99x |
| 100k | group_sum | 24.608 | 7.817 | 22.648 | 5.362 | 4.22x |
| 100k | limit_10 | 0.205 | 0.201 | 0.347 | 0.326 | 1.06x |
| 1m | filter_project | 752.238 | 85.342 | 699.892 | 40.881 | 17.12x |
| 1m | hash_join | 2272.726 | 203.067 | 2164.780 | 101.946 | 21.23x |
| 1m | group_sum | 2132.984 | 99.495 | 2153.476 | 72.272 | 29.80x |
| 1m | limit_10 | 0.191 | 0.194 | 0.313 | 0.312 | 1.01x |

Raw follow-up data: [100k rows](benchmarks/task_queue_balanced_100k.json),
[1m rows](benchmarks/task_queue_balanced_1m.json).

The changes cover result-column concatenation, global input accumulation,
Join build-input merging and Context flattening. They preserve concatenation
order and use the existing column merge implementations. Copy work changes from
repeated full-prefix copying to O(N log K) for N rows and K batches; the required
materialized result still occupies memory. This is not a streaming aggregate
or spill implementation.

At 1m rows, the new 4-worker filtering/projection and Join runs are about twice
as fast as the new 1-worker runs. Global aggregation gains less from additional
workers. Short LIMIT latency remains approximately unchanged by the merge fix;
its parallel dispatch/read-ahead overhead and wave-wide publication still need
separate work.

Validation includes 157 C++ and 368 Python tests, including an uneven-batch copy
budget, row order, empty batches, nulls, sparse aliases, head identity and nested
column serialization compared with a sequential reference. The Python suite
covers per-query workers, queries, import/export, transactions and local
connections; HTTP-server tests are excluded because that build option is off.

## Follow-up: incremental ordered range delivery

The next change removes wave-wide publication. Each completed morsel publishes
its result independently through a completion event. The next range in input
order can reach downstream operators while later tasks are still running. A
slow earlier range still holds back later rows; ordering is preserved.

W worker lanes share W in-flight/completed-result slots. A delivered range frees
a slot that can be refilled while downstream demand is active. The current
range's output is separate, so at most W+1 source ranges are retained. Paused
consumers do not replenish slots from completion callbacks. Cancellation stops
allocation and is checked between reader chunks; running storage calls are
still drained. This does not introduce a byte-based memory budget.

Using the unchanged script, machine and 7/5 repetition counts, the local
end-to-end median milliseconds were:

| Rows | Query | Balanced merges: 4 workers | Incremental delivery: 4 workers |
| --- | --- | ---: | ---: |
| 100k | filter_project | 3.604 | 3.270 |
| 100k | hash_join | 8.270 | 8.098 |
| 100k | group_sum | 5.362 | 4.858 |
| 100k | limit_10 | 0.326 | 0.309 |
| 1m | filter_project | 40.881 | 38.570 |
| 1m | hash_join | 101.946 | 99.352 |
| 1m | group_sum | 72.272 | 67.925 |
| 1m | limit_10 | 0.312 | 0.280 |

Raw samples: [100k rows](benchmarks/task_queue_incremental_100k.json),
[1m rows](benchmarks/task_queue_incremental_1m.json). Query result checksums and
row counts match the previous baseline. Builds and tests did not run concurrently
with these measurements. Small latency differences should not be treated as a
general performance guarantee; the benchmark measures full-result execution,
not first-row latency under skew.

Deterministic tests hold a later range blocked while the first passes through a
downstream operator, and verify that the freed lane starts another range. A
second test blocks the first range and checks that later work cannot exceed the
slot budget. Limit and failure tests hold another range blocked and check that
EOF/error waits for draining without successfully finalizing the source.

## Hash-partitioned Join follow-up

The Join build now routes keys to independent hash buckets. Build tasks no longer
need a serial table merge, and probe workers route each lookup to its bucket.
The same 1m-row benchmark (5 repetitions, 1/2/4 workers) produced these medians:

| Join implementation | 1 worker | 2 workers | 4 workers |
| --- | ---: | ---: | ---: |
| Incremental morsels, range tables then merge | 202.878 ms | 133.792 ms | 99.352 ms |
| Incremental morsels, hash buckets | 198.273 ms | 130.906 ms | 97.697 ms |

These small differences are not strong evidence of a throughput improvement.
The structural change removes table merging; it does not remove collected build
rows or serial partition preparation. Skewed keys still concentrate build work.
The single-worker path skips partition buffering. All query row counts and
checksums match the preceding incremental-morsel baseline. Runs used the same
settings above, with no concurrent builds or tests.

Raw samples: [hash partitions, 1m rows](benchmarks/task_queue_hash_partitions_1m.json).

## Incremental Join build follow-up

Build input now enters bounded batch slots, is partitioned concurrently and
appended to per-bucket tables in input order. Tables retain original chunks and
row references; the executor no longer collects and concatenates the entire
right input before building. Finalization waits for EOF and all append tasks.

The same 1m-row, five-repetition benchmark produced these Join medians:

| Join implementation | 1 worker | 2 workers | 4 workers |
| --- | ---: | ---: | ---: |
| Collected build, hash buckets | 198.273 ms | 130.906 ms | 97.697 ms |
| Incremental build, retained chunks | 217.172 ms | 138.743 ms | 105.898 ms |

This is a regression of about 8% at four workers, not a performance win. The
query restricts `b.id < 17`: its right-side build retains only 17 matching rows,
so it is not a benchmark of large-build scaling. Additional batch dispatch and
matched-row gathering are plausible overheads, but their contributions have not
been isolated. Removing an identity output permutation did not show a measurable
end-to-end improvement in these runs. A larger build-side benchmark and profiles
are needed before claiming a throughput or memory improvement.

Checksums and row counts for every query match the preceding hash-partition
baseline. Settings and timing scope are unchanged, and builds/tests were not
running concurrently. The pending-build limit counts batches, not bytes; all
build chunks and hash-table entries remain resident, with no spill support.

Raw samples: [incremental Join, 1m rows](benchmarks/task_queue_incremental_join_1m.json).

## Empty-batch and single-chunk gather follow-up

Redundant empty Join batches now skip per-bucket append jobs, while the scheduler
still advances bucket sequence numbers. Partition jobs still run; one typed
empty input is retained when needed for metadata. When every match references
the same build chunk, gathering selects its rows directly without per-chunk
buckets or a restoring permutation.

With the original two-column dataset and unchanged queries (1m rows, five
repetitions), Join medians were:

| Version | 1 worker | 2 workers | 4 workers |
| --- | ---: | ---: | ---: |
| Incremental build before these changes | 217.172 ms | 138.743 ms | 105.898 ms |
| Empty-batch/gather changes | 205.337 ms | 131.970 ms | 98.876 ms |

At four workers this run is about 6.6% faster than the preceding incremental
build, and close to the earlier collected-build measurement of 97.697 ms. These
are local observations rather than guaranteed speedups. The two optimizations
were measured together; their individual contributions were not isolated.
All original query checksums and row counts match the preceding baseline.

Raw samples: [original dataset](benchmarks/task_queue_join_fast_original_1m.json).

### Large and skewed build inputs

Pass `--join-shapes` to include three additional joins. This adds stored `shifted`
and `hot` columns to the generated dataset; existing queries remain unchanged.
`shifted = id + 1` supplies mostly unique build keys, `grp = id % 17` supplies
17 repeated keys, and `hot = 0` supplies a single hotspot. Every new query produces
at most one output row per build row, avoiding quadratic result sizes.

```sh
PYTHONPATH=tools/python_bind python tools/benchmarks/task_queue.py --rows 1000000 --repeats 5 --join-shapes --output /tmp/task_queue_join_shapes.json
```

| Join shape | Build rows | 1 worker | 2 workers | 4 workers |
| --- | ---: | ---: | ---: | ---: |
| Mostly unique keys | 1,000,000 | 602.766 ms | 377.090 ms | 250.313 ms |
| 17 repeated keys | 1,000,000 | 317.945 ms | 244.110 ms | 209.894 ms |
| One repeated key | 1,000,000 | 323.657 ms | 250.842 ms | 207.608 ms |

The mostly unique case scales by about 2.4x from one to four workers. The two
skewed cases scale less: keys concentrate build work in fewer buckets, and
matching probe rows can produce large output batches. These measurements compare
worker counts in the new implementation, not against the collected-build version.
Memory peaks, byte budgets, and concurrent-query throughput were not measured.

Each worker count checks result equality before timing. After timed runs, a
separate PROFILE execution records operator metrics and validates right-side
build cardinality and Join output cardinality. PROFILE is outside all reported
timings; summed worker durations are not query wall time. The report contains
this plan evidence along with the timings.

During workload validation, inline arithmetic predicates (`a.id + 1 = b.id`
and `a.id = b.id % 1`) compiled into a Cartesian Join followed by a filter. At
1,000 rows, PROFILE showed 1,000,000 Join rows before filtering. These are not
valid hash-join scaling workloads for this compiler, so the benchmark uses
precomputed columns instead. The compiler itself was not changed here.

Raw samples and PROFILE evidence:
[join shapes](benchmarks/task_queue_join_fast_paths_1m.json).

## Parallel intermediate consumption

Use `--intermediate` to measure projection after a global aggregation or sort.
Each generated node has a distinct `id`; the aggregation emits one row per node,
and the sort emits all nodes in descending ID order. Separate PROFILE runs record
GroupBy/OrderBy before the final projections. Timings still exclude PROFILE.

```sh
PYTHONPATH=tools/python_bind python tools/benchmarks/task_queue.py --rows 1000000 --repeats 5 --intermediate --output /tmp/task_queue_intermediate.json
```

On the same machine, with five repetitions and 1/2/4 workers:

| Rows | Query | 1 worker | 2 workers | 4 workers |
| --- | --- | ---: | ---: | ---: |
| 100k | Aggregation then projection | 14.664 ms | 14.417 ms | 13.146 ms |
| 100k | Sort then projection | 60.011 ms | 59.045 ms | 57.085 ms |
| 1m | Aggregation then projection | 177.875 ms | 196.859 ms | 180.677 ms |
| 1m | Sort then projection | 699.318 ms | 715.539 ms | 694.311 ms |

The 100k cases show modest scaling; the 1m cases show no consistent end-to-end
improvement (the two-worker aggregation is slower than one worker). This change
only parallelizes consumption after the global operator. Aggregation and sorting
themselves remain serial; range slicing also copies data and incurs scheduling
cost. These runs compare worker counts in this version, not old/new execution at
the same worker count. Concurrency and ordering are established by deterministic
C++ tests, not inferred from small timing differences.

The unchanged 1m small-build Join measured 98.322 ms at four workers versus
98.876 ms in the preceding run; this is not evidence of a material speedup or
regression. All original query checksums and row counts match that baseline.
New queries also check result equality across worker counts. Builds and tests
were not run concurrently with benchmarks.

Raw timings and PROFILE evidence: [100k](benchmarks/task_queue_intermediate_100k.json),
[1m](benchmarks/task_queue_intermediate_1m.json).

## Partitioned Dedup

Use `--dedup` for DISTINCT over 17 repeated scalar keys, one million unique
scalar keys, and composite `(grp, id % 101)` keys (1,717 distinct combinations).
All three workloads include a separate PROFILE run that verifies `DedupOpr` is
present; PROFILE is excluded from timings.

```sh
PYTHONPATH=tools/python_bind python tools/benchmarks/task_queue.py --rows 1000000 --repeats 5 --dedup --output /tmp/task_queue_dedup.json
```

The following medians use the same machine and five repetitions. The collected
comparison temporarily restores the pre-change `DedupOpr` implementation on the
same source/build tree; all other executor code and benchmark data are identical.
Builds and regression tests were not running during these measurements.

| DISTINCT keys | Implementation | 1 worker | 2 workers | 4 workers |
| --- | --- | ---: | ---: | ---: |
| 17 scalar keys | Collected | 121.697 ms | 109.003 ms | 111.968 ms |
| 17 scalar keys | Partitioned | 93.709 ms | 48.382 ms | 29.847 ms |
| 1m unique scalar keys | Collected | 63.703 ms | 48.706 ms | 48.590 ms |
| 1m unique scalar keys | Partitioned | 67.702 ms | 51.191 ms | 50.109 ms |
| 1,717 composite keys | Collected | 189.240 ms | 154.934 ms | 138.372 ms |
| 1,717 composite keys | Partitioned | 213.158 ms | 115.078 ms | 64.200 ms |

At four workers, repeated scalar keys improve by about 3.8x and composite keys
by about 2.2x against collected Dedup. The unique-scalar case is about 3% slower
(50.109 vs 48.590 ms), so this does not establish a speedup for low-duplication
inputs. Single-worker composite keys also regress (213.158 vs 189.240 ms).
Extra local hashing, task scheduling and restoring order can exceed the saved
work when little reduction is possible.

An initial implementation that hashed every scalar row measured 128.627 ms with
four workers on unique keys. The final implementation samples each scalar batch
and skips pre-reduction when more than half of up to 64 sampled keys are unique.
This keeps the same task graph and final correctness helper, and avoids most of
that regression. The threshold is heuristic; skewed batches or unrepresentative
prefixes can still choose poorly. Composite keys always use encoded pre-reduction.

The final normalization and candidate merge remain serial. Floating-point and
other single-column types without proven encoding/native equality equivalence
retain all candidates; those cases have no parallel hash reduction benefit.
Memory peaks, spill and concurrent-query throughput were not measured.

All query row counts and checksums match the collected comparison, and each run
checks equality across worker counts. C++ tests separately compare exact row
order, nulls, signed zero, edge property identity and mixed reduced/retained
batches against the existing Dedup helper.

Raw timings and PROFILE evidence: [partitioned](benchmarks/task_queue_dedup_1m.json),
[collected comparison](benchmarks/task_queue_dedup_collected_1m.json).

## Partial GroupBy aggregation

Use `--aggregates` to measure multiple ordinary aggregates, nearly unique groups
and a hot group. The generated input has one million rows with unique `id` and
`grp = id % 17`. The added queries are recorded verbatim in the JSON reports.
The partitioned report includes separate PROFILE evidence verifying `GroupByOpr`
and its output cardinality. PROFILE is outside the timed execute calls.

```sh
PYTHONPATH=tools/python_bind python tools/benchmarks/task_queue.py --rows 1000000 --repeats 5 --aggregates --output /tmp/task_queue_aggregate.json
```

The collected comparison temporarily restores only the pre-change `GroupByOpr`
on the same source/build tree. The compiler and other executor code are
unchanged. All queries use a warm plan cache and five interleaved repetitions
per worker count. No builds or tests ran concurrently with these timings.
These are whole `conn.execute()` medians, including scan, projections, scheduling,
aggregation, materialization and native serialization, not isolated GroupBy time.

| Groups / functions | Implementation | 1 worker | 2 workers | 4 workers |
| --- | --- | ---: | ---: | ---: |
| 17 / SUM | Collected | 97.319 ms | 75.025 ms | 68.501 ms |
| 17 / SUM | Partial | 65.954 ms | 32.384 ms | 18.958 ms |
| 17 / COUNT, SUM, MIN, MAX, AVG | Collected | 97.294 ms | 77.406 ms | 71.336 ms |
| 17 / COUNT, SUM, MIN, MAX, AVG | Partial | 90.760 ms | 45.495 ms | 25.655 ms |
| 1m / COUNT, SUM | Collected | 153.155 ms | 222.371 ms | 212.308 ms |
| 1m / COUNT, SUM | Partial | 365.858 ms | 326.580 ms | 275.310 ms |
| 1 / COUNT, SUM, AVG | Collected | 110.700 ms | 80.978 ms | 68.040 ms |
| 1 / COUNT, SUM, AVG | Partial | 90.722 ms | 44.869 ms | 23.219 ms |

At four workers, SUM over 17 groups improves by about 3.6x, the five-aggregate
query by about 2.8x, and the hot-group query by about 2.9x. Batch-local reduction
shrinks the data being exchanged and merged, including for the hot partition.

The unique-group workload regresses by about 30% at four workers (275.310 vs
212.308 ms), and by about 2.4x at one worker (365.858 vs 153.155 ms). Local and
partition group tables perform extra work without reducing row count, and final
result construction remains serial. Integer lookup in local/partition tables,
preallocated output builders and merging already ordered partition sequences
reduce overhead but do not eliminate this regression. This implementation has
no cardinality-based strategy selection yet; the results are not a claim of
universal aggregate acceleration.

All result row counts and checksums match the collected comparison and across
worker counts for these datasets. Their integer sums are exactly representable
in double where AVG is used; this does not establish bitwise equivalence for
arbitrary floating-point inputs. Partial SUM/AVG changes addition association,
and ill-conditioned or non-finite inputs can differ materially. The execution
document describes numerical semantics and unsupported aggregate combinations.

The benchmark does not measure memory peaks, spill, concurrent queries or
compiler cost-model changes. The collected timing report predates the new
aggregate PROFILE collection; plan evidence is in the partitioned report.

Raw reports: [partial](benchmarks/task_queue_aggregate_1m.json),
[collected](benchmarks/task_queue_aggregate_collected_1m.json).

## Reducing Dedup and GroupBy overhead

This revision uses native optional-integer keys throughout scalar integer Dedup,
reuses encoding buffers for other keys, removes the second global Dedup pass for
composite keys, and skips the final merge/reshuffle for a single partition.
GroupBy allocates only aggregate fields that are used. In particular, SUM does
not need a count array because its empty/all-null result is zero. COUNT plus
SUM(INT64) now uses 16 rather than 48 bytes per group for aggregate-array payloads;
this is not a claim that total process memory falls by the same factor.

Whole-query execute medians, one million rows and five repetitions, compared
with the previously recorded partitioned implementations:

| Query | Previous 1 worker | Current 1 worker | Previous 4 workers | Current 4 workers |
| --- | ---: | ---: | ---: | ---: |
| Dedup: 17 keys | 93.709 ms | 41.402 ms | 29.847 ms | 13.677 ms |
| Dedup: 1m unique keys | 67.702 ms | 63.587 ms | 50.109 ms | 50.241 ms |
| Dedup: 1,717 composite keys | 213.158 ms | 169.471 ms | 64.200 ms | 48.574 ms |
| GroupBy: 1m groups, COUNT/SUM | 365.858 ms | 267.880 ms | 275.310 ms | 253.039 ms |

Dedup's repeated-integer and composite cases improve substantially. Unique
scalar keys remain close to the prior implementation. High-cardinality GroupBy
improves, especially with one worker, but is still slower than the original
collected helper (153.155/212.308 ms at one/four workers). This does not resolve
all of its regression: duplicated local/partition grouping still remains.
These measurements combine several changes; individual speedup contributions
are not isolated. All result row counts/checksums match the previous reports.

### Is integer sampling still useful?

A separate experimental build disabled scalar integer sampling entirely while
retaining native integer local/partition hashing. It was not retained in the
implementation. At four workers, unique-key Dedup measured 89.566 ms without
sampling, versus about 50 ms with it. Native keys reduce overhead, but do not
make an unconditional hash build plus final ordering competitive for this case.
The final code therefore retains the heuristic with native integer samples.
It can still misclassify nonrepresentative prefixes; it is not a cost model.

### Direct state-phase diagnostic

With BUILD_TEST enabled, build the optional diagnostic target (excluded from the
default build and CTest):

```sh
cmake --build build --target partition_phases -j4
build/tests/execution/partition_phases dedup 1000000 17
build/tests/execution/partition_phases composite 1000000 1000000
build/tests/execution/partition_phases group 1000000 1000000
```

The tool generates two INT64 columns: `i % cardinality` and `i % 101`, in
1,024-row chunks. Dedup uses the first column, composite Dedup uses both, and
GroupBy computes COUNT(*) and SUM(second column) grouped by the first column.
It directly runs one partition on one thread, timing local processing, partition
merge and finalization separately. Input generation and state construction are
outside the timers; there is no storage scan, task queue, result serialization
or worker overlap. Time between measured calls, including some destruction,
is excluded. These numbers must not be read as multithreaded end-to-end timings.

Five fresh-process repetitions per case produced these medians:

| Operation / key cardinality | Local | Merge | Finalize | Peak process RSS |
| --- | ---: | ---: | ---: | ---: |
| dedup / 17 | 13.511 ms | 0.204 ms | 0.245 ms | 29.0 MiB |
| dedup / 1000000 | 1.985 ms | 0.063 ms | 33.113 ms | 49.2 MiB |
| composite / 17 | 93.481 ms | 15.142 ms | 0.466 ms | 30.1 MiB |
| composite / 1000000 | 93.519 ms | 59.836 ms | 59.572 ms | 145.1 MiB |
| group / 17 | 23.797 ms | 0.301 ms | 0.004 ms | 28.7 MiB |
| group / 1000000 | 80.636 ms | 114.181 ms | 9.288 ms | 166.9 MiB |

The high-cardinality GroupBy diagnostic spends approximately 81 ms locally and
114 ms merging, versus 9 ms finalizing. Its main remaining cost is grouping and
partial-state handling, not final output construction in this one-partition
experiment. Multiple partitions add ordering and column merging, so this does
not measure their exact contribution at four workers. Unique scalar Dedup skips
hash reduction and spends most of its measured time in final normalization;
composite-key Dedup still pays encoded-key processing costs.

Peak RSS is the maximum over these five process runs and includes libraries,
generated input, allocator retention and output; it is neither operator-only
memory nor a before/after memory comparison. No builds/tests or other benchmark
cases ran concurrently. Correctness is covered separately by regression tests,
including integer extrema/NULL identity, exact row order and cleared output head.

Raw reports: [end-to-end](benchmarks/task_queue_reduction_cost_1m.json),
[no-sampling experiment](benchmarks/task_queue_native_no_sampling_1m.json),
[state phases and process memory](benchmarks/partition_phases_1m.json).


## GroupBy direct row routing and adaptive local reduction

High-cardinality batches can now route original rows directly into partition
aggregate state, avoiding a local group table and partial aggregate arrays.
Low-cardinality batches retain local reduction. Both forms feed the same
partition state and bounded task executor. INT32/INT64 partition routing uses
native integer hashing consistently in both modes.

### Choosing between input modes

The direct-state diagnostic now accepts `group-raw`, `group-partial`, and `group`
(the production adaptive policy). Five fresh-process runs of each forced mode,
on the same implementation, gave these median sums of local + merge + finalize
time for one million rows, 1,024-row batches, one thread and one partition:

| Key cardinality | Direct rows | Local partials |
| --- | ---: | ---: |
| 1 | 35.946 ms | 24.038 ms |
| 17 | 34.536 ms | 24.663 ms |
| 128 | 34.755 ms | 33.722 ms |
| 256 | 34.669 ms | 43.199 ms |
| 512 | 34.441 ms | 61.805 ms |
| 768 | 35.696 ms | 80.168 ms |
| 1024 | 34.613 ms | 101.469 ms |
| 1000000 | 162.177 ms | 251.716 ms |

These are isolated kernel timings, not parallel query execution times. They
explain why always routing raw rows is unsuitable: local reduction is valuable
for repeated keys. The adaptive policy initially reduces a complete batch,
then selects direct routing when that batch contains at least one distinct group
per four rows. Every 32nd batch probes with local reduction again. This threshold
is a heuristic based on the integer COUNT/SUM experiment; it is not established
as optimal for strings, composite keys, other aggregates or different machines.
Strategy updates can arrive in worker completion order; correctness does not
depend on which input mode wins, but floating-point association can vary.

### Whole-query comparison

Execute medians below use one million rows, five repetitions, warm plans, and
1/2/4 workers. Timing includes scan, projection, scheduling, aggregation,
materialization and native serialization; it excludes Python row iteration,
import and the separate PROFILE verification. No builds, tests or other benchmark
cases ran concurrently.

| Query | Workers | Previous partial | Forced direct | Adaptive |
| --- | ---: | ---: | ---: | ---: |
| Group SUM, 17 groups | 1 | 61.514 ms | 77.566 ms | 64.070 ms |
| Group SUM, 17 groups | 2 | 31.116 ms | 42.118 ms | 32.173 ms |
| Group SUM, 17 groups | 4 | 17.918 ms | 36.085 ms | 17.940 ms |
| COUNT/SUM/MIN/MAX/AVG, 17 groups | 1 | 86.761 ms | 101.062 ms | 89.950 ms |
| COUNT/SUM/MIN/MAX/AVG, 17 groups | 2 | 43.545 ms | 59.023 ms | 45.972 ms |
| COUNT/SUM/MIN/MAX/AVG, 17 groups | 4 | 24.074 ms | 40.592 ms | 25.090 ms |
| COUNT/SUM, 1m groups | 1 | 267.880 ms | 221.422 ms | 233.573 ms |
| COUNT/SUM, 1m groups | 2 | 291.442 ms | 239.678 ms | 247.501 ms |
| COUNT/SUM, 1m groups | 4 | 253.039 ms | 222.742 ms | 230.872 ms |
| COUNT/SUM/AVG, one hot group | 1 | 88.764 ms | 103.211 ms | 90.563 ms |
| COUNT/SUM/AVG, one hot group | 2 | 43.582 ms | 51.897 ms | 44.538 ms |
| COUNT/SUM/AVG, one hot group | 4 | 23.301 ms | 39.919 ms | 23.457 ms |

Adaptive routing reduces the recorded unique-group query time by about 13% at
one worker and 9% at four workers versus the previous partial implementation.
It avoids the large low-cardinality regression of unconditional direct routing.
Some repeated-key cases still measure 2–6% slower than the previous report;
these sequential before/after measurements do not isolate overhead from run
variation. The forced-direct experiment predates removal of a per-bucket row
selection copy and the output-head consistency fix, so its comparison with the
final adaptive run also does not isolate policy overhead precisely.

High-cardinality aggregation remains slower than the original collected helper
(153.155/212.308 ms at one/four workers). This change reduces repeated grouping
work but does not eliminate the remaining scheduling, partition-state and output
costs. There is still no spill or byte budget, and direct batches retain source
columns until all their buckets finish.

All result row counts and SHA256 checksums match the previous and forced-direct
reports. PROFILE separately confirms GroupBy execution. Validation passed 183
C++ tests, 368 Python tests (28 skipped, 20 deselected), and 50 concurrency-related
tests repeated 20 times. The GroupBy oracle checks forced partial/direct/adaptive
modes, NULLs, AVG counts, output head and group order, including distributions
that switch strategies in both directions. Floating comparisons use tolerance.

Raw reports: [mode comparison](benchmarks/group_input_modes_1m.json),
[forced direct experiment](benchmarks/task_queue_group_raw_1m.json),
[adaptive execution](benchmarks/task_queue_group_adaptive_1m.json).


## GroupBy output gathering and native integer key access

This comparison starts from `10ac52fe`. All variants use the same deterministic
million-row input, machine, compiler/build settings and query text. The collected
control temporarily changes only GroupBy's pipeline behavior to `kGlobal`, using
its existing collected helper; that temporary change is not retained. Worker
counts are 1 and 4, with five warm-plan repetitions shuffled within each process.
The reported execute interval includes scheduling, scan/projection, aggregation,
materialization and native serialization, and excludes Python result iteration,
import and the separate PROFILE run. Builds and tests do not overlap timings.

### What the diagnostic found

`partition_phases` now accepts an optional partition count:

```sh
build/tests/execution/partition_phases group 1000000 1000000 4
```

It still runs all kernels on one thread, with no scheduler. `merge_ms` is the sum
of all partition calls, not four-worker elapsed time. It isolates kernel work
and final output construction; it cannot measure queue waiting or worker overlap.
Five fresh-process runs per configuration gave these medians for unique keys:

| Partitions | Version | Partition input | Group lookup/update | Finalize |
| --- | --- | ---: | ---: | ---: |
| 1 | Before | 14.447 ms | 130.956 ms | 9.132 ms |
| 1 | Direct output only | 14.984 ms | 148.655 ms | 6.249 ms |
| 1 | Typed access | 14.115 ms | 68.460 ms | 5.722 ms |
| 4 | Before | 14.619 ms | 126.416 ms | 39.646 ms |
| 4 | Direct output only | 14.988 ms | 137.840 ms | 23.058 ms |
| 4 | Typed access | 14.577 ms | 65.360 ms | 22.721 ms |

The first change removes concatenation/reshuffling of intermediate aggregate
result columns. A single partition skips order merging entirely; multiple
partitions compute final partition/row references once and write aggregate
result columns directly from their states. Key columns are gathered once in
that order. First-appearance ordering and cleared output head remain unchanged.

A separate macOS sampling run over four million unique rows found generic Value
construction among the costs inside partition accumulation, alongside hash-table
insertion and resizing. The second change reads native INT32/INT64 ValueColumns
and writes retained integer keys through typed accessors. Other column storage
continues using its virtual accessors. This avoids per-row generic Value
construction at those two points without changing hash/equality or NULL handling.
The sampling run is diagnostic, not part of the timing table.

The intermediate build also shows why independent whole-run timing differences
cannot all be attributed to the edited function: its grouping time moved despite
only finalization code changing. The typed-access implementation substantially reduces
both measured grouping work and output work, but precise contribution percentages
are not inferred by subtracting these separate runs.

### Whole-query results from the initial controlled build sequence

| Query | Workers | Before | Collected control | Direct output only | Typed access |
| --- | ---: | ---: | ---: | ---: | ---: |
| SUM, 17 groups | 1 | 65.560 ms | 99.918 ms | 64.114 ms | 66.190 ms |
| SUM, 17 groups | 4 | 18.835 ms | 68.793 ms | 18.669 ms | 18.688 ms |
| COUNT/SUM/MIN/MAX/AVG, 17 groups | 1 | 92.342 ms | 98.526 ms | 90.066 ms | 93.621 ms |
| COUNT/SUM/MIN/MAX/AVG, 17 groups | 4 | 25.235 ms | 72.020 ms | 25.605 ms | 26.836 ms |
| COUNT/SUM, 1m groups | 1 | 244.557 ms | 156.753 ms | 223.942 ms | 142.588 ms |
| COUNT/SUM, 1m groups | 4 | 227.594 ms | 218.498 ms | 211.071 ms | 154.172 ms |
| COUNT/SUM/AVG, one hot group | 1 | 91.734 ms | 112.336 ms | 96.999 ms | 96.240 ms |
| COUNT/SUM/AVG, one hot group | 4 | 23.390 ms | 68.168 ms | 23.879 ms | 23.372 ms |

The unique-group case improves at both worker counts and now beats this run's
collected control. Four workers still do not outperform one worker for this
particular all-unique query. The output remains serial, and this experiment
does not separately attribute scheduler waiting, memory bandwidth and worker
imbalance. It does not establish that changing task granularity would help.
Low-cardinality runs showed a few percent variation, so an additional interleaved
comparison follows rather than treating every difference as a regression or gain.

All query row counts/checksums match across the four builds. Validation passed
184 C++ tests, 368 Python tests (28 skipped, 20 deselected), and 51 concurrency
related tests repeated 20 times. The added key oracle covers native INT32/INT64
NULL, zero, negative values and extrema across batches, for forced partial/raw
and adaptive execution. Existing tests cover output ordering, sparse aliases,
empty/all-null aggregates and nullable composite/string keys. Floating aggregate
comparisons retain their existing tolerance and association limitations.

Raw reports: [before execute](benchmarks/task_queue_group_control_before_1m.json),
[collected execute](benchmarks/task_queue_group_control_collected_1m.json),
[direct output execute](benchmarks/task_queue_group_control_direct_output_1m.json),
[typed-access execute](benchmarks/task_queue_group_control_after_1m.json),
[before phases](benchmarks/group_control_phases_before_1m.json),
[direct output phases](benchmarks/group_control_phases_direct_output_1m.json),
[typed-access phases](benchmarks/group_control_phases_after_1m.json).


### Final interleaved verification

The first interleaved experiment retained native type dispatch for reduced
batches too. Its repeated-key results showed small slowdowns. The final code
restricts that dispatch to raw batches; local-partial inputs retain the existing
accessors, and INT64 raw batches also skip an unnecessary failed INT32 cast.

Two copies of the shared library were then used in four sequential processes:
before, after, before, after. The dynamic loader's selected library path was
verified separately for both directories; the report records library SHA256s.
Each process imported identical generated data and ran seven warm repetitions
per worker count, shuffled within each query. No builds/tests overlapped these
measurements. Pooled medians of the 14 samples per version/worker are:

| Query | Workers | Before | Final |
| --- | ---: | ---: | ---: |
| group_sum | 1 | 70.443 ms | 65.293 ms |
| group_sum | 4 | 20.378 ms | 18.576 ms |
| aggregate_repeated | 1 | 94.196 ms | 91.002 ms |
| aggregate_repeated | 4 | 26.326 ms | 25.212 ms |
| aggregate_unique | 1 | 244.822 ms | 145.529 ms |
| aggregate_unique | 4 | 229.485 ms | 159.720 ms |
| aggregate_hot | 1 | 90.733 ms | 93.221 ms |
| aggregate_hot | 4 | 23.669 ms | 24.269 ms |

The all-unique improvement persists across both process pairs. Repeated-key
SUM and the five-aggregate case do not show the earlier slowdown in this final
comparison. The hot-group case varies across processes, especially with one
worker; the small remaining difference should not be claimed as a speedup or
as proof of identical performance. Per-process medians and every sample are
retained, rather than hiding this variation behind a single number.

The final code passed the same 184 C++ / 368 Python checks and 51 concurrency
tests repeated 20 times after the dispatch restriction. All checksums and row
counts also match the collected control. Grouping state, adaptive thresholds,
task scheduling and supported aggregate eligibility are unchanged.

Raw reports: [initial interleaved experiment](benchmarks/task_queue_group_interleaved_1m.json),
[final interleaved verification](benchmarks/task_queue_group_final_interleaved_1m.json).
