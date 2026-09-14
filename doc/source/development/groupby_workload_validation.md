# GroupBy workload validation and list result serialization

## Scope and method

This is a deterministic synthetic workload on macOS ARM64, with one million
vertices, not a production trace. It extends the integer-key microbenchmarks
with composite keys, short strings, skew, Join followed by aggregation, and list
outputs. The fixture and exact queries are in
[`group_workloads.py`](../../../tools/benchmarks/group_workloads.py).

Each case runs with 1, 2, and 4 workers. Each implementation/worker/case has two
fresh processes and three warm `Connection.execute` samples per process. Tables
report the median of those six samples. This includes native execution and
construction of the native response; it excludes CSV import, Python iteration,
independent result validation, and the separate PROFILE query. Every row is
checked against the generated data, not merely compared across worker counts.
PROFILE verifies GroupBy and, for the Join case, Join in the executed plan.

Peak memory is process RSS sampled every 2 ms during the first, cold execute,
after import and before Python result validation. That interval also includes
cold planning. The report records baseline RSS and lifetime high-water marks
separately. RSS increase means sampled peak minus baseline, not bytes allocated
by GroupBy. Sampling can miss short peaks, and allocator/library/graph memory
contributes to the measurements. Warm latency and cold peak RSS describe different
executions. Jobs run sequentially in a fixed randomized order, without concurrent
builds or tests. Raw reports retain individual process medians and loader checks.

## Representative queries after primitive serialization

`before` is c58eb3ea; `current` is d0f200c0, which writes primitive output chunks
directly to protobuf arrays. These measurements precede the list change below.

| Query / grouping | Before 1 / 2 / 4 workers, ms | Current 1 / 2 / 4 workers, ms |
| --- | --- | --- |
| Two integer keys, 1,717 groups | 158.3 / 80.4 / 43.6 | 151.5 / 84.9 / 45.7 |
| Short string key, 4,096 groups | 155.7 / 91.2 / 48.3 | 159.2 / 84.3 / 47.7 |
| Unique short string key | 399.4 / 453.4 / 355.3 | 407.0 / 291.1 / 238.4 |
| Unique integer + string key | 768.3 / 754.8 / 659.0 | 799.2 / 608.1 / 522.4 |
| Skew: about 90% in one group | 86.5 / 49.9 / 36.1 | 88.4 / 47.2 / 29.8 |
| Join then 17-group aggregation | 184.9 / 92.0 / 47.2 | 188.3 / 92.0 / 48.3 |
| 17 groups collecting all IDs | 121.9 / 100.8 / 92.2 | 129.4 / 108.4 / 99.0 |

Large primitive outputs benefit; low-output cases do not uniformly improve.
For example, four-worker composite-low and collect-values measurements are
slower in this sample. The unique-string one-worker current process medians
also vary substantially (525.1 and 367.7 ms). These results do not establish a
universal speedup or absence of regression. Composite high-cardinality grouping
still scales weakly, and `collect` still uses the global aggregate kernel.

Four-worker RSS ranges across the two fresh processes, in MiB:

| Case | Before peak | Current peak | Before increase | Current increase |
| --- | --- | --- | --- | --- |
| Composite low | 166.3–175.2 | 163.8–167.5 | 27.5–27.8 | 26.4–27.3 |
| String low | 153.8–175.6 | 161.1–177.6 | 30.1–30.6 | 29.8–30.2 |
| String unique | 467.0–477.8 | 433.4–453.8 | 330.2–331.7 | 291.8–297.7 |
| Composite unique | 424.5–506.8 | 461.5–477.3 | 326.4–360.9 | 319.6–322.9 |
| Skewed | 163.6–172.1 | 176.0–176.0 | 25.4–26.3 | 23.2–26.4 |
| Join then GroupBy | 160.4–178.4 | 153.9–166.1 | 16.4–17.9 | 17.4–17.4 |
| Collect values | 238.6–239.7 | 254.1–257.4 | 97.8–107.9 | 104.9–112.2 |

Some high-cardinality output-copy costs fall, but there is no uniform peak-memory
reduction. Group state, retained output, and response arrays overlap. More
workers can consume more memory than one worker. This work adds neither spilling
nor a byte-based memory budget.

Raw data: [representative matrix](benchmarks/group_workloads_1m.json).

## Million singleton lists exposed a separate bottleneck

`MATCH (n:item) RETURN n.id, collect(n.value)` produces a million one-element
lists. Before this change, it took about 0.29 seconds with one worker but
2.1–2.2 seconds with 2 or 4 workers, both before and after primitive serialization.
The ID array was direct, but the list array still passed through balanced
`ListColumn::union_col` merges. Each merge rebuilt per-row `Value::LIST` objects
and their children. Multiple output chunks therefore added repeated list-copy
work. This is result serialization overhead, not evidence that the aggregation
itself ran in parallel.

Temporary timing around `Sink::sink_results` measured about 1,808–1,828 ms for
four-worker warm serialization (977 chunks). Direct list serialization reduced
that to 17–18 ms. The timing code was removed before the final build and the
uninstrumented comparison below. The one-worker result was already one chunk
and used the original direct serializer.

The serializer now writes list offsets and validity from the input chunks,
selects only referenced child elements, and recursively serializes primitive or
list children. Slices, repeated selections, NULL lists, empty lists, and nested
NULLs preserve the collected wire representation. Other child types are rebuilt
once through a column builder. Other root types retain their existing path.
This removes repeated parent-list construction; it does not eliminate output
buffering, parallelize serialization, or change aggregate scheduling.

Raw data: [original complex output](benchmarks/group_complex_output_1m.json),
[temporary Sink timings](benchmarks/list_sink_phases_1m.json).

## Final uninstrumented list comparison

`before` is the saved d0f200c0 library; `after` is this direct-list serializer.
Both use the same Python binding and fixture. All 24 fresh-process runs passed
the independent result checks.

| Case | Before 1 / 2 / 4 workers, ms | After 1 / 2 / 4 workers, ms |
| --- | --- | --- |
| Million singleton lists | 296.0 / 2,130.1 / 2,114.8 | 297.8 / 279.2 / 270.2 |
| 17 long lists | 119.8 / 101.9 / 94.5 | 118.8 / 111.7 / 93.1 |

Four-worker singleton-list execution improves about 7.8x relative to the old
multi-chunk serialization path. It is only about 1.1x faster than the current
single-worker run, because the aggregate still uses its global kernel. The
17-long-list two-worker result is about 10% slower in this sample; it is not a
case where avoiding a million parent-list rebuilds produces a large benefit.
No general speedup is claimed for low-output-cardinality aggregates.

Four-worker singleton-list peak RSS ranges from 397.2–399.5 MiB before to
356.0–380.0 MiB after; the sampled increases are 260.0–261.8 and 217.9–220.5 MiB.
Two-worker peak RSS instead ranges from 277.6–332.8 MiB before to
314.5–357.5 MiB after. Memory measurements do not support a universal reduction.
The implementation still creates selected child columns and holds the complete
response alongside input chunks.

Raw report: [list comparison](benchmarks/list_chunks_1m.json).

## Validation

The final build includes `execution_test` and `neug_py_bind`, with temporary
phase instrumentation removed. All 186 C++ execution tests pass; 51 scheduler,
Join, GroupBy, Dedup, and morsel tests pass across 20 repetitions. The selected
embedded Python suite reports 368 passed, 28 skipped, and 20 deselected; server,
remote, TP, and async cases are excluded because this build disables HTTP service.

The new list serialization oracle compares complete protobuf bytes against
flattened columns on both heap and protobuf Arena. Cases include selected shared
backing data, repeated/noncontiguous rows, unused NULLs, NULL parents and children,
empty typed chunks, nested lists, lists of arrays, and sparse/repeated aliases.
C++ formatting, Python formatting/lint, and whitespace checks pass.

## Reproduction

Build `execution_test` and `neug_py_bind` using the root build tree. Install
`psutil` in the benchmark Python environment (not a runtime database dependency).
Preserve each build's `libneug` in a separate directory before rebuilding.
From the repository root:

```sh
PYTHONPATH=tools/python_bind python tools/benchmarks/group_workloads.py \
  --rows 1000000 --workers 1 2 4 --repeats 3 --memory-runs 2 \
  --library before=/path/to/before-library-directory \
  --library after=/path/to/after-library-directory \
  --output /path/to/results.json
```

Use `--cases collect_unique collect_values` for the focused list comparison.
Library overrides are checked against dynamic-loader output; hashes are recorded
in the report. The fixture uses short strings; long strings, production data,
and sustained concurrent queries require separate evaluation.

## Composite-key encoding follow-up

The state-kernel diagnostic now supports `group-composite`: integer + short
string keys, COUNT and SUM, 1,024-row input batches. Its phases are executed
serially even with four partitions. `local_ms` includes key encoding, adaptive
local grouping and bucket assignment; `merge_ms` includes partition-table lookup,
retained-key construction and aggregate updates; `finalize_ms` includes ordered
output gathering. These are phase-work measurements, not worker wall times, and
do not include scan, scheduling, or Sink serialization.

The change resolves native INT32/INT64/string columns once per batch, reuses a
batch-local encoding buffer, and encodes native values without temporary Value
objects. Raw batches reserve signature storage and move completed signatures
instead of copying them. NULL bitmap and payload bytes stay identical to the
generic encoder. Unsupported types and column representations use that encoder.
There is no new global state or change to partition assignment or output order.

The encoding oracle covers raw and partial modes, ten key components, INT64_MIN,
NULL and empty strings, embedded zero bytes, long strings, and generic boolean
encoding. Existing grouping and query tests cover the resulting aggregates.

State-kernel medians (three fresh runs, million unique groups), in milliseconds:

| Partitions (serial diagnostic) | Prepare before / after | Update before / after | Finalize before / after |
| --- | --- | --- | --- |
| 1 | 209.4 / 66.1 | 274.2 / 320.1 | 137.9 / 156.7 |
| 4 | 198.9 / 65.5 | 247.2 / 280.4 | 156.0 / 171.8 |

Preparation improves substantially. Update and finalize measured slower in this
non-interleaved diagnostic despite having unchanged code, so its preparation
gain must not be reported as an equal whole-query gain. The interleaved query
comparison below is the end-to-end check. These measurements also show why
encoding alone cannot remove the remaining update and ordered-output costs.

Raw phase data: [before](benchmarks/composite_key_phases_before_1m.json),
[after](benchmarks/composite_key_phases_after_1m.json).
Reproduce with `partition_phases group-composite 1000000 1000000 4`.

Interleaved whole-query comparison: saved 98b73e60 versus this encoding change,
two fresh processes per point and three warm executions per process, with the
same fixture and verification method as above. All 36 processes passed the
independent result checks. Times are medians of six samples, in milliseconds.

| Case | Before 1 / 2 / 4 workers | After 1 / 2 / 4 workers |
| --- | --- | --- |
| Unique integer + string key | 750.0 / 571.8 / 505.4 | 622.3 / 487.3 / 425.1 |
| Two integer keys, 1,717 groups | 149.9 / 80.2 / 44.5 | 119.9 / 60.0 / 33.0 |
| Unique short string key | 393.6 / 301.8 / 239.4 | 361.3 / 252.9 / 217.6 |

The unique-composite four-worker result improves about 16%, while scaling from
one to four workers is still only about 1.46x. Partition-table updates and final
ordered output remain substantial work; this change does not parallelize the
latter or remove group-state materialization.

Four-worker unique-composite peak RSS was 480.4–482.6 MiB before and
434.7–446.3 MiB after (increases 345.3–355.6 versus 313.9–321.3 MiB).
The low-cardinality case had higher absolute peak RSS in the after runs despite
similar increments, and unique-string ranges overlap. These two-process samples
do not establish a general memory reduction.

Raw query report: [encoding comparison](benchmarks/composite_key_encoding_1m.json).

Follow-up validation: final C++/Python binding build passed; 187 C++ tests and
368 selected embedded Python tests passed (28 skipped, 20 deselected). All 52
concurrency-related tests passed in each of 20 repetitions. Changed C++ files
pass clang-format 10 and whitespace checks.

## Partition-local GroupBy result chunks

GroupBy no longer preserves global first-occurrence order of group rows. Each
partition finishes its own key and aggregate columns and publishes a separate
chunk; empty partitions are omitted. Grouped empty input still produces a typed
empty result, and ungrouped aggregation still produces its single empty-input
row. Detecting that case checks all partitions because the empty grouping key
need not hash to partition zero.

Removed state includes the per-group batch/row positions, per-partition sequence
counter, heap merge, global row-reference array, and cross-partition aggregate
output gathering. Group IDs now use a count. The producer still finalizes on one
task after all updates complete; this change does not add parallel finalization.
It also leaves Dedup and the global kernels for collect/FIRST/DISTINCT aggregates
unchanged.

Explicit ORDER BY controls result order. Without it, group row order and which
groups LIMIT selects can differ across worker counts. The ordering of elements
inside an aggregate is a separate property and is not relaxed here.

Tests compare group rows using their complete grouping keys, retaining NULL,
numeric tolerance, and aggregate-content checks. Two Python assertions that
assumed order without ORDER BY now compare unordered rows. A direct state test
verifies four populated partitions return four result chunks, each group appears
once, and TakeOutput transfers ownership once. The per-worker query test also
checks aggregation followed by ORDER BY, SKIP and LIMIT.

Validation: 188 C++ tests and 368 selected embedded Python tests passed (28 skipped,
20 deselected). All 53 concurrency-related tests passed in each of 20 repetitions.
The engine, C++ test binary, Python binding, and diagnostic binary were rebuilt.

Interleaved million-row comparison against d9a1650c (two fresh processes per
point, three warm execute samples each), with all 24 independent result checks
passing. Milliseconds below are medians of the six samples:

| Case | Before 1 / 4 workers | After 1 / 4 workers |
| --- | --- | --- |
| Unique integer + string key | 609.7 / 479.7 | 556.8 / 387.2 |
| Two integer keys, 1,717 groups | 118.5 / 33.5 | 112.6 / 33.1 |
| Unique short string key | 341.4 / 212.9 | 355.1 / 164.3 |

The high-cardinality four-worker cases improve about 19% and 23% in this run.
Low-cardinality four-worker execution changes little; single-worker unique-string
execution is about 4% slower in this sample. Timings vary between processes, so
compare the interleaved variants here rather than treating earlier report values
as a fixed baseline. Finalization remains serial and is not the only cost.

Four-worker peak RSS ranges are 450.7–461.2 to 363.9–407.6 MiB for unique composite
keys, and 415.2–444.8 to 303.7–394.3 MiB for unique string keys. Low-cardinality
peaks instead rise from 155.2–166.1 to 175.3–182.2 MiB. RSS includes the whole
process and two samples do not establish a universal memory improvement.

The serial state diagnostic reports finalization medians of 133.2 ms for one
partition and 90.3 ms for four partitions after this change. These runs were not
interleaved with the prior phase diagnostic, so the query comparison is the
primary performance evidence.

Raw data: [query comparison](benchmarks/group_partition_output_1m.json),
[phase diagnostic](benchmarks/group_partition_output_phases_1m.json).

## Parallel partition finalization

The executor now schedules independent partition finalizers after all build
batches have been consumed. The execution state advertises finalization work;
workers run it through the same queue, timing, exception capture, and failure
handling as other partition tasks. Publication waits for every submitted
finalizer. No threads or task submissions are added inside GroupBy.

GroupBy writes each partition's result into a separate slot owned by that
partition. FinalizeBuild transfers those slots to output chunks and releases
build state. Smaller results call the same FinalizePartition routine inline.
The initial granularity heuristic uses separate jobs only with multiple
partitions and at least 4,096 groups per configured partition on average. It is
not a tuned universal crossover or a byte-memory budget. Join and Dedup request
no new finalization tasks and retain their current finalization behavior.

A scheduler test blocks one finalizer until the other has entered, checks all
build updates are visible, and verifies publication/probe cannot start early.
It tests success, returned errors and thrown exceptions. Failure waits for the
blocked submitted finalizer to exit, prevents probe/publication, and remains
visible on subsequent Next calls. This exercises the queue's existing failure
cancellation/drain path; already-running work is drained, not forcibly interrupted.
The GroupBy state test covers inline and parallel finalization and verifies
complete, unique groups in separate output chunks.

Validation: 189 C++ tests, 368 embedded Python tests (28 skipped, 20 deselected),
and all 54 concurrency-related tests in each of 20 repetitions passed. Both the
engine and Python binding were rebuilt. No temporary timing code is included.

Interleaved comparison against 9b1231c0, million rows, two fresh processes per
point and three warm execute samples per process (median of six samples, ms):

| Case | Before 1 / 4 workers | After 1 / 4 workers |
| --- | --- | --- |
| Unique integer + string key | 643.5 / 398.5 | 626.7 / 391.5 |
| Two integer keys, 1,717 groups | 117.8 / 32.2 | 114.5 / 33.5 |
| Unique short string key | 302.6 / 170.8 | 353.8 / 156.8 |

All 24 result checks passed. Whole-query benefit is modest in this sample:
about 2% for four-worker composite keys and 8% for strings. Low-cardinality
four-worker execution is about 4% slower despite staying inline. The one-worker
string result measured about 17% slower, prompting the focused repeat below;
these data do not establish a universal improvement. Queue finalizers remove
one serial phase, not partition-table update or response serialization costs.

Four-worker unique-composite RSS peaks were 416.9–419.9 MiB before and
387.6–407.6 MiB after. String peaks were 389.0–394.6 and 382.0–407.0 MiB;
there is no general memory-reduction claim.

Raw matrix: [parallel finalization](benchmarks/group_parallel_finalize_1m.json).

The focused one-worker string repeat used three fresh processes per variant and
five warm samples each. Pooled medians were 351.8 ms before and 344.3 ms after;
process medians were 354.5/346.7/339.6 and 358.7/315.4/333.9 ms respectively.
It did not reproduce the earlier large slowdown; it also shows substantial
process variability. Both datasets are retained rather than selecting only the
favorable comparison. All six independent result checks passed.
[Focused repeat](benchmarks/group_parallel_finalize_single_worker_1m.json).

This completes the bounded parallel-finalization change. Remaining grouping,
allocation and response costs are not addressed by this commit.
