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
