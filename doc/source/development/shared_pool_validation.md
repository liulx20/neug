# Shared execution pool validation

The first comparison uses `f4d4fcad` (per-query workers) and `98fef647`
(database-shared workers), both with HTTP support disabled and the same Release
build settings. Library and Python binding snapshots are paired; each child
process's loaded library path was checked in dyld output. A/B/B/A is repeated
twice for each workload. Compilation and other tests are stopped during timings.

Each process loads one million rows with `id=0..999999` and `k=id%200000`, then
reopens the database read-only. Loading is excluded. Single-query cases warm once
and time five execute-and-consume calls. The table reports the median of the four
per-process medians; these are local measurements, not statistical confidence
intervals.

| Query | Workers | Per-query pool ms | Shared pool ms | Change |
| --- | ---: | ---: | ---: | ---: |
| Composite Dedup + projection + aggregation | 1 | 195.1 | 201.1 | +3.1% |
| Composite Dedup + projection + aggregation | 2 | 114.8 | 128.1 | +11.6% |
| Composite Dedup + projection + aggregation | 4 | 74.3 | 73.9 | -0.6% |
| GroupBy + projection + aggregation | 1 | 86.8 | 85.7 | -1.2% |
| GroupBy + projection + aggregation | 2 | 46.6 | 49.5 | +6.2% |
| GroupBy + projection + aggregation | 4 | 28.0 | 28.3 | +1.0% |

The earlier four-worker Dedup slowdown is not reproduced. The two-worker cases
show overhead worth investigating when a query uses only part of the shared pool.

For mixed load, one connection runs 20 grouped queries while three connections
repeatedly look up primary key 42 until the long client finishes. Each query
requests four workers. The old configuration may create workers independently
per query; the shared configuration limits execution workers to four total.
Median short-query p95 across runs falls from 0.209 ms to 0.048 ms. Long-query
median latency is approximately 46.7/46.5 ms. These timings include admission and
execution; they are not isolated queue-wait measurements. Task fairness is not
preemptive and does not promise a tail-latency bound.

Raw data: [shared_pool_abba.json](benchmarks/shared_pool_abba.json).
The workload is reproducible with
[shared_pool_query_validation.py](benchmarks/shared_pool_query_validation.py),
using `--mixed` for mixed load. Set `NEUG_BUILD_DIR` to the selected binding
snapshot and `DYLD_LIBRARY_PATH` to its matching library directory on macOS.

## Wake-up adjustment

Previously, Submit notified an idle worker even when the query was already at
its running-task quota. Completion also notified an idle worker although the
completing worker was about to dispatch again. The change notifies on newly
eligible work and when dispatch leaves additional work that can run in parallel.
It keeps query FIFO/round-robin admission, quotas and query-local draining.

This second comparison rebuilds both `98fef647` and the adjustment with HTTP
support enabled. The same A/B/B/A sequence is repeated twice, with no concurrent
compilation or tests. Do not directly combine absolute timings from the HTTP-off
and HTTP-on comparisons.

| Query | Workers | Shared pool before ms | After adjustment ms | Change |
| --- | ---: | ---: | ---: | ---: |
| Composite Dedup | 1 | 214.0 | 206.7 | -3.4% |
| Composite Dedup | 2 | 133.8 | 119.6 | -10.6% |
| Composite Dedup | 4 | 75.1 | 75.6 | +0.7% |
| GroupBy | 1 | 106.3 | 92.2 | -13.2% |
| GroupBy | 2 | 54.3 | 46.6 | -14.2% |
| GroupBy | 4 | 31.3 | 32.3 | +3.0% |

The two-worker cases improve; four-worker cases show no gain. Mixed-load short
query p95 is 0.0665/0.0606 ms, and long-query median is 48.3/46.7 ms. Long-query
p95 is 55.0/64.1 ms, and the median of each run's maximum short-query latency is
94.6/144.9 ms. Thus the change does not establish uniformly better tail latency.
These are medians of four run-level statistics, not pooled percentile estimates.
The results support reducing unnecessary wakes, not a claim of isolated lock or
queue-wait costs. No priority scheduling or latency guarantee was introduced.

Raw data: [shared_pool_wake_abba.json](benchmarks/shared_pool_wake_abba.json).

## Service and correctness validation

HTTP support was configured and built with four compile jobs after initializing
the repository's pinned brpc and leveldb submodules. No dependency revisions
were changed. All 42 `test_db_svc` C++ tests pass with
`MODERN_GRAPH_DATA_DIR=example_dataset/modern_graph`. All 197 execution tests pass;
72 reader/scheduler/morsel/Dedup/GroupBy tests pass 20 repetitions.

The selected Python regression passes 390 tests (34 skipped, 20 deselected).
`test_shared_pool_service.py` starts three explicit read-only transactions,
then concurrently commits five writes and rolls back five writes. Each reader
checks its original snapshot again after all writes finish. A fresh session
checks exactly the committed rows.

The three existing `test_tp_*` cases also pass. Their module hard-codes port
10000, which is occupied on this host. For this validation only, a temporary
copy replaces that fixture with an ephemeral loopback port; test bodies remain
unchanged and the repository's existing fixture is not modified. No unrelated
listener was stopped.
