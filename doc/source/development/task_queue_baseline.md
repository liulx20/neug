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

## Interpretation and next work

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
work. No wave scheduling or column merging behavior is changed by this commit.
