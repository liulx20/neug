"""Reproducible embedded query latency baseline (warm plan cache, no PROFILE)."""

import argparse
import hashlib
import json
import os
import platform
import random
import statistics
import tempfile
import time
from pathlib import Path

from neug import Database


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--join-shapes",
        action="store_true",
        help="include large and skewed join inputs with plan evidence",
    )
    args = parser.parse_args()
    if args.rows < 17 or args.repeats < 1 or min(args.workers) < 1:
        parser.error("rows >= 17, repeats >= 1 and positive workers are required")
    if max(args.workers) > (os.cpu_count() or 1):
        parser.error("requested workers exceed available logical CPUs")
    queries = {
        "filter_project": "MATCH (n:parallel_item) WHERE n.id % 3 = 0 "
        "RETURN n.id, n.id * 7 + n.grp",
        "hash_join": "MATCH (a:parallel_item), (b:parallel_item) "
        "WHERE a.grp = b.grp AND b.id < 17 RETURN a.id, b.id",
        "group_sum": "MATCH (n:parallel_item) RETURN n.grp, sum(n.id)",
        "limit_10": "MATCH (n:parallel_item) RETURN n.id LIMIT 10",
    }
    if args.join_shapes:
        queries.update(
            {
                "hash_join_large_build": "MATCH (a:parallel_item), (b:parallel_item) "
                "WHERE a.id = b.shifted RETURN a.id, b.id",
                "hash_join_skewed_build": "MATCH (a:parallel_item), (b:parallel_item) "
                "WHERE a.id = b.grp RETURN a.id, b.id",
                "hash_join_single_hot_key": "MATCH (a:parallel_item), (b:parallel_item) "
                "WHERE a.id = b.hot RETURN a.id, b.id",
            }
        )
    report = {
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "rows": args.rows,
        "repeats": args.repeats,
        "timing": "execute only, includes task pool, materialization and native serialization; "
        "excludes Python row iteration, warm plan cache, no PROFILE",
        "queries": {},
    }
    with tempfile.TemporaryDirectory() as directory:
        csv = Path(directory) / "input.csv"
        if args.join_shapes:
            csv.write_text(
                "id|grp|shifted|hot\n"
                + "".join(f"{i}|{i % 17}|{i + 1}|0\n" for i in range(args.rows))
            )
        else:
            csv.write_text(
                "id|grp\n" + "".join(f"{i}|{i % 17}\n" for i in range(args.rows))
            )
        db = Database("", max_thread_num=max(args.workers), checkpoint_on_close=False)
        conn = db.connect()
        try:
            extra = ", shifted INT64, hot INT64" if args.join_shapes else ""
            conn.execute(
                f"CREATE NODE TABLE parallel_item(id INT64, grp INT64{extra}, PRIMARY KEY(id))"
            )
            conn.execute(f'COPY parallel_item FROM "{csv}"')
            for name, query in queries.items():
                signatures = {}
                timings = {workers: [] for workers in args.workers}
                for workers in args.workers:
                    rows = sorted(list(conn.execute(query, num_threads=workers)))
                    signatures[workers] = hashlib.sha256(
                        json.dumps(rows).encode()
                    ).hexdigest()
                assert len(set(signatures.values())) == 1, (name, signatures)
                order = args.workers * args.repeats
                random.Random(0).shuffle(order)
                for workers in order:
                    start = time.perf_counter_ns()
                    result = conn.execute(query, num_threads=workers)
                    elapsed = (time.perf_counter_ns() - start) / 1e6
                    timings[workers].append(elapsed)
                    assert len(result) == len(rows)
                report["queries"][name] = {
                    "query": query,
                    "result_rows": len(rows),
                    "sha256": next(iter(signatures.values())),
                    "workers": {
                        workers: {
                            "median_ms": statistics.median(values),
                            "samples_ms": values,
                        }
                        for workers, values in timings.items()
                    },
                }
                if args.join_shapes and name.startswith("hash_join"):
                    profiled = conn.execute(
                        "PROFILE " + query, num_threads=max(args.workers)
                    )
                    report["queries"][name]["profile"] = profiled.get_profile_metrics()
                    metrics = report["queries"][name]["profile"]
                    joins = [
                        op
                        for op in metrics["operators"]
                        if op["operator_name"] == "JoinOpr"
                    ]
                    assert len(joins) == 1, (name, metrics)
                    assert joins[0]["output_rows"] == len(rows), (name, metrics)
                    by_id = {op["operator_id"]: op for op in metrics["operators"]}
                    build_rows = by_id[joins[0]["child_ids"][1]]["output_rows"]
                    assert build_rows == (17 if name == "hash_join" else args.rows)
                    report["queries"][name]["profile_build_rows"] = build_rows
                print(
                    name,
                    {w: round(statistics.median(v), 3) for w, v in timings.items()},
                    flush=True,
                )
        finally:
            conn.close()
            db.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
