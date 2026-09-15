"""Warm-cache end-to-end queries. Import, compilation and validation are untimed."""

import argparse
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from neug import Database


def stats(samples):
    ordered = sorted(samples)
    return dict(
        count=len(samples),
        samples_ms=samples,
        median_ms=statistics.median(samples),
        p95_ms=ordered[int((len(ordered) - 1) * 0.95)],
        max_ms=max(samples),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("database")
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--mixed", action="store_true")
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--query")
    args = parser.parse_args()
    n = args.rows
    cardinality = n // 5
    if args.prepare:
        path = Path(args.database).parent
        data, dimension, edges = (
            path / name for name in ("items.csv", "dim.csv", "edges.csv")
        )
        data.write_text(
            "id|k\n" + "".join(f"{i}|{i % cardinality}\n" for i in range(n))
        )
        dimension.write_text("id\n" + "".join(f"{i}\n" for i in range(cardinality)))
        with edges.open("w") as f:
            f.write("src|dst\n")
            for i in range(n):
                f.write(f"{i}|{(i + 1) % n}\n{i}|{(i + 7) % n}\n")
        db = Database(args.database, max_thread_num=4)
        c = db.connect()
        for query in [
            "CREATE NODE TABLE item(id INT64,k INT64,PRIMARY KEY(id))",
            "CREATE NODE TABLE dim(id INT64,PRIMARY KEY(id))",
            "CREATE REL TABLE link(FROM item TO item)",
            f'COPY item FROM "{data}"',
            f'COPY dim FROM "{dimension}"',
            f'COPY link FROM "{edges}"',
        ]:
            c.execute(query)
        c.close()
        db.close()
        return
    total = n * (n - 1) // 2
    keys = sum(i % cardinality for i in range(n))
    queries = {
        "scan_project": (
            "MATCH (n:item) WHERE n.id % 3 = 0 RETURN sum(n.id+n.k)",
            [[sum(i + i % cardinality for i in range(0, n, 3))]],
        ),
        "unfold": (
            "MATCH (n:item) UNWIND [n.id,n.id+1,n.id+2,n.id+3] AS x RETURN sum(x)",
            [[4 * total + 6 * n]],
        ),
        "expand": ("MATCH (a:item)-[:link]->(b:item) RETURN sum(b.id)", [[2 * total]]),
        "join": (
            "MATCH (a:item), (b:dim) WHERE a.k=b.id RETURN sum(a.id+b.id)",
            [[total + keys]],
        ),
        "dedup": (
            "MATCH (n:item) WITH DISTINCT n.k AS k,n.k % 7 AS j RETURN sum(k+j)",
            [[sum(i + i % 7 for i in range(cardinality))]],
        ),
        "group": (
            "MATCH (n:item) WITH n.k AS k,sum(n.id) AS s RETURN sum(k+s)",
            [[total + cardinality * (cardinality - 1) // 2]],
        ),
        "short": ("MATCH (n:item) WHERE n.id=42 RETURN n.k", [[42 % cardinality]]),
    }
    if args.query:
        queries = {args.query: queries[args.query]}
    db = Database(args.database, mode="r", max_thread_num=4)
    c = db.connect()
    plans = {}
    for name, (query, expected) in queries.items():
        result = c.execute("PROFILE " + query, num_threads=4)
        assert list(result) == expected, name
        plans[name] = result.get_profile_metrics()
    results = []
    if not args.mixed:
        for name, (query, expected) in queries.items():
            for workers in (1, 2, 4):
                assert list(c.execute(query, num_threads=workers)) == expected
                samples = []
                for _ in range(args.repeats):
                    start = time.perf_counter()
                    rows = list(c.execute(query, num_threads=workers))
                    samples.append(1000 * (time.perf_counter() - start))
                    assert rows == expected
                results.append(dict(query=name, workers=workers, **stats(samples)))
    else:
        connections = [db.connect() for _ in range(4)]
        gate, done = threading.Barrier(4), threading.Event()
        for i, conn in enumerate(connections):
            q, expected = queries["group" if i == 0 else "short"]
            assert list(conn.execute(q, num_threads=4)) == expected

        def run(i):
            q, expected = queries["group" if i == 0 else "short"]
            samples = []
            gate.wait(timeout=10)
            try:
                while len(samples) < 30 if i == 0 else not done.is_set():
                    start = time.perf_counter()
                    rows = list(connections[i].execute(q, num_threads=4))
                    samples.append(1000 * (time.perf_counter() - start))
                    assert rows == expected
            finally:
                if i == 0:
                    done.set()
            return samples

        with ThreadPoolExecutor(max_workers=4) as executor:
            runs = list(executor.map(run, range(4)))
        results = dict(
            long=stats(runs[0]), short=stats([v for run in runs[1:] for v in run])
        )
        for summary in results.values():
            summary.pop("samples_ms")
        for conn in connections:
            conn.close()
    c.close()
    db.close()
    print(
        "BENCH_JSON "
        + json.dumps(dict(rows=n, mixed=args.mixed, results=results, profiles=plans))
    )


if __name__ == "__main__":
    main()
