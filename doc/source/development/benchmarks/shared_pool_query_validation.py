"""Single-query and mixed-query validation; loading is outside timed sections."""

import argparse
import json
import statistics
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from neug import Database

p = argparse.ArgumentParser()
p.add_argument("--mixed", action="store_true")
a = p.parse_args()
queries = {
    "dedup": "MATCH (n:item) WITH DISTINCT n.k AS k, n.k % 7 AS j RETURN sum(k+j)",
    "group": "MATCH (n:item) WITH n.k AS k, sum(n.id) AS s RETURN sum(k+s)",
    "short": "MATCH (n:item) WHERE n.id=42 RETURN n.k",
}
expected = {"dedup": [[20000499994]], "group": [[519999400000]], "short": [[42]]}
with tempfile.TemporaryDirectory() as d:
    data = Path(d) / "data.csv"
    data.write_text("id|k\n" + "".join(f"{i}|{i % 200000}\n" for i in range(1000000)))
    dbpath = str(Path(d) / "db")
    db = Database(dbpath, max_thread_num=4)
    c = db.connect()
    c.execute("CREATE NODE TABLE item(id INT64,k INT64,PRIMARY KEY(id))")
    c.execute(f'COPY item FROM "{data}"')
    c.close()
    db.close()
    db = Database(dbpath, mode="r", max_thread_num=4)
    if not a.mixed:
        c = db.connect()
        results = []
        for name in ["dedup", "group"]:
            for w in [1, 2, 4]:
                assert list(c.execute(queries[name], num_threads=w)) == expected[name]
                timings = []
                for _ in range(5):
                    t = time.perf_counter()
                    r = list(c.execute(queries[name], num_threads=w))
                    timings.append((time.perf_counter() - t) * 1000)
                    assert r == expected[name]
                results.append(
                    dict(
                        query=name,
                        workers=w,
                        samples_ms=timings,
                        median_ms=statistics.median(timings),
                    )
                )
        c.close()
    else:
        cs = [db.connect() for _ in range(4)]
        gate = threading.Barrier(4)
        done = threading.Event()
        for i, c in enumerate(cs):
            assert (
                list(c.execute(queries["group" if i == 0 else "short"], num_threads=4))
                == expected["group" if i == 0 else "short"]
            )

        def run(i):
            name = "group" if i == 0 else "short"
            ts = []
            gate.wait(timeout=10)
            try:
                while len(ts) < 20 if i == 0 else not done.is_set():
                    t = time.perf_counter()
                    r = list(cs[i].execute(queries[name], num_threads=4))
                    ts.append((time.perf_counter() - t) * 1000)
                    assert r == expected[name]
            finally:
                if i == 0:
                    done.set()
            return ts

        with ThreadPoolExecutor(max_workers=4) as pool:
            t = time.perf_counter()
            runs = list(pool.map(run, range(4)))
            elapsed = time.perf_counter() - t

        def stats(ts):
            ts = sorted(ts)
            return dict(
                count=len(ts),
                median_ms=statistics.median(ts),
                p95_ms=ts[int((len(ts) - 1) * 0.95)],
                max_ms=max(ts),
            )

        results = dict(
            elapsed_s=elapsed,
            long=stats(runs[0]),
            short=stats([t for row in runs[1:] for t in row]),
        )
        for c in cs:
            c.close()
    db.close()
    print("BENCH_JSON " + json.dumps(dict(mixed=a.mixed, results=results)), flush=True)
