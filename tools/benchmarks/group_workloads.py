"""GroupBy workload validation and isolated process-RSS sampling.

Requires psutil in the parent process. Run with PYTHONPATH=tools/python_bind.
Library overrides select directories containing libneug via the dynamic loader.
"""

import argparse
import hashlib
import json
import os
import platform
import random
import resource
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

QUERIES = {
    "composite_low": "MATCH (n:item) RETURN n.grp, n.sub, count(*), sum(n.id)",
    "string_low": "MATCH (n:item) RETURN n.category, count(*), sum(n.id)",
    "string_unique": "MATCH (n:item) RETURN n.name, count(*), sum(n.value)",
    "composite_unique": "MATCH (n:item) RETURN n.id, n.name, count(*), sum(n.value)",
    "skewed": "MATCH (n:item) RETURN n.skew, count(*), sum(n.id)",
    "join_group": "MATCH (a:item), (b:item) WHERE a.grp = b.grp AND b.id < 17 "
    "RETURN a.grp, count(*), sum(a.id), sum(b.id)",
    "collect_values": "MATCH (n:item) RETURN n.grp, collect(n.id)",
    "collect_unique": "MATCH (n:item) RETURN n.id, collect(n.value)",
}


def progression(first, step, rows):
    count = max(0, (rows - 1 - first) // step + 1)
    return count, count * (2 * first + (count - 1) * step) // 2


def validate(result, case, rows):
    """Check against the generated data, independently of the execution engine."""
    seen = set()
    ids = bytearray(rows) if case == "collect_values" else None
    residues = {(i % 17, i % 101): i for i in range(1717)}
    digest = 0
    for row in result:
        if case == "composite_low":
            key = tuple(row[:2])
            assert tuple(row[2:]) == progression(residues[key], 1717, rows), row
        elif case == "string_low":
            key = row[0]
            index = int(key.removeprefix("group-"))
            assert key == f"group-{index}" and 0 <= index < 4096
            assert tuple(row[1:]) == progression(index, 4096, rows), row
        elif case in ("string_unique", "composite_unique"):
            name = row[0] if case == "string_unique" else row[1]
            key = int(name.removeprefix("item-"))
            assert 0 <= key < rows and name == f"item-{key:07d}", row
            if case == "composite_unique":
                assert row[0] == key, row
            assert tuple(row[-2:]) == (1, key % 101), row
        elif case == "collect_unique":
            key = row[0]
            assert 0 <= key < rows and row[1] == [key % 101], row
        elif case == "skewed":
            key = row[0]
            if key == 0:
                unique = (rows - 1) // 10
                expected = (
                    rows - unique,
                    rows * (rows - 1) // 2 - 10 * unique * (unique + 1) // 2,
                )
            else:
                assert 0 < key < rows and key % 10 == 0, row
                expected = (1, key)
            assert tuple(row[1:]) == expected, row
        elif case == "join_group":
            key = row[0]
            count, total = progression(key, 17, rows)
            assert 0 <= key < 17 and tuple(row[1:]) == (count, total, key * count)
        else:
            key = row[0]
            assert 0 <= key < 17
            values = row[1]
            assert len(values) == progression(key, 17, rows)[0]
            for value in values:
                assert 0 <= value < rows and value % 17 == key and not ids[value]
                ids[value] = 1
            row = [key, sorted(values)]
        assert key not in seen, key
        seen.add(key)
        digest = (
            digest
            + int.from_bytes(hashlib.sha256(json.dumps(row).encode()).digest(), "big")
        ) % (1 << 256)
    expected_count = {
        "composite_low": min(rows, 1717),
        "string_low": min(rows, 4096),
        "string_unique": rows,
        "composite_unique": rows,
        "skewed": (rows - 1) // 10 + 1,
        "join_group": 17,
        "collect_values": 17,
        "collect_unique": rows,
    }[case]
    assert len(seen) == expected_count, (case, len(seen), expected_count)
    if ids is not None:
        assert all(ids)
    return {"result_rows": len(seen), "row_hash_sum_sha256": f"{digest:064x}"}


def high_water():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def child(args):
    from neug import Database

    db = Database("", max_thread_num=args.worker, checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE item(id INT64, grp INT64, sub INT64, category STRING, name STRING, skew INT64, value INT64, PRIMARY KEY(id))"
        )
        conn.execute(f'COPY item FROM "{args.csv}"')
        print(
            json.dumps({"event": "ready", "process_high_water_before": high_water()}),
            flush=True,
        )
        assert sys.stdin.readline().strip() == "execute"
        start = time.perf_counter_ns()
        result = conn.execute(QUERIES[args.case], num_threads=args.worker)
        cold_ms = (time.perf_counter_ns() - start) / 1e6
        print(
            json.dumps(
                {
                    "event": "executed",
                    "cold_execute_ms": cold_ms,
                    "process_high_water_after": high_water(),
                }
            ),
            flush=True,
        )
        assert sys.stdin.readline().strip() == "validate"
        checked = validate(result, args.case, args.rows)
        del result
        samples = []
        for _ in range(args.repeats):
            start = time.perf_counter_ns()
            result = conn.execute(QUERIES[args.case], num_threads=args.worker)
            samples.append((time.perf_counter_ns() - start) / 1e6)
            assert len(result) == checked["result_rows"]
            del result
        profile = conn.execute(
            "PROFILE " + QUERIES[args.case], num_threads=args.worker
        ).get_profile_metrics()
        names = [op["operator_name"] for op in profile["operators"]]
        assert "GroupByOpr" in names, names
        if args.case == "join_group":
            assert "JoinOpr" in names, names
        print(
            json.dumps(
                {
                    "event": "result",
                    **checked,
                    "samples_ms": samples,
                    "median_ms": statistics.median(samples),
                    "profile": profile,
                }
            ),
            flush=True,
        )
    finally:
        conn.close()
        db.close()


def run_one(args, csv, library, case, workers, index, directory):
    import psutil

    env = dict(os.environ)
    if library:
        variable = (
            "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
        )
        env[variable] = str(library)
        if sys.platform == "darwin":
            env["DYLD_PRINT_LIBRARIES"] = "1"
        else:
            env["LD_DEBUG"] = "libs"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--case",
        case,
        "--worker",
        str(workers),
        "--rows",
        str(args.rows),
        "--repeats",
        str(args.repeats),
        "--csv",
        str(csv),
    ]
    stderr = (args.log_dir or directory) / f"child-{index}.log"
    with stderr.open("w") as log:
        process = subprocess.Popen(
            command,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            bufsize=1,
        )
        stop = threading.Event()
        monitor = None
        try:

            def receive(event):
                line = process.stdout.readline()
                if not line:
                    raise RuntimeError(stderr.read_text()[-8000:])
                result = json.loads(line)
                assert result.pop("event") == event, result
                return result

            ready = receive("ready")
            if library:
                binary = library / (
                    "libneug.dylib" if sys.platform == "darwin" else "libneug.so"
                )
                loaded = stderr.read_text()
                assert str(binary) in loaded, "Library override not reported by loader"
                ready["library_override_verified"] = True
            observed = psutil.Process(process.pid)
            baseline = observed.memory_info().rss
            peak = [baseline]
            count = [0]

            def sample():
                while not stop.is_set():
                    try:
                        peak[0] = max(peak[0], observed.memory_info().rss)
                        count[0] += 1
                    except psutil.NoSuchProcess:
                        break
                    stop.wait(0.002)

            monitor = threading.Thread(target=sample)
            monitor.start()
            process.stdin.write("execute\n")
            process.stdin.flush()
            executed = receive("executed")
            peak[0] = max(peak[0], observed.memory_info().rss)
            stop.set()
            monitor.join()
            process.stdin.write("validate\n")
            process.stdin.flush()
            result = receive("result")
            process.wait(timeout=60)
            if process.returncode:
                raise RuntimeError(stderr.read_text()[-8000:])
            return {
                "case": case,
                "workers": workers,
                **ready,
                **executed,
                **result,
                "rss_before_execute": baseline,
                "sampled_peak_rss": peak[0],
                "sampled_rss_increase": peak[0] - baseline,
                "rss_samples": count[0],
            }
        finally:
            stop.set()
            if monitor:
                monitor.join()
            if process.poll() is None:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--cases", nargs="+", choices=QUERIES, default=list(QUERIES))
    parser.add_argument(
        "--library",
        action="append",
        default=[],
        help="NAME=DIRECTORY; omit for current build",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--log-dir", type=Path, help="Preserve child stderr logs")
    parser.add_argument("--memory-runs", type=int, default=1)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--case", choices=QUERIES, help=argparse.SUPPRESS)
    parser.add_argument("--worker", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--csv", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (
        args.rows < 17
        or args.repeats < 1
        or args.memory_runs < 1
        or min(args.workers) < 1
    ):
        parser.error("rows >= 17 and positive counts required")
    if args.child:
        child(args)
        return
    if not args.output:
        parser.error("--output is required")
    if args.log_dir:
        args.log_dir.mkdir(parents=True, exist_ok=True)
    variants = (
        {
            name: Path(path).resolve()
            for name, path in (entry.split("=", 1) for entry in args.library)
        }
        if args.library
        else {"current": None}
    )
    report = {
        "platform": platform.platform(),
        "rows": args.rows,
        "repeats": args.repeats,
        "queries": QUERIES,
        "method": "Fresh process per case/worker/variant/memory run. Sample RSS every 2ms during first execute only, including cold planning and native response, excluding Python validation and PROFILE. Record loaded-process RSS separately from lifetime high-water mark. Warm timings follow correctness validation, with prior result released outside each timing. RSS is process memory, not operator allocations; sampling may miss short peaks.",
        "libraries": {},
        "runs": [],
    }
    for name, path in variants.items():
        if path:
            binary = path / (
                "libneug.dylib" if sys.platform == "darwin" else "libneug.so"
            )
            report["libraries"][name] = {
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()
            }
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        csv = directory / "input.csv"
        with csv.open("w") as output:
            output.write("id|grp|sub|category|name|skew|value\n")
            for i in range(args.rows):
                output.write(
                    f"{i}|{i % 17}|{i % 101}|group-{i % 4096}|item-{i:07d}|{i if i % 10 == 0 else 0}|{i % 101}\n"
                )
        jobs = [
            (name, case, workers, repeat)
            for name in variants
            for case in args.cases
            for workers in args.workers
            for repeat in range(args.memory_runs)
        ]
        random.Random(0).shuffle(jobs)
        signatures = {}
        for index, (name, case, workers, repeat) in enumerate(jobs):
            run = run_one(args, csv, variants[name], case, workers, index, directory)
            signature = (run["result_rows"], run["row_hash_sum_sha256"])
            assert signature == signatures.setdefault(case, signature), (
                name,
                case,
                workers,
            )
            report["runs"].append({"variant": name, "memory_run": repeat, **run})
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(
                name,
                case,
                workers,
                round(run["median_ms"], 3),
                "ms",
                round(run["sampled_peak_rss"] / 2**20, 1),
                "MiB peak",
                flush=True,
            )


if __name__ == "__main__":
    main()
