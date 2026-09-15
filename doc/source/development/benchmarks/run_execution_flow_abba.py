"""Run paired builds in fresh processes; verify loaded libneug on macOS."""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("database", type=Path)
    p.add_argument("before", type=Path)
    p.add_argument("after", type=Path)
    p.add_argument("output", type=Path)
    p.add_argument("--native", type=Path)
    p.add_argument("--rows", type=int, default=1000000)
    p.add_argument("--query")
    p.add_argument("--repeats", type=int, default=5)
    a = p.parse_args()
    root = Path(__file__).resolve().parents[4]
    output = dict(platform=platform.platform(), rows=a.rows, libraries={}, runs=[])
    for name in ("before", "after"):
        lib = getattr(a, name).resolve() / "libneug.dylib"
        output["libraries"][name] = dict(
            path=str(lib), sha256=hashlib.sha256(lib.read_bytes()).hexdigest()
        )
    a.output.parent.mkdir(parents=True, exist_ok=True)
    modes = (
        ["single"] if a.query else ["single", "mixed"] + (["pk"] if a.native else [])
    )
    for mode in modes:
        for order, version in enumerate(["before", "after", "after", "before"] * 2):
            location = getattr(a, version).resolve()
            env = os.environ.copy()
            env.update(
                NEUG_BUILD_DIR=str(location),
                DYLD_LIBRARY_PATH=str(location),
                DYLD_PRINT_LIBRARIES="1",
                PYTHONPATH=str(root / "tools/python_bind"),
            )
            if mode == "pk":
                cmd = [str(a.native.resolve()), str(a.database.resolve()), str(a.rows)]
            else:
                cmd = [
                    sys.executable,
                    str(Path(__file__).with_name("execution_flow_validation.py")),
                    str(a.database.resolve()),
                    "--rows",
                    str(a.rows),
                ]
                cmd.extend(["--repeats", str(a.repeats)])
                if a.query:
                    cmd.extend(["--query", a.query])
                if mode == "mixed":
                    cmd.append("--mixed")
            proc = subprocess.run(
                cmd,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=300,
            )
            log = a.output.with_name(f"{a.output.stem}-{mode}-{order}-{version}.log")
            log.write_text(proc.stdout)
            if proc.returncode:
                raise RuntimeError(f"{cmd}: {proc.returncode}\n{proc.stdout[-4000:]}")
            assert (
                str(location / "libneug.dylib") in proc.stdout
            ), "library loading not verified"
            result = json.loads(
                next(
                    line[11:]
                    for line in proc.stdout.splitlines()
                    if line.startswith("BENCH_JSON ")
                )
            )
            output["runs"].append(
                dict(mode=mode, order=order, version=version, data=result)
            )
            a.output.write_text(json.dumps(output, indent=2) + "\n")
            print(mode, order, version, flush=True)


if __name__ == "__main__":
    main()
