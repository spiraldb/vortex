# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Confirm the clustered-list CI flag with longer rounds and both V1 controls."""

from __future__ import annotations

import argparse
import importlib.util
import itertools
import json
import os
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["baseline", "candidate"]:
        parser.add_argument(f"--{name}", type=Path, required=True)
        parser.add_argument(f"--{name}-commit", required=True)
    parser.add_argument("--list-dir", type=Path, default=Path("scan-v2-data"))
    parser.add_argument("--output", type=Path, default=Path("scan-v2-results/list-control"))
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("comparison", Path(__file__).with_name("scan-v2-regression.py"))
    bench = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bench)
    cpus = sorted(os.sched_getaffinity(0))[:30]
    if len(cpus) != 30:
        raise RuntimeError("List-control comparison requires 30 available CPUs")
    args.output.mkdir(parents=True, exist_ok=True)
    raw, checks = (args.output / name for name in ["raw", "correctness"])
    raw.mkdir(exist_ok=True)
    checks.mkdir(exist_ok=True)
    files = {
        "list-64KiB": (args.list_dir / "elements-64k.vortex").resolve(),
        "list-1MiB": (args.list_dir / "elements-default.vortex").resolve(),
    }
    versions = {
        "old_v1": (args.baseline.resolve(), "0"),
        "v1": (args.candidate.resolve(), "0"),
        "old_v2": (args.baseline.resolve(), "1"),
        "new_v2": (args.candidate.resolve(), "1"),
    }
    metadata = dict(
        baseline_commit=args.baseline_commit,
        candidate_commit=args.candidate_commit,
        cpus=cpus,
        method=dict(iterations=4096, rounds=4, seeds=[17, 42, 73], warmups=512, output="full Arrow decode/drop"),
        files={k: dict(path=str(p), sha256=bench.digest(p), bytes=p.stat().st_size) for k, p in files.items()},
        versions={k: dict(binary=str(p), sha256=bench.digest(p), scan_v2=f) for k, (p, f) in versions.items()},
    )
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("VORTEX_", "TOKIO_"))}
    env.update(VORTEX_SCAN_V2="1", RUST_LOG="warn")
    commands = []
    selections = [(0, "uniform"), (1, "uniform"), (100, "uniform"), (100, "clustered"), (4096, "uniform"), (0, "all")]
    for cores, (name, file), (rows, selector) in itertools.product([1, 30], files.items(), selections):
        command = [
            "taskset",
            "-c",
            ",".join(map(str, cpus[:cores])),
            str(args.candidate.resolve()),
            str(file),
            "1",
            str(rows),
            selector,
            "cached",
            "disk",
            "check",
            "42",
        ]
        result = subprocess.run(command, env=env, capture_output=True, text=True, check=True, timeout=180)
        if not json.loads(result.stdout)["correct"]:
            raise RuntimeError(f"Wrong selected output: {name}, {rows}, {selector}")
        (checks / f"{name}-{cores}-{rows}-{selector}.json").write_text(result.stdout)
        commands.append(command)
    (checks / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    print(f"PASS: {len(commands)} real-file V1 full-scan-plus-take comparisons", flush=True)
    summary = []
    for cores, dataset, mode in itertools.product([1, 30], files, ["cached", "reopen"]):
        case = dict(dataset=dataset, cores=cores, pattern="clustered", mode=mode)
        summary.append(bench.measure(case, versions, files, cpus, raw, 4096, 4))
        bench.save(args.output, metadata, summary, [])
    line = (
        f"List controls: {sum(r['vs_old_v2_pct'] > 3 for r in summary)}/{len(summary)} cases above +3% versus parent."
    )
    print(line, flush=True)
    if step_summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(step_summary).open("a") as target:
            target.write("\n" + line + "\n")


if __name__ == "__main__":
    main()
