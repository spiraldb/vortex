# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare large list rows after checking selected output against a V1 full scan."""

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
    for name in ["baseline", "candidate", "generator"]:
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--intermediate", type=Path)
    parser.add_argument("--intermediate-commit")
    parser.add_argument("--confirm-singletons", action="store_true")
    parser.add_argument("--baseline-commit", required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("scan-v2-large-data"))
    parser.add_argument("--output", type=Path, default=Path("scan-v2-results/large-lists"))
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("comparison", Path(__file__).with_name("scan-v2-regression.py"))
    bench = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bench)
    cpus = sorted(os.sched_getaffinity(0))[:30]
    if len(cpus) != 30:
        raise RuntimeError("Large-list comparison requires 30 available CPUs")
    args.data_dir.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    raw, checks, reruns = (args.output / name for name in ["raw", "correctness", "reruns"])
    for directory in [raw, checks, reruns]:
        directory.mkdir(exist_ok=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("VORTEX_", "TOKIO_"))}
    env["RUST_LOG"] = "warn"
    files = {}
    fixture_metadata = {}
    for distribution, shape, target in itertools.product(
        ["balanced", "skewed"], ["primitive", "nested"], [65536, 1048576]
    ):
        name = f"{distribution}-{shape}-{target}"
        file = (args.data_dir / f"{name}.vortex").resolve()
        command = [str(args.generator.resolve()), str(file), distribution, shape, str(target)]
        result = subprocess.run(command, env=env, capture_output=True, text=True, check=True)
        (args.output / f"{name}-layout.json").write_text(result.stdout)
        generated = json.loads(result.stdout)
        files[name] = file
        fixture_metadata[name] = dict(
            path=str(file), sha256=bench.digest(file), bytes=file.stat().st_size, command=command
        )
        fixture_metadata[name]["rows"] = generated["rows"]
    versions = {
        "old_v1": (args.baseline.resolve(), "0"),
        "v1": (args.candidate.resolve(), "0"),
        "old_v2": (args.baseline.resolve(), "1"),
        "new_v2": (args.candidate.resolve(), "1"),
    }
    if args.intermediate:
        versions["mask_v1"] = (args.intermediate.resolve(), "0")
        versions["mask_v2"] = (args.intermediate.resolve(), "1")
    iterations, rounds = (4096, 4) if args.confirm_singletons else (32, 2)
    metadata = dict(
        baseline_commit=args.baseline_commit,
        intermediate_commit=args.intermediate_commit,
        candidate_commit=args.candidate_commit,
        files=fixture_metadata,
        cpus=cpus,
        versions={
            k: dict(binary=str(binary), sha256=bench.digest(binary), scan_v2=flag)
            for k, (binary, flag) in versions.items()
        },
        method=dict(
            iterations=iterations,
            rounds=rounds,
            seeds=[17, 42, 73],
            warmups=iterations // 8,
            output="full Arrow decode/drop",
            confirm_singletons=args.confirm_singletons,
            confirmation_iterations=128,
            confirmation_rounds=4,
        ),
    )
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    selections = [
        (0, "uniform"),
        (1, "indices:16"),
        (1, "indices:17"),
        (1, "indices:19"),
        (4, "indices:16,17,19,32"),
        (100, "uniform"),
        (100, "clustered"),
        (0, "all"),
    ]
    check_commands = []
    for cores, (name, file), (count, selector) in itertools.product([1, 30], files.items(), selections):
        command = [
            "taskset",
            "-c",
            ",".join(map(str, cpus[:cores])),
            str(args.candidate.resolve()),
            str(file),
            "1",
            str(count),
            selector,
            "cached",
            "disk",
            "check",
            "42",
        ]
        result = subprocess.run(
            command, env=dict(env, VORTEX_SCAN_V2="1"), capture_output=True, text=True, check=True, timeout=180
        )
        if not json.loads(result.stdout)["correct"]:
            raise RuntimeError(f"Wrong selected output: {name}, {selector}")
        # Artifact uploads reject colons, including those in explicit row selectors.
        filename_selector = selector.replace(":", "-")
        (checks / f"{name}-{cores}-{count}-{filename_selector}.json").write_text(result.stdout)
        check_commands.append(command)
    (checks / "commands.json").write_text(json.dumps(check_commands, indent=2) + "\n")
    print(f"PASS: {len(check_commands)} real-file V1 full-scan-plus-take comparisons", flush=True)
    summary, confirmations = [], []

    def save():
        (args.output / "results.json").write_text(
            json.dumps(dict(metadata, summary=summary, confirmations=confirmations), indent=2) + "\n"
        )

    cases = (
        [
            (1, "skewed-primitive-65536", "row16", "cached"),
            (1, "balanced-primitive-65536", "row16", "cached"),
            (1, "balanced-nested-1048576", "row16", "reopen"),
            (1, "balanced-nested-65536", "row16", "reopen"),
            (1, "balanced-nested-1048576", "row16", "cached"),
            (30, "skewed-nested-65536", "row16", "cached"),
            (30, "skewed-nested-1048576", "row16", "cached"),
        ]
        if args.confirm_singletons
        else itertools.product([1, 30], files, ["row16", "clustered", "uniform"], ["cached", "reopen"])
    )
    for cores, dataset, pattern, mode in cases:
        case = dict(
            dataset=dataset,
            cores=cores,
            pattern=pattern,
            mode=mode,
            rows=1 if pattern == "row16" else 100,
            selector="indices:16" if pattern == "row16" else pattern,
        )
        row = bench.measure(case, versions, files, cpus, raw, iterations, rounds)
        if args.intermediate:
            row["mask_vs_baseline_pct"] = bench.change(row["mask_v2_ms"], row["old_v2_ms"])
            row["null_span_vs_mask_pct"] = bench.change(row["new_v2_ms"], row["mask_v2_ms"])
        summary.append(row)
        save()
    for row in summary:
        if args.confirm_singletons:
            break
        if row["vs_old_v2_pct"] <= 2 and all(
            bench.change(seed["new_v2"], seed["old_v2"]) <= 3 for seed in row["seeds"]
        ):
            continue
        case = {k: row[k] for k in ["dataset", "cores", "pattern", "mode", "rows", "selector"]}
        confirmations.append(bench.measure(case, versions, files, cpus, reruns, 128, 4))
        save()

    def key(row: dict) -> tuple:
        return tuple(row[k] for k in ["dataset", "cores", "pattern", "mode"])

    effective = {key(row): row for row in summary}
    effective.update({key(row): row for row in confirmations})
    (args.output / "effective.json").write_text(json.dumps(list(effective.values()), indent=2) + "\n")
    line = (
        f"Large lists: {sum(row['vs_old_v2_pct'] > 3 for row in effective.values())}/{len(effective)} "
        f"cases above +3% versus parent after {len(confirmations)} longer reruns."
    )
    print(line, flush=True)
    if step_summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(step_summary).open("a") as target:
            target.write("\n" + line + "\n")


if __name__ == "__main__":
    main()
