# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare V1, previous V2, and current V2 with full Arrow materialization."""

import argparse
import csv
import hashlib
import itertools
import json
import os
import platform
import statistics
import subprocess
from pathlib import Path


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def change(current: float, baseline: float) -> float:
    return 100 * (current / baseline - 1)


def measure(
    case: dict,
    versions: dict[str, tuple[Path, str]],
    files: dict[str, Path],
    cpus: list[int],
    output: Path,
    iterations: int,
    rounds: int,
) -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("VORTEX_", "TOKIO_"))}
    env["RUST_LOG"] = "warn"
    seeds = []
    for seed in [17, 42, 73]:
        medians = {v: [] for v in versions}
        expected_indices = None
        for repeat in range(rounds):
            order = list(versions)[:: 1 if repeat % 2 == 0 else -1]
            for version in order:
                binary, flag = versions[version]
                stem = "-".join(str(v) for v in (*case.values(), seed, repeat, version))
                command = [
                    "taskset",
                    "-c",
                    ",".join(map(str, cpus[: case["cores"]])),
                    str(binary),
                    str(files[case["dataset"]]),
                    str(iterations),
                    "100",
                    case["pattern"],
                    case["mode"],
                    "disk",
                    "decode",
                    str(seed),
                ]
                with (output / f"{stem}.json").open("w") as target, (output / f"{stem}.log").open("w") as log:
                    subprocess.run(
                        command, env=dict(env, VORTEX_SCAN_V2=flag), stdout=target, stderr=log, check=True, timeout=180
                    )
                result = json.loads((output / f"{stem}.json").read_text())
                if result["v2"] != flag or len(result["runs"]) != iterations:
                    raise RuntimeError(f"Wrong scan mode or sample count: {stem}")
                if any(run["instrumented"] for run in result["runs"]):
                    raise RuntimeError(f"Instrumented binary used for timing: {stem}")
                if expected_indices is None:
                    expected_indices = result["indices"]
                if result["indices"] != expected_indices:
                    raise RuntimeError(f"Versions selected different rows: {stem}")
                samples = result["runs"][iterations // 8 :]
                medians[version].append(statistics.median(x["elapsed_ns"] for x in samples) / 1e6)
        times = {v: statistics.median(values) for v, values in medians.items()}
        seeds.append(dict(seed=seed, **times, rounds=medians))
    row = dict(case, **{v + "_ms": statistics.median(s[v] for s in seeds) for v in versions}, seeds=seeds)
    row["vs_old_v2_pct"] = change(row["new_v2_ms"], row["old_v2_ms"])
    if "v1" in versions:
        row["vs_v1_pct"] = change(row["new_v2_ms"], row["v1_ms"])
    print(json.dumps({k: v for k, v in row.items() if k != "seeds"}), flush=True)
    return row


def save(output: Path, metadata: dict, summary: list[dict], confirmations: list[dict]):
    (output / "results.json").write_text(
        json.dumps(dict(metadata, summary=summary, confirmations=confirmations), indent=2) + "\n"
    )
    with (output / "results.csv").open("w") as target:
        writer = csv.DictWriter(target, fieldnames=[k for k in summary[0] if k != "seeds"])
        writer.writeheader()
        writer.writerows({k: v for k, v in row.items() if k != "seeds"} for row in summary)


def report(output: Path, metadata: dict, summary: list[dict], confirmations: list[dict]):
    lines = [
        "# Scan V2 regression comparison",
        "",
        f"Candidate: `{metadata['candidate_commit']}`; previous V2: `{metadata['baseline_commit']}`.",
        "",
        "100 selected rows; full Arrow decode/drop; warm local disk. "
        "Cached reuses file preparation; reopen includes opening/metadata.",
        "Three seeds, two alternating rounds of 64 queries, first 8 discarded. Positive percentages mean slower.",
        "",
        f"Cases above +3% vs previous V2: {sum(r['vs_old_v2_pct'] > 3 for r in summary)}/{len(summary)}; "
        f"above +5% vs V1: {sum(r['vs_v1_pct'] > 5 for r in summary)}/{len(summary)}.",
        "",
        "| Dataset | Cores | Selection | Open | V1 ms | Previous V2 ms | Current V2 ms | vs V1 | vs previous V2 |",
        "|---|---:|---|---|---:|---:|---:|---:|---:|",
    ]
    for r in summary:
        lines.append(
            f"| {r['dataset']} | {r['cores']} | {r['pattern']} | {r['mode']} | {r['v1_ms']:.4f} | "
            f"{r['old_v2_ms']:.4f} | {r['new_v2_ms']:.4f} | {r['vs_v1_pct']:+.1f}% | {r['vs_old_v2_pct']:+.1f}% |"
        )
    if confirmations:
        lines += [
            "",
            "## Longer reruns",
            "",
            "Rerun when the first-pass aggregate exceeds +2%, or any seed exceeds +3%, versus previous V2.",
            "Four alternating rounds of 128 queries, first 16 discarded, same three seeds.",
            "",
            "| Dataset | Cores | Selection | Open | Previous V2 ms | Current V2 ms | Change |",
            "|---|---:|---|---|---:|---:|---:|",
        ]
        for r in confirmations:
            lines.append(
                f"| {r['dataset']} | {r['cores']} | {r['pattern']} | {r['mode']} | "
                f"{r['old_v2_ms']:.4f} | {r['new_v2_ms']:.4f} | {r['vs_old_v2_pct']:+.1f}% |"
            )
    text = "\n".join(lines) + "\n"
    (output / "summary.md").write_text(text)
    if step_summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(step_summary).open("a") as target:
            target.write(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-commit", required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("vortex-bench/data"))
    parser.add_argument("--list-dir", type=Path, default=Path("scan-v2-data"))
    parser.add_argument("--output", type=Path, default=Path("scan-v2-results"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    raw = args.output / "raw"
    raw.mkdir(exist_ok=True)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 30:
        raise RuntimeError(f"This comparison requires 30 available CPUs; found {len(cpus)}")
    files = {
        "taxi": args.data_dir / "taxi/taxi.vortex",
        "feature-vectors": args.data_dir / "random_access/feature_vectors/feature_vectors.vortex",
        "nested-structs": args.data_dir / "random_access/nested_structs/nested_structs.vortex",
        "flat-lists": args.list_dir / "flat-control.vortex",
        "list-1MiB": args.list_dir / "elements-default.vortex",
        "list-64KiB": args.list_dir / "elements-64k.vortex",
    }
    versions = {
        "v1": (args.candidate.resolve(), "0"),
        "old_v2": (args.baseline.resolve(), "1"),
        "new_v2": (args.candidate.resolve(), "1"),
    }
    metadata = dict(
        baseline_commit=args.baseline_commit,
        candidate_commit=args.candidate_commit,
        platform=platform.platform(),
        cpus=cpus[:30],
        method=dict(
            rows=100,
            seeds=[17, 42, 73],
            iterations=64,
            warmups=8,
            rounds=2,
            output="full Arrow decode and drop",
            storage="warm local disk",
            aggregation="median of round medians, then median across seeds",
            confirmation_iterations=128,
            confirmation_warmups=16,
            confirmation_rounds=4,
        ),
        files={k: dict(path=str(p), sha256=digest(p), bytes=p.stat().st_size) for k, p in files.items()},
        versions={k: dict(binary=str(p), sha256=digest(p), scan_v2=f) for k, (p, f) in versions.items()},
    )
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    summary = []
    confirmations = []
    for cores, dataset, pattern, mode in itertools.product(
        [1, 30], files, ["clustered", "uniform"], ["cached", "reopen"]
    ):
        case = dict(dataset=dataset, cores=cores, pattern=pattern, mode=mode)
        summary.append(measure(case, versions, files, cpus, raw, 64, 2))
        save(args.output, metadata, summary, confirmations)
    reruns = args.output / "reruns"
    reruns.mkdir(exist_ok=True)
    for row in summary:
        if row["vs_old_v2_pct"] <= 2 and all(change(s["new_v2"], s["old_v2"]) <= 3 for s in row["seeds"]):
            continue
        case = {k: row[k] for k in ["dataset", "cores", "pattern", "mode"]}
        confirmations.append(
            measure(case, {k: v for k, v in versions.items() if k != "v1"}, files, cpus, reruns, 128, 4)
        )
        save(args.output, metadata, summary, confirmations)
    report(args.output, metadata, summary, confirmations)


if __name__ == "__main__":
    main()
