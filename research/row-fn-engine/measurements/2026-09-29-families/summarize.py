# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Summarize matched Arrow and RowFn process medians at all four row counts."""

import argparse
import csv
import json
from pathlib import Path
import re
from statistics import median


script_dir = Path(__file__).resolve().parent
durations = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s+(ps|ns|µs|ms|s)")
branch = re.compile(r"[├╰]─ ")
units_to_us = {"ps": 0.000001, "ns": 0.001, "µs": 1, "ms": 1000, "s": 1_000_000}
sizes = {0, 64, 1024, 16384}


def parse(path):
    results = {}
    stack = []
    for line in path.read_text().splitlines():
        marker = branch.search(line)
        if marker is None:
            continue
        depth = marker.start() // 3
        cells = line[marker.end():].split("│")
        heading = durations.search(cells[0])
        name = cells[0][:heading.start()].strip() if heading else cells[0].strip()
        stack[depth:] = [name]
        if heading is None:
            continue
        if len(cells) < 3:
            raise ValueError(f"missing median column in {path.name}: {line}")
        value = durations.fullmatch(cells[2].strip())
        if value is None:
            raise ValueError(f"unrecognized median in {path.name}: {line}")
        key = "/".join(stack)
        if key in results:
            raise ValueError(f"duplicate benchmark case in {path.name}: {key}")
        results[key] = float(value[1]) * units_to_us[value[2]]
    if not results:
        raise ValueError(f"no benchmark medians in {path.name}")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=script_dir,
                        help="Result directory created by run.py")
    args = parser.parse_args()
    root = args.output_dir.resolve()
    metadata = json.loads((root / "runs.json").read_text())
    if metadata["preflight"]["exit_code"] or len(metadata["runs"]) != 3:
        raise ValueError("preflight and all three benchmark runs must succeed")
    if any(run["exit_code"] for run in metadata["runs"]):
        raise ValueError("a benchmark run failed")

    runs = [parse(root / run["output"]) for run in metadata["runs"]]
    keys = set(runs[0])
    for repeat, run in enumerate(runs[1:], 2):
        if set(run) != keys:
            missing = sorted(keys - set(run))
            extra = sorted(set(run) - keys)
            raise ValueError(f"run {repeat} has different cases: missing={missing}, extra={extra}")

    cases = set()
    for key in keys:
        parts = key.split("/", 2)
        if len(parts) != 3 or parts[0] not in {"arrow_native", "arrow_rofl"}:
            raise ValueError(f"unexpected benchmark path: {key}")
        size = int(parts[1])
        if size not in sizes:
            raise ValueError(f"unexpected row count in benchmark path: {key}")
        cases.add(parts[2])
    expected = {f"{group}/{size}/{case}" for group in ("arrow_native", "arrow_rofl")
                for size in sizes for case in cases}
    if keys != expected:
        raise ValueError(f"missing matched cases: {sorted(expected - keys)}")

    rows = []
    for size in sorted(sizes):
        for case in sorted(cases):
            native = [run[f"arrow_native/{size}/{case}"] for run in runs]
            rofl = [run[f"arrow_rofl/{size}/{case}"] for run in runs]
            if any(value <= 0 for value in native):
                raise ValueError(f"native median must be positive: {size}/{case}")
            rows.append({
                "rows": size,
                "case": case,
                "native_median_us": median(native),
                "rofl_median_us": median(rofl),
                "rofl_native_ratio": median(rofl) / median(native),
                "median_paired_ratio": median(candidate / baseline
                                               for baseline, candidate in zip(native, rofl)),
                **{f"native_run_{repeat}_us": value for repeat, value in enumerate(native, 1)},
                **{f"rofl_run_{repeat}_us": value for repeat, value in enumerate(rofl, 1)},
            })

    with (root / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "<!-- SPDX-License-Identifier: Apache-2.0 -->",
        "<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->",
        "",
        "# Arrow scalar families at 16,384 rows",
        "",
        "Times are microseconds per invocation. Each time is the median of three process medians.",
        "A ratio above 1 means the complete RowFn invocation was slower than the Arrow kernel.",
        "The paired ratio is the median of three within-run ratios. See `summary.csv` for all sizes.",
        "",
        "| Case | Arrow native | RowFn | Ratio | Paired ratio |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in sorted((row for row in rows if row["rows"] == 16384),
                      key=lambda row: row["rofl_native_ratio"], reverse=True):
        lines.append(
            f"| {row['case']} | {row['native_median_us']:.3f} | {row['rofl_median_us']:.3f} | "
            f"{row['rofl_native_ratio']:.3f} | {row['median_paired_ratio']:.3f} |"
        )
    (root / "summary-16384.md").write_text("\n".join(lines) + "\n")
    print(f"Summarized {len(cases)} matched cases at four sizes from three process runs.")


if __name__ == "__main__":
    main()
