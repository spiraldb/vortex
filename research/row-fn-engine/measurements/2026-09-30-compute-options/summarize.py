# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Summarize every recorded process median and preserve its three-run range."""

import csv
import json
from pathlib import Path
import runpy
from statistics import median


directory = Path(__file__).resolve().parent
parse = runpy.run_path(str(directory.parent / "2026-09-29-families/summarize.py"))["parse"]
records = json.loads((directory / "runs.json").read_text())
assert len(records) == 16 and all(record["exit_code"] == 0 for record in records)
rows = []
for suite in ["arrow_families", "arrow_scalar", "arrow_workloads", "boundaries"]:
    runs = [parse(directory / f"{suite}-{repeat}.txt") for repeat in range(1, 4)]
    assert all(run.keys() == runs[0].keys() for run in runs)
    for case in sorted(runs[0]):
        values = [run[case] for run in runs]
        rows.append({"suite": suite, "case": case, "median_us": median(values),
                     "min_us": min(values), "max_us": max(values),
                     **{f"run_{repeat}_us": value for repeat, value in enumerate(values, 1)}})
with (directory / "summary.csv").open("w") as output:
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
print(f"Summarized {len(rows)} benchmark paths.")
