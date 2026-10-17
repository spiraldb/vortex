# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Summarize three process medians, preserving their range and paired ratios."""

import csv
from pathlib import Path
import re
from statistics import median

root = Path(__file__).resolve().parent


def read(path):
    results = {}
    chain = []
    for line in path.read_text().splitlines():
        branch = re.search(r"[├╰]─ ", line)
        if branch is None:
            continue
        depth = branch.start() // 3
        body = line[branch.end():]
        name = re.split(r"\s{2,}", body, maxsplit=1)[0]
        chain = chain[:depth] + [name]
        values = re.findall(r"(\d+(?:\.\d+)?) (ns|µs|ms|s)", body)
        if values:
            value, unit = values[2]
            results["/".join(chain)] = float(value) * {
                "ns": .001, "µs": 1, "ms": 1000, "s": 1000000,
            }[unit]
    return results


with (root / "summary.csv").open("w") as output:
    writer = csv.writer(output)
    writer.writerow(["suite", "case", "before_us", "after_us", "after_min_us", "after_max_us",
                     "paired_after_before", "arrow_native_us", "after_native"])
    for suite in ["scalar", "workloads", "boundaries"]:
        before = [read(root / f"baseline-{suite}-{repeat}.txt") for repeat in range(1, 4)]
        after = [read(root / f"candidate-{suite}-{repeat}.txt") for repeat in range(1, 4)]
        assert all(result.keys() == before[0].keys() for result in before + after)
        for case in sorted(before[0]):
            old = [result[case] for result in before]
            new = [result[case] for result in after]
            native_case = case.replace("arrow_rofl/", "arrow_native/", 1)
            native = median(result[native_case] for result in after) if case.startswith("arrow_rofl/") else None
            writer.writerow([suite, case, median(old), median(new), min(new), max(new),
                             median(y / x for x, y in zip(old, new)), native,
                             median(new) / native if native is not None else None])
