# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Summarize complete paired diagnostic runs without combining compiler configurations."""

import csv
from pathlib import Path
import re
import statistics

root = Path(__file__).resolve().parent
units = {'ps': .001, 'ns': 1, 'µs': 1000, 'ms': 1_000_000, 's': 1_000_000_000}
number = re.compile(r'([\d.]+)\s+(ps|ns|µs|ms|s)')
records = []
for variant in ['baseline', 'inline', 'final', 'lists']:
    groups = {}
    for repeat in range(1, 4):
        stack = []
        for line in (root / f'{variant}-{repeat}.txt').read_text().splitlines():
            branch = re.search('[├╰]─ ', line)
            if not branch:
                continue
            depth = branch.start() // 3
            parts = line[branch.end():].split('│')
            duration = number.search(parts[0])
            name = parts[0][:duration.start()].strip() if duration else parts[0].strip()
            stack[depth:] = [name]
            if duration:
                value = number.fullmatch(parts[2].strip())
                groups.setdefault('/'.join(stack), {})[repeat] = float(value[1]) * units[value[2]]
    for case, runs in groups.items():
        assert set(runs) == {1, 2, 3}, (variant, case, runs)
        values = list(runs.values())
        records.append({'variant': variant, 'case': case, 'median_ns': statistics.median(values),
                        'min_ns': min(values), 'max_ns': max(values),
                        **{f'run_{i}_ns': runs[i] for i in range(1, 4)}})
with (root / 'summary.csv').open('w') as output:
    writer = csv.DictWriter(output, fieldnames=list(records[0]))
    writer.writeheader()
    writer.writerows(records)
lookup = {(r['variant'], r['case']): r['median_ns'] / 1000 for r in records}
for case in ['StartsWithShortScalar', 'StartsWithScalar', 'StartsWithUtf8Scalar', 'StartsWithArray',
             'EndsWithScalar', 'ContainsScalar', 'AsciiEqScalar', 'ConcatLong', 'ConcatShort', 'MultiplyArray']:
    print(case)
    for group in ['arrow_native', 'arrow_rofl', 'rofl_arrow_bytes', 'rofl_typed_strings',
                  'rofl_prepared_pattern', 'rofl_view_header', 'arrow_plus_two_validations']:
        key = f'{group}/16384/{case}'
        if ('baseline', key) in lookup:
            print(f'  {group:28s} {lookup["baseline", key]:8.2f} -> {lookup["inline", key]:8.2f} us')
