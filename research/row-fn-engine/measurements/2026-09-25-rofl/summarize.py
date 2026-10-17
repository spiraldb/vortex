# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Extract median timings from Divan trees and retain each repetition in CSV."""

import argparse
import csv
from pathlib import Path
import re
import statistics

parser = argparse.ArgumentParser()
parser.add_argument('--suite', choices=['boundaries', 'arrow', 'scalar'], action='append')
parser.add_argument('--prefix', default='baseline')
parser.add_argument('--output', default='summary.csv')
args = parser.parse_args()
root = Path(__file__).resolve().parent
units = {'ps': .001, 'ns': 1, 'µs': 1000, 'ms': 1_000_000, 's': 1_000_000_000}
number = re.compile(r'([\d.]+)\s+(ps|ns|µs|ms|s)')
all_rows = []
for suite in args.suite or ['boundaries', 'arrow']:
    groups = {}
    for repeat in range(1, 4):
        path = root / f'{args.prefix}-{suite}-{repeat}.txt'
        if not path.exists():
            continue
        stack = []
        for line in path.read_text().splitlines():
            branch = re.search('[├╰]─ ', line)
            if not branch:
                continue
            depth = branch.start() // 3
            parts = line[branch.end():].split('│')
            duration = number.search(parts[0])
            name = parts[0][:duration.start()].strip() if duration else parts[0].strip()
            stack[depth:] = [name]
            if not duration:
                continue
            value = number.fullmatch(parts[2].strip())
            ns = float(value[1]) * units[value[2]]
            key = '/'.join(stack)
            groups.setdefault(key, {})[repeat] = ns
    for key, runs in groups.items():
        if set(runs) != {1, 2, 3}:
            raise ValueError(f'{suite}/{key}: expected three complete repetitions')
        values = list(runs.values())
        all_rows.append({'suite': suite, 'case': key, 'median_ns': statistics.median(values),
                         'min_run_ns': min(values), 'max_run_ns': max(values),
                         **{f'run_{i}_ns': runs.get(i, '') for i in range(1, 4)}})
with (root / args.output).open('w') as stream:
    writer = csv.DictWriter(stream, fieldnames=list(all_rows[0]))
    writer.writeheader()
    writer.writerows(all_rows)
for rows in [64, 16384]:
    print(f'\nArrow {rows} rows: baseline ns, rofl ns, ratio')
    timings = {row['case']: row['median_ns'] for row in all_rows if row['suite'] in ['arrow', 'scalar']}
    prefix = f'arrow_native/{rows}/'
    for name in sorted(key[len(prefix):] for key in timings if key.startswith(prefix)):
        native = timings[prefix + name]
        extracted = timings.get(f'arrow_rofl/{rows}/{name}')
        if extracted is not None:
            print(f'{name:24s} {native:10.2f} {extracted:10.2f} {extracted/native:7.2f}x')
print('\nBoundaries: median ns')
for row in all_rows:
    if row['suite'] == 'boundaries' and ('16384' in row['case'] or 'nullable_bool' in row['case'] or row['case'].endswith('/64')):
        print(row['case'], row['median_ns'])
