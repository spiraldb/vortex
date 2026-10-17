# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Alternate captured baseline and inline-candidate binaries without concurrent builds."""

import argparse
import json
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument("--final", action="store_true")
parser.add_argument("--lists", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parent
records = []
for variant in (['lists'] if args.lists else ['final'] if args.final else ['baseline', 'inline']):
    with (root / f'{variant}-preflight.log').open('w') as output:
        subprocess.run([f'target/rofl-diagnosis/{variant}', '--test', '--color', 'never'],
                       stdout=output, stderr=subprocess.STDOUT, check=True)
for repeat in range(1, 4):
    for variant in (['lists'] if args.lists else ['final'] if args.final else ['inline', 'baseline'] if repeat == 2 else ['baseline', 'inline']):
        command = [f'target/rofl-diagnosis/{variant}', '16384', '--bench', '--timer', 'os',
                   '--color', 'never', '--sample-count', '1000', '--min-time', '0.1',
                   '--max-time', '0.3', '--sortr' if repeat == 2 else '--sort', 'name']
        path = root / f'{variant}-{repeat}.txt'
        print(path.name, flush=True)
        start = time.time()
        with path.open('w') as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
        records.append({'command': command, 'start_unix': start, 'seconds': time.time() - start,
                        'output': path.name, 'exit_code': result.returncode})
        (root / ('lists-runs.json' if args.lists else 'final-runs.json' if args.final else 'runs.json')).write_text(json.dumps(records, indent=2) + '\n')
        result.check_returncode()
