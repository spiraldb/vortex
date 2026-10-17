# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Run matched benchmark binaries serially and retain commands and complete output."""

import argparse
import json
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('boundaries', type=Path, nargs='?')
parser.add_argument('workloads', type=Path, nargs='?')
parser.add_argument('--scalar', type=Path)
parser.add_argument('--prefix', default='baseline')
args = parser.parse_args()
root = Path(__file__).resolve().parent
binaries = [(suite, path.resolve()) for suite, path in
            [('boundaries', args.boundaries), ('arrow', args.workloads), ('scalar', args.scalar)]
            if path is not None]
if not binaries:
    parser.error('provide at least one benchmark executable')
records = []
for repeat in range(1, 4):
    order = binaries if repeat != 2 else list(reversed(binaries))
    for suite, binary in order:
        command = [str(binary), '--bench', '--timer', 'os', '--color', 'never', '--sample-count', '1000',
                   '--min-time', '0.1', '--max-time', '0.3',
                   '--sortr' if repeat == 2 else '--sort', 'name']
        output = root / f'{args.prefix}-{suite}-{repeat}.txt'
        print(f'Running {output.name}', flush=True)
        start = time.time()
        with output.open('w') as stream:
            result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT)
        records.append({'command': command, 'start_unix': start, 'seconds': time.time() - start,
                        'output': output.name, 'exit_code': result.returncode})
        (root / f'{args.prefix}-runs.json').write_text(json.dumps(records, indent=2) + '\n')
        result.check_returncode()
