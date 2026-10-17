# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Alternate matching baseline and candidate binaries, with no concurrent builds."""

import json
from pathlib import Path
import subprocess
import time

root = Path(__file__).resolve().parent
records = []
for suite in ['scalar', 'workloads', 'boundaries']:
    for variant in ['baseline', 'candidate']:
        executable = f'target/rofl-opt/{variant}-{suite}'
        with (root / f'{variant}-{suite}-preflight.log').open('w') as output:
            subprocess.run([executable, '--test', '--color', 'never'], stdout=output,
                           stderr=subprocess.STDOUT, check=True)
for repeat in range(1, 4):
    for suite in ['scalar', 'workloads', 'boundaries']:
        for variant in (['candidate', 'baseline'] if repeat == 2 else ['baseline', 'candidate']):
            command = [f'target/rofl-opt/{variant}-{suite}']
            if suite != 'boundaries':
                command += ['16384']
            command += ['--bench', '--timer', 'os', '--color', 'never', '--sample-count', '1000',
                        '--min-time', '0.1', '--max-time', '0.3',
                        '--sortr' if repeat == 2 else '--sort', 'name']
            path = root / f'{variant}-{suite}-{repeat}.txt'
            start = time.time()
            with path.open('w') as output:
                result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
            records.append({'command': command, 'start_unix': start, 'seconds': time.time() - start,
                            'output': path.name, 'exit_code': result.returncode})
            (root / 'runs.json').write_text(json.dumps(records, indent=2) + '\n')
            result.check_returncode()
            print(path.name, flush=True)
