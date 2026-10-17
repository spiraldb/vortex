# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Build captured benchmark binaries, restoring current files after baseline reconstruction."""

import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile

root = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument('variant', choices=['baseline', 'candidate'])
args = parser.parse_args()
saved = {}
baseline = {}
if args.variant == 'baseline':
    with tarfile.open(root / 'before.tar.gz') as archive:
        for item in archive.getmembers():
            if item.name.endswith('.rs'):
                baseline[Path(item.name)] = archive.extractfile(item).read()
    legacy = Path('vortex-array/src/scalar_fn/unstable/row/types/sink/utf8.rs')
    baseline[legacy] = subprocess.check_output(['git', '--work-tree=' + str(Path.cwd()), 'show', f'HEAD:{legacy}'])
    saved = {path: path.read_bytes() for path in baseline}
    with tarfile.open('/tmp/rofl-opt-current.tar.gz', 'w:gz') as recovery:
        for path, data in saved.items():
            item = tarfile.TarInfo(str(path))
            item.size = len(data)
            recovery.addfile(item, io.BytesIO(data))
    for path, data in baseline.items():
        path.write_bytes(data)

records = []
try:
    for name, target in [('scalar', 'arrow_scalar'), ('workloads', 'arrow_workloads'), ('boundaries', 'boundaries')]:
        command = ['cargo', 'rustc', '--locked', '--profile', 'bench', '-p', 'rofl-examples',
                   '--bench', target, '--message-format=json', '--', '--emit=llvm-ir,asm,link']
        result = subprocess.run(command, capture_output=True, text=True)
        (root / f'build-{args.variant}-{name}.log').write_text(result.stderr)
        result.check_returncode()
        artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
        executable = next(a['executable'] for a in artifacts if a.get('reason') == 'compiler-artifact'
                          and a['target']['name'] == target and a.get('executable'))
        destination = Path('target/rofl-opt') / f'{args.variant}-{name}'
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(executable, destination)
        destination.chmod(0o755)
        records.append({'command': command, 'executable': executable, 'captured': str(destination),
                        'sha256': hashlib.sha256(destination.read_bytes()).hexdigest()})
        (root / f'build-{args.variant}.json').write_text(json.dumps(records, indent=2) + '\n')
        print(destination, flush=True)
finally:
    conflicts = []
    for path, data in saved.items():
        if path.read_bytes() == baseline[path]:
            path.write_bytes(data)
        else:
            conflicts.append(str(path))
    if conflicts:
        raise RuntimeError(f'Files changed during baseline build; current edits preserved: {conflicts}. Recovery: /tmp/rofl-opt-current.tar.gz')
