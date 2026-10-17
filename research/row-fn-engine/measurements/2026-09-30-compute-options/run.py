# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Measure the recorded benchmark executables serially, without rebuilding."""

import hashlib
import json
from pathlib import Path
import subprocess
import time


directory = Path(__file__).resolve().parent
build = json.loads((directory / "build.json").read_text())
suites = ["arrow_families", "arrow_scalar", "arrow_workloads", "boundaries"]
records = []


def run(suite, arguments, name):
    executable = build["executables"][suite]
    path = Path(executable["path"])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == executable["sha256"]
    output = directory / name
    command = [str(path), *arguments, "--color", "never"]
    started = time.time()
    with output.open("x") as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    records.append({"command": command, "output": name, "start_unix": started,
                    "seconds": time.time() - started, "exit_code": result.returncode})
    (directory / "runs.json").write_text(json.dumps(records, indent=2) + "\n")
    result.check_returncode()
    if "--bench" in arguments:
        assert "median" in output.read_text(), "benchmark did not report timing"
    print(name, flush=True)


def selection(suite):
    if suite == "boundaries":
        return []
    return ["16384", "--skip", "diagnosis", "--skip", "rofl_fixed_width"]


for suite in suites:
    run(suite, [*selection(suite), "--test"], f"{suite}-preflight.txt")

for repeat in range(1, 4):
    for suite in suites:
        run(suite, [*selection(suite), "--bench", "--timer", "os",
                    "--sample-count", "1000", "--min-time", "0.1", "--max-time", "0.3",
                    "--sortr" if repeat == 2 else "--sort", "name"],
            f"{suite}-{repeat}.txt")
