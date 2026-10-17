# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Run one prebuilt Arrow-family benchmark binary three times in serial."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone


script_dir = Path(__file__).resolve().parent
repo = script_dir.parents[3]


def command_output(command):
    return subprocess.check_output(command, cwd=repo, text=True).strip()


def write_record(record, output_dir):
    temporary = output_dir / "runs.json.tmp"
    temporary.write_text(json.dumps(record, indent=2) + "\n")
    temporary.replace(output_dir / "runs.json")


def run_to_file(command, path):
    started = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()
    with path.open("w") as output:
        result = subprocess.run(command, cwd=repo, stdout=output, stderr=subprocess.STDOUT)
    return {
        "command": command,
        "output": path.name,
        "started_utc": started,
        "elapsed_seconds": time.monotonic() - start,
        "exit_code": result.returncode,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True,
                        help="Path to the already-built arrow_families benchmark executable")
    parser.add_argument("--output-dir", type=Path, default=script_dir,
                        help="New result directory under this measurement directory")
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    if output_dir != script_dir and script_dir not in output_dir.parents:
        parser.error(f"output directory must be under {script_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    binary = args.binary if args.binary.is_absolute() else repo / args.binary
    binary = binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"benchmark binary is not executable: {binary}")
    if any(output_dir.glob("run-*.txt")) or (output_dir / "runs.json").exists():
        parser.error("this output directory already contains a run; preserve it and use a new directory")

    data = binary.read_bytes()
    record = {
        "benchmark": "rofl-examples/benches/arrow_families.rs",
        "declared_build_profile": "bench",
        "build_profile_verified_from_binary": False,
        "binary": str(binary),
        "binary_sha256": hashlib.sha256(data).hexdigest(),
        "binary_bytes": len(data),
        "output_dir": str(output_dir),
        "git_head": command_output(["git", "rev-parse", "HEAD"]),
        "git_branch": command_output(["git", "branch", "--show-current"]),
        "git_status_porcelain": command_output(["git", "status", "--porcelain=v1"]),
        "cargo_lock_sha256": hashlib.sha256((repo / "Cargo.lock").read_bytes()).hexdigest(),
        "rustc_version": command_output(["rustc", "-vV"]),
        "system": platform.uname()._asdict(),
        "python_version": sys.version,
        "repo": str(repo),
        "configured_rows": [0, 64, 1024, 16384],
        "timer": "os",
        "sample_count": 1000,
        "min_time_seconds": 0.03,
        "max_time_seconds": 0.1,
        "runs": [],
    }
    write_record(record, output_dir)

    record["preflight"] = run_to_file(
        [str(binary), "--test", "--color", "never"], output_dir / "preflight.txt"
    )
    write_record(record, output_dir)
    if record["preflight"]["exit_code"]:
        raise RuntimeError("benchmark fixture preflight failed; see preflight.txt")

    for repeat in range(1, 4):
        order = ["--sortr", "name"] if repeat == 2 else ["--sort", "name"]
        command = [
            str(binary), "--bench", "--timer", "os", "--color", "never",
            "--sample-count", "1000", "--min-time", "0.03", "--max-time", "0.1",
            *order,
        ]
        path = output_dir / f"run-{repeat}.txt"
        result = run_to_file(command, path)
        record["runs"].append(result)
        write_record(record, output_dir)
        print(path.name, flush=True)
        if result["exit_code"]:
            raise RuntimeError(f"benchmark run {repeat} failed; see {path.name}")


if __name__ == "__main__":
    main()
