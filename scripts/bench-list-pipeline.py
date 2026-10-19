# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare current and prebuilt list pipelines on identical ListLayout files."""

import argparse
import hashlib
import itertools
import json
import os
import platform
import statistics
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEEDS = [17, 42, 73]
VERSIONS = ["baseline", "candidate"]


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


class Comparison:
    def __init__(self, args: argparse.Namespace) -> None:
        self.binaries = {
            "baseline": (args.baseline_binary or args.binary).resolve(),
            "candidate": args.binary.resolve(),
        }
        self.results = args.output.resolve()
        self.data = self.results / "data"
        self.generator = args.generator.resolve()
        self.iterations = args.iterations
        self.rounds = args.rounds
        self.discard = 8
        if self.iterations <= self.discard or self.rounds <= 0:
            raise ValueError("Need more than 8 iterations and at least one round")
        (self.results / "raw").mkdir(parents=True, exist_ok=True)
        self.data.mkdir(parents=True, exist_ok=True)

    def prepare(self) -> dict[str, Path]:
        for target_bytes in [65536, 1048576]:
            for size, shape in [("large", "primitive"), ("large", "nested"), ("small", "primitive")]:
                name = f"{size}-{shape}-{target_bytes}"
                path = self.data / f"{name}.vortex"
                if path.exists():
                    continue
                command = [
                    str(self.generator),
                    str(path),
                    "small" if size == "small" else "skewed",
                    shape,
                    str(target_bytes),
                ]
                result = subprocess.run(command, capture_output=True, text=True, timeout=600)
                (self.results / f"layout-{name}.log").write_text(result.stderr)
                result.check_returncode()
                (self.results / f"layout-{name}.json").write_text(result.stdout)
        files = {path.stem: path for path in sorted(self.data.glob("*.vortex"))}
        assert len(files) == 6, files
        metadata = {
            "baseline": "current list implementation, prototype disabled",
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "candidate": "prebuilt pipeline prototype; VORTEX_LIST_PIPELINE=1",
            "platform": platform.platform(),
            "logical_cpu_count": os.cpu_count(),
            "affinity": "unrestricted",
            "seeds": SEEDS,
            "rounds": self.rounds,
            "iterations": self.iterations,
            "discard": self.discard,
            "output": "full Arrow decode and drop",
            "storage": "warm local disk",
            "rustflags": os.environ.get("RUSTFLAGS", "from Cargo configuration"),
            "profile": "release_debug",
            "files": {
                name: {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}
                for name, path in files.items()
            },
            "binaries": {version: digest(path) for version, path in self.binaries.items()},
        }
        self.write_json("metadata.json", metadata)
        return files

    def write_json(self, name: str, value: object) -> None:
        (self.results / name).write_text(json.dumps(value, indent=2) + "\n")

    def run(
        self,
        version: str,
        path: Path,
        count: int,
        pattern: str,
        mode: str,
        seed: int,
        check: bool,
        stem: str,
    ) -> dict:
        env = {key: value for key, value in os.environ.items() if not key.startswith(("VORTEX_", "TOKIO_"))}
        env.update(VORTEX_SCAN_V2="1", RUST_LOG="warn")
        if version == "candidate":
            env["VORTEX_LIST_PIPELINE"] = "1"
        command = [
            str(self.binaries[version]),
            str(path),
            "1" if check else str(self.iterations),
            str(count),
            pattern,
            mode,
            "disk",
            "check" if check else "decode",
            str(seed),
        ]
        with (self.results / "commands.jsonl").open("a") as output:
            output.write(
                json.dumps({"command": command, "selection_gate": version == "candidate", "VORTEX_SCAN_V2": "1"}) + "\n"
            )
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=180)
        (self.results / "raw" / f"{stem}.log").write_text(result.stderr)
        (self.results / "raw" / f"{stem}.json").write_text(result.stdout)
        result.check_returncode()
        return json.loads(result.stdout)

    def check(self, files: dict[str, Path]) -> int:
        checks = []
        selections = [
            (0, "indices:"),
            (1, "indices:0"),
            (1, "indices:17"),
            (1, "indices:16"),
            (100, "uniform"),
            (100, "clustered"),
        ]
        for name, path in files.items():
            for count, pattern in selections:
                for version in VERSIONS:
                    stem = f"check-{name}-{count}-{pattern.replace(':', '_')}-{version}"
                    data = self.run(version, path, count, pattern, "cached", 42, True, stem)
                    assert data["correct"]
                    checks.append({"dataset": name, "version": version, "pattern": pattern, "rows": count})
                    self.write_json("correctness.json", checks)
            print(json.dumps({"correctness": name, "passed": True}), flush=True)
        return len(checks)

    def measure(self, files: dict[str, Path]) -> list[dict]:
        summary = []
        for name, path in files.items():
            patterns = [(100, "uniform"), (100, "clustered")]
            if not name.startswith("small"):
                patterns.insert(0, (1, "indices:16"))
            for (count, pattern), mode in itertools.product(patterns, ["cached", "reopen"]):
                medians: dict[str, list[float]] = {version: [] for version in VERSIONS}
                for seed in SEEDS:
                    expected_indices = None
                    for repeat in range(self.rounds):
                        order = VERSIONS if repeat % 2 == 0 else list(reversed(VERSIONS))
                        for version in order:
                            stem = f"{name}-{count}-{pattern.replace(':', '_')}-{mode}-{seed}-{repeat}-{version}"
                            data = self.run(version, path, count, pattern, mode, seed, False, stem)
                            assert data["v2"] == "1"
                            assert len(data["runs"]) == self.iterations
                            assert not any(run["instrumented"] for run in data["runs"])
                            if expected_indices is None:
                                expected_indices = data["indices"]
                            assert data["indices"] == expected_indices
                            median = statistics.median(run["elapsed_ns"] for run in data["runs"][self.discard :])
                            medians[version].append(median / 1e6)
                row = {"dataset": name, "rows": count, "pattern": pattern, "mode": mode}
                row.update({version + "_ms": statistics.median(times) for version, times in medians.items()})
                row["change_pct"] = 100 * (row["candidate_ms"] / row["baseline_ms"] - 1)
                row["round_medians_ms"] = medians
                summary.append(row)
                self.write_json("summary.json", summary)
                print(json.dumps({key: value for key, value in row.items() if key != "round_medians_ms"}), flush=True)
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release_debug/examples/selection_scan")
    parser.add_argument("--baseline-binary", type=Path, help="Defaults to --binary with the prototype disabled")
    parser.add_argument("--generator", type=Path, default=ROOT / "target/release_debug/examples/generate_list_fixture")
    parser.add_argument("--output", type=Path, default=ROOT / "list-pipeline-results")
    parser.add_argument("--iterations", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=2)
    comparison = Comparison(parser.parse_args())
    files = comparison.prepare()
    checks = comparison.check(files)
    summary = comparison.measure(files)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(destination, "a") as output:
            output.write("\n### Deferred list pipeline experiment\n\n")
            output.write(
                f"{checks} reference checks passed; full Arrow decode on warm local disk. "
                "The prototype prebuilds pipelines but does not implement the current decoded-work admission budget. "
                "Positive changes are slower.\n\n"
            )
            output.write("| Dataset | Rows | Selection | Open mode | Current ms | Prototype ms | Change |\n")
            output.write("| --- | ---: | --- | --- | ---: | ---: | ---: |\n")
            for row in summary:
                output.write(
                    f"| {row['dataset']} | {row['rows']} | {row['pattern']} | {row['mode']} | "
                    f"{row['baseline_ms']:.3f} | {row['candidate_ms']:.3f} | {row['change_pct']:+.1f}% |\n"
                )


if __name__ == "__main__":
    main()
