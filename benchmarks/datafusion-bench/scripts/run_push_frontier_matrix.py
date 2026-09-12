#!/usr/bin/env python3
"""Run a globally correctness-gated V1 versus push-frontier benchmark matrix.

The runner verifies every selected query before starting any measurement. It deliberately
launches one query and one backend per child process. Measured children use an explicit symmetric
HOT-cache protocol: an identical, untimed child is run immediately before each measured child.
No dataset generation, deletion, or OS cache eviction is performed, and the output is not
cold-cache evidence.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import threading
import time
from typing import BinaryIO, Iterable, Sequence


SUPPORTED_SUITES = ("clickbench", "tpch", "fineweb", "tpcds")
BACKEND_V1 = "v1"
BACKEND_FRONTIER = "push-frontier"
BACKENDS = (BACKEND_V1, BACKEND_FRONTIER)
GLOBAL_PHASE_CORRECTNESS = "correctness"
GLOBAL_PHASE_MEASUREMENT = "measurement"
CORRECTNESS_SUCCESS_MARKER = "all-correctness-succeeded"
FORMAT = "vortex"
JSONL_NAME = "matrix.jsonl"
RUN_MANIFEST_NAME = "run-manifest.json"
DEFAULT_MAX_LOG_BYTES = 4 * 1024 * 1024
MAX_LOG_BYTES = 256 * 1024 * 1024
MAX_INPUT_FILES = 1_000_000
MAX_QUERIES = 10_000
MAX_QUERY_ID = 999_999
MAX_SAMPLES = 10_000
MAX_PLANNED_CHILDREN = 1_000_000
MAX_OPTIONS = 64
MAX_OPTION_LENGTH = 2_048
MAX_OUTPUT_PATH_LENGTH = 1_024
MACOS_TIME = Path("/usr/bin/time")
QUERY_SELECTION_RE = re.compile(r"\d+(?:,\d+)*\Z")
BENCH_OPT_RE = re.compile(r"[^\s=]+=[^\s=]+\Z")
MACOS_RSS_RE = re.compile(
    r"^\s*(\d+)\s+maximum resident set size\s*$", re.MULTILINE
)


class MatrixError(RuntimeError):
    """A runner validation or child-process failure."""


@dataclasses.dataclass(frozen=True)
class MatrixConfig:
    binary: Path
    suite: str
    output_dir: Path
    input_root: Path | None
    samples: int
    partitions: int
    correctness_partitions: int
    queries: str | None
    exclude_queries: str | None
    bench_options: tuple[str, ...]
    correctness_bench_options: tuple[str, ...]
    runner_prefix: str
    timeout_seconds: float
    max_log_bytes: int
    dry_run: bool


@dataclasses.dataclass(frozen=True)
class ChildSpec:
    global_phase: str
    phase: str
    query_id: int
    backend: str
    sample: int | None
    argv: tuple[str, ...]
    partitions: int
    included_in_measurements: bool
    collect_peak_rss: bool


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def bounded_samples(value: str) -> int:
    parsed = positive_int(value)
    if parsed > MAX_SAMPLES:
        raise argparse.ArgumentTypeError(f"must be at most {MAX_SAMPLES}")
    return parsed


def bounded_log_bytes(value: str) -> int:
    parsed = positive_int(value)
    if parsed > MAX_LOG_BYTES:
        raise argparse.ArgumentTypeError(f"must be at most {MAX_LOG_BYTES}")
    return parsed


def query_selection(value: str) -> str:
    if not QUERY_SELECTION_RE.fullmatch(value):
        raise argparse.ArgumentTypeError("must be a comma-separated list of query IDs")
    query_ids = value.split(",")
    if len(query_ids) > MAX_QUERIES:
        raise argparse.ArgumentTypeError(f"must contain at most {MAX_QUERIES} query IDs")
    if any(int(query_id) > MAX_QUERY_ID for query_id in query_ids):
        raise argparse.ArgumentTypeError(f"query IDs must be at most {MAX_QUERY_ID}")
    return value


def bench_option(value: str) -> str:
    if len(value) > MAX_OPTION_LENGTH:
        raise argparse.ArgumentTypeError(
            f"benchmark option must be at most {MAX_OPTION_LENGTH} characters"
        )
    if not BENCH_OPT_RE.fullmatch(value):
        raise argparse.ArgumentTypeError(
            "benchmark option must use the datafusion-bench KEY=VALUE grammar"
        )
    return value


def parse_args(argv: Sequence[str] | None = None) -> MatrixConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=SUPPORTED_SUITES)
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path("target/release_debug/datafusion-bench"),
        help="prebuilt datafusion-bench binary",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="new or empty directory for JSONL, result artifacts, and child logs",
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        help=(
            "existing local dataset directory to hash into the immutable run manifest; "
            "required unless --dry-run is used"
        ),
    )
    parser.add_argument("--samples", type=bounded_samples, default=5)
    parser.add_argument(
        "--partitions",
        type=positive_int,
        default=4,
        help=(
            "timed/prewarm worker count passed as --threads N and fixed in "
            "DATAFUSION_EXECUTION_TARGET_PARTITIONS (default: 4)"
        ),
    )
    parser.add_argument(
        "--correctness-partitions",
        type=positive_int,
        default=1,
        help=(
            "correctness write/verify worker count passed as --threads N and fixed in "
            "DATAFUSION_EXECUTION_TARGET_PARTITIONS (default: 1)"
        ),
    )
    parser.add_argument(
        "--queries",
        type=query_selection,
        help="optional comma-separated query selection passed to --print-queries",
    )
    parser.add_argument(
        "--exclude-queries",
        type=query_selection,
        help="optional comma-separated exclusion passed to --print-queries",
    )
    parser.add_argument(
        "--opt",
        dest="bench_options",
        action="append",
        type=bench_option,
        default=[],
        metavar="KEY=VALUE",
        help=(
            "benchmark option passed through as a separate --opt argument; repeat for "
            "ClickBench flavor, TPC scale factor, or remote-data-dir"
        ),
    )
    parser.add_argument(
        "--correctness-opt",
        dest="correctness_bench_options",
        action="append",
        type=bench_option,
        default=[],
        metavar="KEY=VALUE",
        help=(
            "additional benchmark option appended only for correctness discovery, "
            "V1 writes, and push-frontier verification; repeat as needed"
        ),
    )
    parser.add_argument("--runner-prefix", default="push-frontier-matrix")
    parser.add_argument("--timeout-seconds", type=float, default=3_600.0)
    parser.add_argument(
        "--max-log-bytes",
        type=bounded_log_bytes,
        default=DEFAULT_MAX_LOG_BYTES,
        help="maximum bytes retained in each child stdout/stderr file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="run query discovery, print the child plan as JSONL, and execute no queries",
    )
    args = parser.parse_args(argv)

    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be greater than zero")
    if len(args.bench_options) + len(args.correctness_bench_options) > MAX_OPTIONS:
        parser.error(
            f"at most {MAX_OPTIONS} combined --opt and --correctness-opt arguments "
            "are supported"
        )
    if not args.runner_prefix or len(args.runner_prefix) > 128:
        parser.error("--runner-prefix must contain 1 to 128 characters")
    if any(ord(char) < 32 for char in args.runner_prefix):
        parser.error("--runner-prefix must not contain control characters")
    if not args.dry_run and args.input_root is None:
        parser.error("--input-root is required unless --dry-run is used")

    return MatrixConfig(
        binary=args.binary,
        suite=args.suite,
        output_dir=args.output_dir,
        input_root=args.input_root,
        samples=args.samples,
        partitions=args.partitions,
        correctness_partitions=args.correctness_partitions,
        queries=args.queries,
        exclude_queries=args.exclude_queries,
        bench_options=tuple(args.bench_options),
        correctness_bench_options=tuple(args.correctness_bench_options),
        runner_prefix=args.runner_prefix,
        timeout_seconds=args.timeout_seconds,
        max_log_bytes=args.max_log_bytes,
        dry_run=args.dry_run,
    )


def benchmark_options(
    config: MatrixConfig, *, correctness: bool
) -> tuple[str, ...]:
    if correctness:
        return config.bench_options + config.correctness_bench_options
    return config.bench_options


def selection_argv(config: MatrixConfig, *, correctness: bool = False) -> list[str]:
    argv: list[str] = []
    if config.queries is not None:
        argv.extend(("--queries", config.queries))
    if config.exclude_queries is not None:
        argv.extend(("--exclude-queries", config.exclude_queries))
    for option in benchmark_options(config, correctness=correctness):
        argv.extend(("--opt", option))
    return argv


def discovery_argv(
    config: MatrixConfig, *, correctness: bool = False
) -> tuple[str, ...]:
    return (
        str(config.binary),
        config.suite,
        "--print-queries",
        *selection_argv(config, correctness=correctness),
    )


def benchmark_argv(
    config: MatrixConfig, query_id: int, *, correctness: bool = False
) -> list[str]:
    partitions = (
        config.correctness_partitions if correctness else config.partitions
    )
    argv = [
        str(config.binary),
        config.suite,
        "--formats",
        FORMAT,
        "--queries",
        str(query_id),
        "--threads",
        str(partitions),
    ]
    for option in benchmark_options(config, correctness=correctness):
        argv.extend(("--opt", option))
    return argv


def result_argv(
    config: MatrixConfig,
    query_id: int,
    artifact_dir: Path,
    *,
    verify: bool,
) -> tuple[str, ...]:
    flag = "--verify-result-artifacts" if verify else "--write-result-artifacts"
    return (
        *benchmark_argv(config, query_id, correctness=True),
        flag,
        str(artifact_dir),
    )


def measured_argv(
    config: MatrixConfig, query_id: int, sample: int, backend: str
) -> tuple[str, ...]:
    runner = f"{config.runner_prefix}-{config.suite}-q{query_id}-{backend}-s{sample}"
    return (
        *benchmark_argv(config, query_id),
        "--iterations",
        "1",
        "--display-format",
        "gh-json",
        "--hide-progress-bar",
        "--runner",
        runner,
    )


def measurement_order(query_position: int, sample: int) -> tuple[str, str]:
    """Alternate pair order across both query and sample position."""
    if (query_position + sample) % 2 == 0:
        return BACKENDS
    return BACKENDS[1], BACKENDS[0]


def safe_output_path(root: Path, *parts: str) -> Path:
    resolved_root = root.resolve()
    if len(str(resolved_root)) > MAX_OUTPUT_PATH_LENGTH:
        raise MatrixError("output directory path is too long")
    candidate = resolved_root.joinpath(*parts).resolve()
    if candidate != resolved_root and resolved_root not in candidate.parents:
        raise MatrixError(f"output path escapes output directory: {candidate}")
    if len(str(candidate)) > MAX_OUTPUT_PATH_LENGTH:
        raise MatrixError("generated output path is too long")
    return candidate


def canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def resolved_input_root(input_root: Path | None) -> Path:
    if input_root is None:
        raise MatrixError("--input-root is required for an executable matrix")
    if input_root.is_symlink():
        raise MatrixError(f"input root must not be a symlink: {input_root}")
    try:
        root = input_root.resolve(strict=True)
    except OSError as err:
        raise MatrixError(f"input root does not exist: {input_root}") from err
    if not root.is_dir():
        raise MatrixError(f"input root is not a directory: {root}")
    return root


def validate_input_output_separation(config: MatrixConfig) -> Path:
    input_root = resolved_input_root(config.input_root)
    output_root = config.output_dir.resolve()
    if (
        input_root == output_root
        or input_root in output_root.parents
        or output_root in input_root.parents
    ):
        raise MatrixError(
            "input and output directories must be disjoint: "
            f"input={input_root}, output={output_root}"
        )
    return input_root


def sha256_regular_file(path: Path, root: Path) -> tuple[str, int]:
    if path.is_symlink():
        raise MatrixError(f"input contains a symlink: {path}")
    try:
        resolved = path.resolve(strict=True)
    except OSError as err:
        raise MatrixError(f"input path disappeared while hashing: {path}") from err
    if resolved == root or root not in resolved.parents:
        raise MatrixError(f"input path escapes input root: {path}")

    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as err:
        raise MatrixError(f"failed to open input file: {path}: {err}") from err

    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise MatrixError(f"input path is not a regular file: {path}")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
            after = os.fstat(stream.fileno())
    except OSError as err:
        raise MatrixError(f"failed while hashing input file: {path}: {err}") from err
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    signature_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    signature_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if signature_before != signature_after:
        raise MatrixError(f"input file changed while it was hashed: {path}")
    try:
        final = path.stat(follow_symlinks=False)
    except OSError as err:
        raise MatrixError(f"input file disappeared after hashing: {path}") from err
    signature_final = (
        final.st_dev,
        final.st_ino,
        final.st_size,
        final.st_mtime_ns,
        final.st_ctime_ns,
    )
    if not stat.S_ISREG(final.st_mode) or signature_before != signature_final:
        raise MatrixError(f"input file changed while it was hashed: {path}")
    return digest.hexdigest(), before.st_size


def input_file_paths(input_root: Path) -> list[Path]:
    paths: list[Path] = []

    def visit(directory: Path) -> None:
        try:
            with os.scandir(directory) as entries:
                ordered = sorted(entries, key=lambda entry: os.fsencode(entry.name))
        except OSError as err:
            raise MatrixError(f"failed to enumerate input directory: {directory}: {err}") from err
        for entry in ordered:
            path = Path(entry.path)
            try:
                if entry.is_symlink():
                    raise MatrixError(f"input contains a symlink: {path}")
                if entry.is_dir(follow_symlinks=False):
                    resolved = path.resolve(strict=True)
                    if input_root not in resolved.parents:
                        raise MatrixError(f"input directory escapes input root: {path}")
                    visit(path)
                elif entry.is_file(follow_symlinks=False):
                    paths.append(path)
                    if len(paths) > MAX_INPUT_FILES:
                        raise MatrixError(
                            f"input root contains more than {MAX_INPUT_FILES} files"
                        )
                else:
                    raise MatrixError(f"input contains a non-file entry: {path}")
            except OSError as err:
                raise MatrixError(f"failed to inspect input path: {path}: {err}") from err

    visit(input_root)
    if not paths:
        raise MatrixError(f"input root contains no files: {input_root}")
    return paths


def collect_input_identity(input_root: Path) -> dict[str, object]:
    files: list[dict[str, object]] = []
    total_bytes = 0
    for path in input_file_paths(input_root):
        digest, size = sha256_regular_file(path, input_root)
        relative_path = path.relative_to(input_root).as_posix()
        files.append(
            {
                "relative_path": relative_path,
                "sha256": digest,
                "size_bytes": size,
            }
        )
        total_bytes += size
    payload: dict[str, object] = {
        "root": str(input_root),
        "file_count": len(files),
        "total_size_bytes": total_bytes,
        "files": files,
    }
    return payload | {"manifest_sha256": sha256_bytes(canonical_json_bytes(payload))}


def collect_file_identity(path: Path) -> dict[str, object]:
    try:
        resolved = path.resolve(strict=True)
    except OSError as err:
        raise MatrixError(f"benchmark executable does not exist: {path}") from err
    digest, size = sha256_regular_file(resolved, resolved.parent)
    return {"path": str(resolved), "sha256": digest, "size_bytes": size}


def git_output(repository: Path, *args: str) -> bytes:
    try:
        completed = subprocess.run(
            ("git", "-C", str(repository), *args),
            shell=False,
            check=False,
            capture_output=True,
        )
    except OSError as err:
        raise MatrixError(f"failed to inspect Git provenance: {err}") from err
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace")[-4_096:].strip()
        raise MatrixError(f"Git provenance command failed: {detail}")
    return completed.stdout


def framed_sha256(parts: Sequence[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for label, value in parts:
        label_bytes = label.encode("ascii")
        digest.update(len(label_bytes).to_bytes(8, "big"))
        digest.update(label_bytes)
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def collect_git_identity(repository_hint: Path) -> dict[str, object]:
    root_output = git_output(repository_hint, "rev-parse", "--show-toplevel")
    try:
        repository = Path(os.fsdecode(root_output.rstrip(b"\n"))).resolve(strict=True)
    except OSError as err:
        raise MatrixError("Git repository root does not exist") from err
    head = git_output(repository, "rev-parse", "--verify", "HEAD").decode(
        "ascii", errors="strict"
    ).strip()
    status_args = ("status", "--porcelain=v1", "-z", "--untracked-files=all", "--", ".")
    status_before = git_output(repository, *status_args)
    diff = git_output(
        repository,
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--binary",
        "HEAD",
        "--",
        ".",
    )
    status_after = git_output(repository, *status_args)
    if status_before != status_after:
        raise MatrixError("Git working-tree status changed while provenance was collected")
    return {
        "repository_root": str(repository),
        "scope": ".",
        "head": head,
        "dirty": bool(status_before),
        "status_sha256": sha256_bytes(status_before),
        "diff_sha256": sha256_bytes(diff),
        "working_tree_sha256": framed_sha256(
            (("status-porcelain-v1-z", status_before), ("diff-binary-head", diff))
        ),
    }


def matrix_policy(config: MatrixConfig) -> dict[str, object]:
    return {
        "global_phase_order": [
            GLOBAL_PHASE_CORRECTNESS,
            GLOBAL_PHASE_MEASUREMENT,
        ],
        "measurement_requires_marker": CORRECTNESS_SUCCESS_MARKER,
        "measurement": {
            "partitions": config.partitions,
            "threads": config.partitions,
            "bench_options": list(config.bench_options),
            "discovery_command": list(discovery_argv(config)),
        },
        "correctness": {
            "partitions": config.correctness_partitions,
            "threads": config.correctness_partitions,
            "bench_options": list(
                benchmark_options(config, correctness=True)
            ),
            "correctness_only_bench_options": list(
                config.correctness_bench_options
            ),
            "discovery_command": list(
                discovery_argv(config, correctness=True)
            ),
        },
    }


def collect_run_manifest(
    config: MatrixConfig, *, repository_hint: Path | None = None
) -> dict[str, object]:
    input_root = validate_input_output_separation(config)
    return {
        "schema_version": 1,
        "input": collect_input_identity(input_root),
        "benchmark_executable": collect_file_identity(config.binary),
        "git": collect_git_identity(repository_hint or Path.cwd()),
        "matrix_policy": matrix_policy(config),
    }


def write_run_manifest(root: Path, manifest: dict[str, object]) -> dict[str, str]:
    path = safe_output_path(root, RUN_MANIFEST_NAME)
    contents = canonical_json_bytes(manifest)
    with path.open("xb") as stream:
        stream.write(contents)
    input_identity = manifest["input"]
    assert isinstance(input_identity, dict)
    input_manifest_sha256 = input_identity["manifest_sha256"]
    assert isinstance(input_manifest_sha256, str)
    return {
        "run_manifest_path": str(path.relative_to(root.resolve())),
        "run_manifest_sha256": sha256_bytes(contents),
        "input_manifest_sha256": input_manifest_sha256,
    }


def correctness_specs(config: MatrixConfig, query_id: int) -> Iterable[ChildSpec]:
    artifact_dir = safe_output_path(
        config.output_dir, "results", f"q{query_id:06d}"
    )
    yield ChildSpec(
        global_phase=GLOBAL_PHASE_CORRECTNESS,
        phase="exact-write",
        query_id=query_id,
        backend=BACKEND_V1,
        sample=None,
        argv=result_argv(config, query_id, artifact_dir, verify=False),
        partitions=config.correctness_partitions,
        included_in_measurements=False,
        collect_peak_rss=False,
    )
    yield ChildSpec(
        global_phase=GLOBAL_PHASE_CORRECTNESS,
        phase="exact-verify",
        query_id=query_id,
        backend=BACKEND_FRONTIER,
        sample=None,
        argv=result_argv(config, query_id, artifact_dir, verify=True),
        partitions=config.correctness_partitions,
        included_in_measurements=False,
        collect_peak_rss=False,
    )


def measurement_specs(
    config: MatrixConfig, query_id: int, query_position: int
) -> Iterable[ChildSpec]:
    for sample in range(config.samples):
        for backend in measurement_order(query_position, sample):
            argv = measured_argv(config, query_id, sample, backend)
            yield ChildSpec(
                global_phase=GLOBAL_PHASE_MEASUREMENT,
                phase="hot-prewarm",
                query_id=query_id,
                backend=backend,
                sample=sample,
                argv=argv,
                partitions=config.partitions,
                included_in_measurements=False,
                collect_peak_rss=False,
            )
            yield ChildSpec(
                global_phase=GLOBAL_PHASE_MEASUREMENT,
                phase="measure",
                query_id=query_id,
                backend=backend,
                sample=sample,
                argv=argv,
                partitions=config.partitions,
                included_in_measurements=True,
                collect_peak_rss=True,
            )


def correctness_success_marker(
    sequence: int,
    query_ids: Sequence[int],
    identity_ref: dict[str, str] | None = None,
    *,
    planned: bool = False,
) -> dict[str, object]:
    record: dict[str, object] = {
        "record_type": "planned-phase-marker" if planned else "phase-marker",
        "schema_version": 1,
        "sequence": sequence,
        "global_phase": GLOBAL_PHASE_CORRECTNESS,
        "marker": CORRECTNESS_SUCCESS_MARKER,
        "status": "planned" if planned else "succeeded",
        "completed_query_ids": list(query_ids),
        "completed_query_count": len(query_ids),
        "completed_child_count": 2 * len(query_ids),
        "next_global_phase": GLOBAL_PHASE_MEASUREMENT,
    }
    if identity_ref is not None:
        record.update(identity_ref)
    return record


def child_environment(base: dict[str, str], backend: str, partitions: int) -> dict[str, str]:
    if backend not in BACKENDS:
        raise MatrixError(f"unsupported backend: {backend}")
    env = base.copy()
    env.pop("VORTEX_USE_SCAN_API", None)
    env["VORTEX_SCAN_BACKEND"] = backend
    env["DATAFUSION_EXECUTION_TARGET_PARTITIONS"] = str(partitions)
    return env


def environment_changes(backend: str, partitions: int) -> dict[str, str | None]:
    """Describe child environment mutations; ``None`` records an explicitly unset variable."""
    return {
        "VORTEX_USE_SCAN_API": None,
        "VORTEX_SCAN_BACKEND": backend,
        "DATAFUSION_EXECUTION_TARGET_PARTITIONS": str(partitions),
    }


def parse_query_ids(stdout: str) -> list[int]:
    query_ids: list[int] = []
    seen: set[int] = set()
    for line_number, line in enumerate(stdout.splitlines(), start=1):
        value = line.strip()
        if not value:
            continue
        if not value.isascii() or not value.isdecimal():
            raise MatrixError(
                f"invalid --print-queries output on line {line_number}: {value!r}"
            )
        query_id = int(value)
        if query_id > MAX_QUERY_ID:
            raise MatrixError(f"query ID exceeds {MAX_QUERY_ID}: {query_id}")
        if query_id in seen:
            raise MatrixError(f"duplicate query ID from --print-queries: {query_id}")
        seen.add(query_id)
        query_ids.append(query_id)
        if len(query_ids) > MAX_QUERIES:
            raise MatrixError(f"--print-queries returned more than {MAX_QUERIES} IDs")
    if not query_ids:
        raise MatrixError("--print-queries selected no queries")
    return query_ids


def discover_query_ids(
    config: MatrixConfig, *, correctness: bool = False
) -> list[int]:
    env = child_environment(os.environ.copy(), BACKEND_V1, config.partitions)
    mode = "correctness" if correctness else "measurement"
    try:
        completed = subprocess.run(
            discovery_argv(config, correctness=correctness),
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            timeout=min(config.timeout_seconds, 60.0),
        )
    except subprocess.TimeoutExpired as err:
        raise MatrixError(f"{mode} --print-queries timed out") from err
    except OSError as err:
        raise MatrixError(f"failed to run {mode} --print-queries: {err}") from err
    if completed.returncode != 0:
        stderr = completed.stderr[-4_096:].strip()
        raise MatrixError(
            f"{mode} --print-queries failed with exit status "
            f"{completed.returncode}: {stderr}"
        )
    if len(completed.stdout.encode("utf-8")) > 1_048_576:
        raise MatrixError(f"{mode} --print-queries output exceeded 1 MiB")
    return parse_query_ids(completed.stdout)


def validate_query_id_sets(
    measurement_query_ids: Sequence[int], correctness_query_ids: Sequence[int]
) -> None:
    measurement = set(measurement_query_ids)
    correctness = set(correctness_query_ids)
    if measurement != correctness:
        raise MatrixError(
            "measurement and correctness query ID sets differ: "
            f"measurement_only={sorted(measurement - correctness)}, "
            f"correctness_only={sorted(correctness - measurement)}"
        )


def parse_macos_peak_rss(stderr: str) -> int | None:
    matches = MACOS_RSS_RE.findall(stderr)
    return int(matches[-1]) if matches else None


def resource_wrapped_argv(
    argv: Sequence[str], collect_peak_rss: bool, *, platform: str = sys.platform
) -> tuple[tuple[str, ...], str]:
    if collect_peak_rss and platform == "darwin" and MACOS_TIME.is_file():
        return (str(MACOS_TIME), "-l", *argv), "macos:/usr/bin/time -l"
    if collect_peak_rss:
        return tuple(argv), "unavailable"
    return tuple(argv), "not-collected"


class BoundedCapture:
    """Drain a pipe while retaining a bounded head and tail."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._head_limit = limit // 2
        self._tail_limit = limit - self._head_limit
        self._head = bytearray()
        self._tail = bytearray()
        self.total = 0

    def consume(self, stream: BinaryIO) -> None:
        while chunk := stream.read(64 * 1024):
            self.total += len(chunk)
            head_room = self._head_limit - len(self._head)
            if head_room > 0:
                self._head.extend(chunk[:head_room])
                chunk = chunk[head_room:]
            if chunk and self._tail_limit:
                self._tail.extend(chunk)
                if len(self._tail) > self._tail_limit:
                    del self._tail[: len(self._tail) - self._tail_limit]

    def bytes(self) -> bytes:
        retained = len(self._head) + len(self._tail)
        if self.total <= retained:
            return bytes(self._head + self._tail)
        marker = b"\n... output truncated by matrix runner ...\n"
        if len(marker) >= self._limit:
            return marker[: self._limit]
        available = self._limit - len(marker)
        head_bytes = bytes(self._head[: available // 2])
        tail_keep = available - len(head_bytes)
        tail_bytes = bytes(self._tail[-tail_keep:]) if tail_keep else b""
        return head_bytes + marker + tail_bytes

    @property
    def truncated(self) -> bool:
        return self.total > len(self._head) + len(self._tail)


def run_child(
    config: MatrixConfig,
    spec: ChildSpec,
    sequence: int,
    identity_ref: dict[str, str],
) -> dict[str, object]:
    query_dir = safe_output_path(config.output_dir, "logs", f"q{spec.query_id:06d}")
    query_dir.mkdir(parents=True, exist_ok=True)
    sample = "exact" if spec.sample is None else f"s{spec.sample:04d}"
    stem = f"{sequence:06d}-{sample}-{spec.phase}-{spec.backend}"
    stdout_path = safe_output_path(query_dir, f"{stem}.stdout.log")
    stderr_path = safe_output_path(query_dir, f"{stem}.stderr.log")
    command, rss_source = resource_wrapped_argv(spec.argv, spec.collect_peak_rss)
    env_overrides = environment_changes(spec.backend, spec.partitions)
    env = child_environment(os.environ.copy(), spec.backend, spec.partitions)
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()
    start = time.monotonic_ns()
    try:
        process = subprocess.Popen(
            command,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            start_new_session=os.name == "posix",
        )
    except OSError as err:
        raise MatrixError(f"failed to launch Q{spec.query_id} {spec.backend}: {err}") from err
    assert process.stdout is not None
    assert process.stderr is not None
    stdout_capture = BoundedCapture(config.max_log_bytes)
    stderr_capture = BoundedCapture(config.max_log_bytes)
    stdout_thread = threading.Thread(
        target=stdout_capture.consume, args=(process.stdout,), daemon=True
    )
    stderr_thread = threading.Thread(
        target=stderr_capture.consume, args=(process.stderr,), daemon=True
    )
    stdout_thread.start()
    stderr_thread.start()
    timed_out = False
    try:
        process.wait(timeout=config.timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait()
    stdout_thread.join()
    stderr_thread.join()
    elapsed = (time.monotonic_ns() - start) / 1_000_000_000
    stdout_bytes = stdout_capture.bytes()
    stderr_bytes = stderr_capture.bytes()
    stdout_path.write_bytes(stdout_bytes)
    stderr_path.write_bytes(stderr_bytes)

    peak_rss = None
    if rss_source.startswith("macos:"):
        peak_rss = parse_macos_peak_rss(stderr_bytes.decode("utf-8", errors="replace"))
        if peak_rss is None:
            rss_source += ":unparsed"

    return {
        "record_type": "child",
        "schema_version": 1,
        "sequence": sequence,
        "started_at_utc": started_at,
        "suite": config.suite,
        "query_id": spec.query_id,
        "sample": spec.sample,
        "global_phase": spec.global_phase,
        "phase": spec.phase,
        "backend": spec.backend,
        "partitions": spec.partitions,
        "threads": spec.partitions,
        "cache_protocol": "HOT:symmetric-same-query-same-backend-fresh-process-prewarm",
        "included_in_measurements": spec.included_in_measurements,
        "command": list(command),
        "benchmark_argv": list(spec.argv),
        "environment": env_overrides,
        "exit_status": process.returncode,
        "timed_out": timed_out,
        "elapsed_wall_seconds": elapsed,
        "peak_rss_bytes": peak_rss,
        "rss_source": rss_source,
        "stdout_path": str(stdout_path.relative_to(config.output_dir.resolve())),
        "stderr_path": str(stderr_path.relative_to(config.output_dir.resolve())),
        "stdout_bytes_seen": stdout_capture.total,
        "stderr_bytes_seen": stderr_capture.total,
        "stdout_truncated": stdout_capture.truncated,
        "stderr_truncated": stderr_capture.truncated,
    } | identity_ref


def config_record(
    config: MatrixConfig,
    query_ids: Sequence[int],
    identity_ref: dict[str, str] | None = None,
    correctness_query_ids: Sequence[int] | None = None,
) -> dict[str, object]:
    if correctness_query_ids is None:
        correctness_query_ids = query_ids
    record: dict[str, object] = {
        "record_type": "matrix-config",
        "schema_version": 1,
        "suite": config.suite,
        "query_ids": list(query_ids),
        "measurement_query_ids": list(query_ids),
        "correctness_query_ids": list(correctness_query_ids),
        "samples_per_backend": config.samples,
        "format": FORMAT,
        "input_root_argument": (
            str(config.input_root) if config.input_root is not None else None
        ),
        "partitions": config.partitions,
        "threads": config.partitions,
        "correctness_partitions": config.correctness_partitions,
        "correctness_threads": config.correctness_partitions,
        "bench_options": list(config.bench_options),
        "correctness_bench_options": list(
            benchmark_options(config, correctness=True)
        ),
        "correctness_only_bench_options": list(config.correctness_bench_options),
        "policies": matrix_policy(config),
        "global_phase_order": [
            GLOBAL_PHASE_CORRECTNESS,
            GLOBAL_PHASE_MEASUREMENT,
        ],
        "measurement_requires_marker": CORRECTNESS_SUCCESS_MARKER,
        "cache_protocol": "HOT:symmetric-same-query-same-backend-fresh-process-prewarm",
        "measurement_order": "alternating by query position plus sample index",
        "correctness_protocol": "v1-write-then-push-frontier-verify-canonical-artifact",
        "dataset_mutation": "none",
        "cache_eviction": "none",
        "cold_cache_evidence": False,
        "discovery_command": list(discovery_argv(config)),
        "correctness_discovery_command": list(
            discovery_argv(config, correctness=True)
        ),
        "environment": {
            "VORTEX_USE_SCAN_API": None,
            "DATAFUSION_EXECUTION_TARGET_PARTITIONS": str(config.partitions),
        },
    }
    if identity_ref is not None:
        record.update(identity_ref)
    return record


def validate_plan_size(config: MatrixConfig, query_ids: Sequence[int]) -> None:
    children = len(query_ids) * (2 + 4 * config.samples)
    if children > MAX_PLANNED_CHILDREN:
        raise MatrixError(
            f"matrix would launch {children} query children; maximum is "
            f"{MAX_PLANNED_CHILDREN}"
        )


def dry_run_records(
    config: MatrixConfig,
    query_ids: Sequence[int],
    correctness_query_ids: Sequence[int] | None = None,
) -> Iterable[dict[str, object]]:
    yield config_record(
        config, query_ids, correctness_query_ids=correctness_query_ids
    ) | {"dry_run": True}
    sequence = 0
    for query_id in query_ids:
        for spec in correctness_specs(config, query_id):
            command, rss_source = resource_wrapped_argv(
                spec.argv, spec.collect_peak_rss
            )
            yield {
                "record_type": "planned-child",
                "schema_version": 1,
                "sequence": sequence,
                "suite": config.suite,
                "query_id": query_id,
                "sample": spec.sample,
                "global_phase": spec.global_phase,
                "phase": spec.phase,
                "backend": spec.backend,
                "partitions": spec.partitions,
                "threads": spec.partitions,
                "included_in_measurements": spec.included_in_measurements,
                "command": list(command),
                "benchmark_argv": list(spec.argv),
                "environment": environment_changes(spec.backend, spec.partitions),
                "rss_source": rss_source,
            }
            sequence += 1
    yield correctness_success_marker(sequence, query_ids, planned=True)
    sequence += 1
    for query_position, query_id in enumerate(query_ids):
        for spec in measurement_specs(config, query_id, query_position):
            command, rss_source = resource_wrapped_argv(
                spec.argv, spec.collect_peak_rss
            )
            yield {
                "record_type": "planned-child",
                "schema_version": 1,
                "sequence": sequence,
                "suite": config.suite,
                "query_id": query_id,
                "sample": spec.sample,
                "global_phase": spec.global_phase,
                "phase": spec.phase,
                "backend": spec.backend,
                "partitions": spec.partitions,
                "threads": spec.partitions,
                "included_in_measurements": spec.included_in_measurements,
                "command": list(command),
                "benchmark_argv": list(spec.argv),
                "environment": environment_changes(spec.backend, spec.partitions),
                "rss_source": rss_source,
            }
            sequence += 1


def prepare_output_dir(config: MatrixConfig) -> Path:
    root = config.output_dir.resolve()
    safe_output_path(root, JSONL_NAME)
    if root.exists():
        if not root.is_dir():
            raise MatrixError(f"output path is not a directory: {root}")
        if any(root.iterdir()):
            raise MatrixError(f"output directory must be empty: {root}")
    else:
        root.mkdir(parents=True)
    return root


def validate_binary(binary: Path) -> None:
    if not binary.is_file():
        raise MatrixError(f"datafusion-bench binary does not exist: {binary}")
    if not os.access(binary, os.X_OK):
        raise MatrixError(f"datafusion-bench binary is not executable: {binary}")


def execute_matrix(
    config: MatrixConfig,
    query_ids: Sequence[int],
    correctness_query_ids: Sequence[int],
) -> None:
    validate_input_output_separation(config)
    root = prepare_output_dir(config)
    manifest = collect_run_manifest(config)
    identity_ref = write_run_manifest(root, manifest)
    jsonl_path = safe_output_path(root, JSONL_NAME)
    with jsonl_path.open("x", encoding="utf-8") as jsonl:
        jsonl.write(
            json.dumps(
                config_record(
                    config,
                    query_ids,
                    identity_ref,
                    correctness_query_ids,
                ),
                sort_keys=True,
            )
            + "\n"
        )
        jsonl.flush()
        sequence = 0

        def run_specs(specs: Iterable[ChildSpec], sequence: int) -> int:
            for spec in specs:
                record = run_child(config, spec, sequence, identity_ref)
                jsonl.write(json.dumps(record, sort_keys=True) + "\n")
                jsonl.flush()
                if record["timed_out"]:
                    raise MatrixError(
                        f"Q{spec.query_id} {spec.backend} {spec.phase} timed out; see "
                        f"{record['stderr_path']}"
                    )
                if record["exit_status"] != 0:
                    raise MatrixError(
                        f"Q{spec.query_id} {spec.backend} {spec.phase} failed with exit status "
                        f"{record['exit_status']}; see {record['stderr_path']}"
                    )
                sequence += 1
            return sequence

        for query_id in query_ids:
            sequence = run_specs(correctness_specs(config, query_id), sequence)

        jsonl.write(
            json.dumps(
                correctness_success_marker(sequence, query_ids, identity_ref),
                sort_keys=True,
            )
            + "\n"
        )
        jsonl.flush()
        sequence += 1

        for query_position, query_id in enumerate(query_ids):
            sequence = run_specs(
                measurement_specs(config, query_id, query_position), sequence
            )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        config = parse_args(argv)
        validate_binary(config.binary)
        if not config.dry_run:
            validate_input_output_separation(config)
        query_ids = discover_query_ids(config)
        correctness_query_ids = discover_query_ids(config, correctness=True)
        validate_query_id_sets(query_ids, correctness_query_ids)
        validate_plan_size(config, query_ids)
        if config.dry_run:
            for record in dry_run_records(
                config, query_ids, correctness_query_ids
            ):
                print(json.dumps(record, sort_keys=True))
            return 0
        execute_matrix(config, query_ids, correctness_query_ids)
        return 0
    except MatrixError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
