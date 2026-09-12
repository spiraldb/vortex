from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import io
import json
import os
import subprocess
import tempfile
from pathlib import Path
import unittest

import run_push_frontier_matrix as matrix


def config(output_dir: Path, *, samples: int = 2) -> matrix.MatrixConfig:
    return matrix.MatrixConfig(
        binary=Path("target/release_debug/datafusion-bench"),
        suite="tpch",
        output_dir=output_dir,
        input_root=None,
        samples=samples,
        partitions=8,
        correctness_partitions=1,
        queries="1,6",
        exclude_queries=None,
        bench_options=("scale-factor=1.0", "remote-data-dir=s3://bench/"),
        correctness_bench_options=("queries-file=correctness.sql",),
        runner_prefix="test-matrix",
        timeout_seconds=60.0,
        max_log_bytes=1024,
        dry_run=True,
    )


class MatrixRunnerTests(unittest.TestCase):
    def test_non_dry_run_requires_input_root(self) -> None:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            matrix.parse_args(
                ["tpch", "--output-dir", "matrix", "--partitions", "1"]
            )
        self.assertIn("--input-root is required", stderr.getvalue())

    def test_partition_defaults_split_correctness_from_measurement(self) -> None:
        cfg = matrix.parse_args(
            ["tpch", "--output-dir", "matrix", "--dry-run"]
        )
        self.assertEqual(cfg.partitions, 4)
        self.assertEqual(cfg.correctness_partitions, 1)

    def test_parses_print_queries_strictly(self) -> None:
        self.assertEqual(matrix.parse_query_ids("0\n7\n99\n"), [0, 7, 99])
        for invalid in ("", "1\nQ2\n", "1\n1\n", "-1\n"):
            with self.subTest(invalid=invalid), self.assertRaises(matrix.MatrixError):
                matrix.parse_query_ids(invalid)

    def test_correctness_and_measurement_argv_are_separate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            cfg = config(Path(temporary) / "matrix")
            artifact_dir = cfg.output_dir / "results" / "q000006"
            write = matrix.result_argv(cfg, 6, artifact_dir, verify=False)
            verify = matrix.result_argv(cfg, 6, artifact_dir, verify=True)
            measured = matrix.measured_argv(cfg, 6, 0, "v1")
            self.assertEqual(
                write[:8],
                (
                    str(cfg.binary),
                    "tpch",
                    "--formats",
                    "vortex",
                    "--queries",
                    "6",
                    "--threads",
                    "1",
                ),
            )
            self.assertEqual(measured[7], "8")
            self.assertIn("--write-result-artifacts", write)
            self.assertIn("--verify-result-artifacts", verify)
            self.assertEqual(write.count("--opt"), 3)
            self.assertIn("queries-file=correctness.sql", write)
            self.assertNotIn("queries-file=correctness.sql", measured)
            self.assertEqual(measured.count("--opt"), 2)
            self.assertEqual(
                matrix.discovery_argv(cfg),
                (
                    str(cfg.binary),
                    "tpch",
                    "--print-queries",
                    "--queries",
                    "1,6",
                    "--opt",
                    "scale-factor=1.0",
                    "--opt",
                    "remote-data-dir=s3://bench/",
                ),
            )
            self.assertEqual(
                matrix.discovery_argv(cfg, correctness=True),
                (
                    str(cfg.binary),
                    "tpch",
                    "--print-queries",
                    "--queries",
                    "1,6",
                    "--opt",
                    "scale-factor=1.0",
                    "--opt",
                    "remote-data-dir=s3://bench/",
                    "--opt",
                    "queries-file=correctness.sql",
                ),
            )

    def test_measurement_order_alternates_by_query_and_sample(self) -> None:
        self.assertEqual(matrix.measurement_order(0, 0), ("v1", "push-frontier"))
        self.assertEqual(matrix.measurement_order(0, 1), ("push-frontier", "v1"))
        self.assertEqual(matrix.measurement_order(1, 0), ("push-frontier", "v1"))
        self.assertEqual(matrix.measurement_order(1, 1), ("v1", "push-frontier"))

    def test_child_environment_unsets_scan_api_selector(self) -> None:
        child = matrix.child_environment(
            {"VORTEX_USE_SCAN_API": "1", "PRESERVED": "yes"}, "push-frontier", 8
        )
        self.assertNotIn("VORTEX_USE_SCAN_API", child)
        self.assertEqual(child["VORTEX_SCAN_BACKEND"], "push-frontier")
        self.assertEqual(child["DATAFUSION_EXECUTION_TARGET_PARTITIONS"], "8")
        self.assertEqual(child["PRESERVED"], "yes")
        self.assertIsNone(
            matrix.environment_changes("push-frontier", 8)["VORTEX_USE_SCAN_API"]
        )

    def test_each_measurement_has_identical_fresh_prewarm(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            specs = list(
                matrix.query_specs(
                    config(Path(temporary) / "matrix", samples=2), 6, 0
                )
            )
        self.assertEqual([spec.phase for spec in specs[:2]], ["exact-write", "exact-verify"])
        self.assertEqual([spec.partitions for spec in specs[:2]], [1, 1])
        timed_specs = specs[2:]
        self.assertEqual(len(timed_specs), 8)
        self.assertTrue(all(spec.partitions == 8 for spec in timed_specs))
        for prewarm, measured in zip(timed_specs[::2], timed_specs[1::2]):
            self.assertEqual(prewarm.phase, "hot-prewarm")
            self.assertEqual(measured.phase, "measure")
            self.assertEqual(prewarm.backend, measured.backend)
            self.assertEqual(prewarm.query_id, measured.query_id)
            self.assertEqual(prewarm.sample, measured.sample)
            self.assertEqual(prewarm.argv, measured.argv)
            self.assertFalse(prewarm.collect_peak_rss)
            self.assertTrue(measured.collect_peak_rss)

    def test_macos_peak_rss_parser_never_invents_a_value(self) -> None:
        stderr = "  1.23 real 0.90 user 0.10 sys\n  123456 maximum resident set size\n"
        self.assertEqual(matrix.parse_macos_peak_rss(stderr), 123456)
        self.assertIsNone(matrix.parse_macos_peak_rss("no RSS line here"))
        command, source = matrix.resource_wrapped_argv(
            ("datafusion-bench", "tpch"), True, platform="linux"
        )
        self.assertEqual(command, ("datafusion-bench", "tpch"))
        self.assertEqual(source, "unavailable")

    def test_paths_cannot_escape_output_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "matrix"
            self.assertEqual(
                matrix.safe_output_path(root, "logs", "q000001").parent.name,
                "logs",
            )
            with self.assertRaises(matrix.MatrixError):
                matrix.safe_output_path(root, "..", "outside")

    def test_input_manifest_is_deterministic_and_rejects_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            input_root = Path(temporary) / "input"
            (input_root / "a").mkdir(parents=True)
            (input_root / "z.txt").write_bytes(b"last")
            (input_root / "a" / "first.bin").write_bytes(b"first")
            identity = matrix.collect_input_identity(input_root.resolve())
            self.assertEqual(
                [entry["relative_path"] for entry in identity["files"]],
                ["a/first.bin", "z.txt"],
            )
            self.assertEqual(identity["file_count"], 2)
            self.assertEqual(identity["total_size_bytes"], 9)
            self.assertEqual(
                identity["files"][0]["sha256"], hashlib.sha256(b"first").hexdigest()
            )
            self.assertEqual(identity, matrix.collect_input_identity(input_root.resolve()))

            os.symlink(input_root / "z.txt", input_root / "linked")
            with self.assertRaisesRegex(matrix.MatrixError, "symlink"):
                matrix.collect_input_identity(input_root.resolve())

    def test_input_and_output_directories_must_be_disjoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            input_root = Path(temporary) / "input"
            input_root.mkdir()
            cfg = dataclasses.replace(
                config(input_root / "matrix"), input_root=input_root, dry_run=False
            )
            with self.assertRaisesRegex(matrix.MatrixError, "must be disjoint"):
                matrix.validate_input_output_separation(cfg)

    def test_run_manifest_captures_binary_input_and_dirty_git_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repo"
            repository.mkdir()
            subprocess.run(
                ("git", "init", "-q", str(repository)), check=True, capture_output=True
            )
            tracked = repository / "tracked.txt"
            tracked.write_text("committed\n", encoding="utf-8")
            subprocess.run(
                ("git", "-C", str(repository), "add", "tracked.txt"),
                check=True,
                capture_output=True,
            )
            subprocess.run(
                (
                    "git",
                    "-C",
                    str(repository),
                    "-c",
                    "user.name=Matrix Test",
                    "-c",
                    "user.email=matrix@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "-q",
                    "-m",
                    "fixture",
                ),
                check=True,
                capture_output=True,
            )
            tracked.write_text("dirty\n", encoding="utf-8")

            input_root = root / "input"
            input_root.mkdir()
            (input_root / "data.vortex").write_bytes(b"tiny input")
            binary = root / "datafusion-bench"
            binary.write_bytes(b"tiny executable")
            binary.chmod(0o755)
            output_root = root / "output"
            cfg = dataclasses.replace(
                config(output_root),
                binary=binary,
                input_root=input_root,
                dry_run=False,
            )

            manifest = matrix.collect_run_manifest(cfg, repository_hint=repository)
            self.assertEqual(
                manifest["benchmark_executable"]["path"], str(binary.resolve())
            )
            self.assertEqual(
                manifest["benchmark_executable"]["sha256"],
                hashlib.sha256(b"tiny executable").hexdigest(),
            )
            self.assertTrue(manifest["git"]["dirty"])
            self.assertEqual(len(manifest["git"]["head"]), 40)
            self.assertEqual(len(manifest["git"]["working_tree_sha256"]), 64)
            self.assertEqual(manifest["matrix_policy"]["measurement"]["threads"], 8)
            self.assertEqual(manifest["matrix_policy"]["correctness"]["threads"], 1)
            self.assertEqual(
                manifest["matrix_policy"]["correctness"]["bench_options"],
                [
                    "scale-factor=1.0",
                    "remote-data-dir=s3://bench/",
                    "queries-file=correctness.sql",
                ],
            )

            output_root.mkdir()
            reference = matrix.write_run_manifest(output_root, manifest)
            contents = (output_root / matrix.RUN_MANIFEST_NAME).read_bytes()
            self.assertEqual(
                reference["run_manifest_sha256"], hashlib.sha256(contents).hexdigest()
            )
            record = matrix.config_record(cfg, [1], reference)
            self.assertEqual(record["run_manifest_sha256"], reference["run_manifest_sha256"])
            self.assertEqual(
                record["input_manifest_sha256"],
                manifest["input"]["manifest_sha256"],
            )

    def test_bounded_capture_keeps_output_within_limit(self) -> None:
        capture = matrix.BoundedCapture(64)
        capture.consume(io.BytesIO(b"a" * 200))
        self.assertTrue(capture.truncated)
        self.assertLessEqual(len(capture.bytes()), 64)

    def test_dry_run_records_execute_no_children(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            cfg = config(Path(temporary) / "matrix", samples=1)
            records = list(matrix.dry_run_records(cfg, [1]))
            self.assertFalse(cfg.output_dir.exists())
        self.assertTrue(records[0]["dry_run"])
        self.assertEqual(records[0]["measurement_query_ids"], [1])
        self.assertEqual(records[0]["correctness_query_ids"], [1])
        self.assertEqual(records[0]["policies"]["measurement"]["threads"], 8)
        self.assertEqual(records[0]["policies"]["correctness"]["threads"], 1)
        self.assertIsNone(records[0]["environment"]["VORTEX_USE_SCAN_API"])
        self.assertEqual(
            [(record["phase"], record["backend"]) for record in records[1:]],
            [
                ("exact-write", "v1"),
                ("exact-verify", "push-frontier"),
                ("hot-prewarm", "v1"),
                ("measure", "v1"),
                ("hot-prewarm", "push-frontier"),
                ("measure", "push-frontier"),
            ],
        )
        self.assertTrue(
            all(
                record["environment"]["VORTEX_USE_SCAN_API"] is None
                for record in records[1:]
            )
        )
        self.assertEqual([record["partitions"] for record in records[1:3]], [1, 1])
        self.assertTrue(all(record["partitions"] == 8 for record in records[3:]))

    def test_query_id_mismatch_fails_before_output_or_query_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_binary = root / "fake-datafusion-bench"
            fake_binary.write_text(
                "#!/usr/bin/env python3\n"
                "import sys\n"
                "if '--print-queries' not in sys.argv:\n"
                "    raise SystemExit('query execution was attempted')\n"
                "if 'queries-file=correctness.sql' in sys.argv:\n"
                "    print('2')\n"
                "else:\n"
                "    print('1')\n",
                encoding="utf-8",
            )
            fake_binary.chmod(0o755)
            input_root = root / "input"
            input_root.mkdir()
            (input_root / "data.vortex").write_bytes(b"not hashed")
            output_dir = root / "matrix"
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_status = matrix.main(
                    [
                        "clickbench",
                        "--binary",
                        str(fake_binary),
                        "--output-dir",
                        str(output_dir),
                        "--input-root",
                        str(input_root),
                        "--correctness-opt",
                        "queries-file=correctness.sql",
                    ]
                )
            self.assertEqual(exit_status, 1)
            self.assertFalse(output_dir.exists())
            self.assertIn("query ID sets differ", stderr.getvalue())

    def test_main_dry_run_only_executes_query_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_binary = root / "fake-datafusion-bench"
            fake_binary.write_text(
                "#!/usr/bin/env python3\n"
                "import sys\n"
                "if '--print-queries' in sys.argv:\n"
                "    print('1')\n"
                "    raise SystemExit(0)\n"
                "raise SystemExit('query execution was attempted')\n",
                encoding="utf-8",
            )
            fake_binary.chmod(0o755)
            output_dir = root / "matrix"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_status = matrix.main(
                    [
                        "tpch",
                        "--binary",
                        str(fake_binary),
                        "--output-dir",
                        str(output_dir),
                        "--partitions",
                        "8",
                        "--samples",
                        "1",
                        "--input-root",
                        str(root / "intentionally-missing-input"),
                        "--dry-run",
                    ]
                )
            self.assertEqual(exit_status, 0)
            self.assertFalse(output_dir.exists())
            records = [json.loads(line) for line in stdout.getvalue().splitlines()]
            self.assertEqual(records[0]["record_type"], "matrix-config")
            self.assertTrue(records[0]["dry_run"])


if __name__ == "__main__":
    unittest.main()
