# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

SPEC = importlib.util.spec_from_file_location("bench_io", Path(__file__).parents[1] / "bench-io.py")
assert SPEC is not None and SPEC.loader is not None
BENCH_IO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH_IO)


class BenchIoTests(unittest.TestCase):
    def test_native_byte_occupancy_weights_size_and_keeps_missing_lengths_unknown(self):
        iteration = {
            "begin_ns": 100,
            "ts_ns": 150,
            "reads": [
                {"start_ns": 110, "prepare_ns": 20, "queue_ns": 10, "read_ns": 20, "length": 1024},
                {"start_ns": 130, "prepare_ns": 0, "queue_ns": 0, "read_ns": 20, "length": 4096},
                {"start_ns": 130, "prepare_ns": 0, "queue_ns": 0, "read_ns": 20},
            ],
        }
        native = BENCH_IO.native_queue_summary(iteration)["phases"]["native_read"]
        self.assertEqual(native["observed_intervals"], 3)
        self.assertEqual(native["byte_observed_intervals"], 2)
        self.assertEqual(native["peak_depth"], 3)
        self.assertEqual(native["peak_observed_bytes"], 5120)
        self.assertEqual(native["mean_observed_bytes"], 1843.2)
        iteration["reads"] = iteration["reads"][-1:]
        native = BENCH_IO.native_queue_summary(iteration)["phases"]["native_read"]
        self.assertIsNone(native["peak_observed_bytes"])
        self.assertIsNone(native["mean_observed_bytes"])

    def test_blocking_allocation_is_separate_from_queue_and_read_time(self):
        log = "\n".join(
            [
                "IO_ITERATION_BEGIN ts_ns=100",
                "local object-store read path=file offset=0 length=4096 start_ns=100 "
                "get_ns=5 admission_ns=5 allocation_ns=0 prepare_ns=10 queue_ns=10 "
                "blocking_allocation_ns=15 read_ns=20 resume_ns=5",
                "IO_ITERATION_END rows=1 query_ns=80 ts_ns=180",
            ]
        )
        result = BENCH_IO.diagnostics(log)
        self.assertEqual(result["read_phases_us"]["allocation_ns"]["max"], 0)
        self.assertEqual(result["read_phases_us"]["blocking_allocation_ns"]["max"], 0.015)
        queues = result["observed_native_queues"]
        self.assertEqual(queues["first_native_start_ns"], 35)
        self.assertEqual(queues["first_native_completion_ns"], 55)
        self.assertEqual(queues["phases"]["blocking_queue"]["busy_union_ns"], 10)
        self.assertEqual(queues["phases"]["blocking_allocation"]["busy_union_ns"], 15)
        self.assertEqual(queues["phases"]["native_read"]["busy_union_ns"], 20)

    def test_per_process_quiet_excludes_waiting_but_retains_measured_contention(self):
        phase = "waiting"
        args = SimpleNamespace(
            per_process_quiet=True,
            output=Path("output"),
            data_root=Path("data"),
            evict_local_files=True,
            quiet_seconds=2,
        )

        def quiet(*_: object):
            nonlocal phase
            phase = "waiting"
            self.assertEqual(BENCH_IO.competing_processes(), [(99, "other-bench")])
            phase = "clean"

        def run(_: object, workload: str, config: tuple[str, str], tag: str) -> dict[str, int]:
            nonlocal phase
            self.assertEqual(phase, "clean")
            self.assertEqual(tag, "round3-attempt7")
            phase = "contended" if config[0] == "candidate" else "clean"
            return {"median_ms": 1}

        def competitors() -> list[tuple[int, str]]:
            return [(55, "cargo")] if phase == "contended" else [(99, "other-bench")] if phase == "waiting" else []

        with (
            mock.patch.object(BENCH_IO, "wait_for_quiet", side_effect=quiet) as wait,
            mock.patch.object(BENCH_IO, "run", side_effect=run),
            mock.patch.object(BENCH_IO, "competing_processes", side_effect=competitors),
        ):
            results, observed = BENCH_IO.measured_round(
                args, "tpch-q19", [("baseline", "direct"), ("candidate", "direct")], 3, 7
            )
        self.assertEqual(wait.call_count, 2)
        self.assertEqual(observed, [(55, "cargo")])
        self.assertEqual(set(results), {"baseline-direct", "candidate-direct"})

    def test_native_queue_occupancy_clips_post_query_work_and_preserves_unknown_startup(self):
        iteration = {
            "begin_ns": 100,
            "ts_ns": 150,
            "reads": [
                {"start_ns": 110, "get_ns": 5, "admission_ns": 15, "prepare_ns": 20, "queue_ns": 10, "read_ns": 20},
                {"start_ns": 130, "prepare_ns": 0, "queue_ns": 0, "read_ns": 20},
                {"read_ns": 999},
            ],
        }
        result = BENCH_IO.native_queue_summary(iteration)
        self.assertEqual(result["first_native_start_ns"], 30)
        self.assertEqual(result["first_native_completion_ns"], 50)
        native = result["phases"]["native_read"]
        self.assertEqual(native["observed_intervals"], 2)
        self.assertEqual(native["mean_depth"], 0.6)
        self.assertEqual(native["peak_depth"], 2)
        self.assertEqual(native["busy_union_ns"], 20)
        self.assertIsNone(BENCH_IO.native_queue_summary({"reads": []}))

    def test_failed_cache_probe_precedes_admission_and_cached_hits_are_counted(self):
        log = "\n".join(
            [
                "IO_ITERATION_BEGIN ts_ns=120",
                "local object-store read path=file offset=0 length=1024 start_ns=100 "
                "get_ns=5 cache_probe_ns=10 admission_ns=20 prepare_ns=35 queue_ns=0 "
                "read_ns=10 resume_ns=0 cache_probe_hit=false",
                "local object-store read path=file offset=1024 length=1024 start_ns=100 "
                "get_ns=5 cache_probe_ns=0 admission_ns=0 prepare_ns=7 queue_ns=0 "
                "read_ns=3 resume_ns=0 cache_probe_hit=true",
                "IO_ITERATION_END rows=1 query_ns=30 ts_ns=150",
            ]
        )
        result = BENCH_IO.diagnostics(log)
        self.assertEqual(result["local_nowait"], {"observed_reads": 2, "cached_reads": 1, "fallback_probes": 1})
        self.assertEqual(result["read_phases_us"]["cache_probe_ns"]["max"], 0.01)
        admission = result["observed_native_queues"]["phases"]["admission"]
        self.assertEqual(admission["busy_union_ns"], 15)
        self.assertEqual(admission["mean_depth"], 0.5)

    def test_sleeping_coordinator_exemption_still_detects_native_children(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for pid, command, executable, parent in (
                (999991, "python3", "python3\0bench.py\0", 1),
                (999992, "datafusion", "/home/user/votex/target/datafusion-bench\0", 999991),
            ):
                proc = root / str(pid)
                proc.mkdir()
                fields = ["0"] * 20
                fields[0], fields[1], fields[11], fields[19] = "S", str(parent), "100", "7"
                (proc / "stat").write_text(f"{pid} ({command}) " + " ".join(fields))
                (proc / "comm").write_text(command)
                (proc / "cmdline").write_bytes(executable.encode())
            with (
                mock.patch.object(
                    BENCH_IO, "Path", side_effect=lambda value: root if value == "/proc" else Path(value)
                ),
                mock.patch.object(BENCH_IO.time, "monotonic", side_effect=[10, 12, 14]),
                mock.patch.dict(BENCH_IO.COORDINATOR_SAMPLES, clear=True),
            ):
                self.assertEqual(BENCH_IO.competing_processes(), [(999991, "python3"), (999992, "datafusion")])
                self.assertEqual(BENCH_IO.competing_processes(), [(999992, "datafusion")])
                for file in (root / "999992").iterdir():
                    file.unlink()
                (root / "999992").rmdir()
                self.assertEqual(BENCH_IO.competing_processes(), [])

    def test_native_phase_summaries_keep_missing_samples_out_of_size_groups(self):
        log = "\n".join(
            [
                "IO_ITERATION_BEGIN",
                "local object-store read path=file offset=0 length=1024 read_ns=1000 admission_ns=0",
                "local object-store read path=file offset=1024 length=1024 read_ns=2000",
                "local object-store read path=file offset=2048 length=1024 read_ns=3000 admission_ns=6000",
                "IO_ITERATION_END rows=1 query_ns=100000",
            ]
        )
        result = BENCH_IO.diagnostics(log)
        admission = result["read_phases_us"]["admission_ns"]
        self.assertEqual(admission["samples"], 2)
        self.assertEqual(admission["median"], 3)
        self.assertEqual(admission["min"], 0)
        self.assertIsNone(result["read_phases_median_us"]["allocation_ns"])
        small = result["read_phases_by_size"]["up_to_4KiB"]
        self.assertEqual(small["reads"], 3)
        self.assertEqual(small["bytes"], 3072)
        self.assertEqual(small["phases_us"]["admission_ns"], admission)
        self.assertEqual(result["read_phases_by_size"]["above_1MiB"]["reads"], 0)

    def test_idle_reservation_timer_does_not_hide_a_real_native_benchmark(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for pid, command, executable in (
                (
                    999991,
                    "datafusion-comp",
                    "/mnt/vortex-ssd/vortex/target/datafusion-compress-bench-reservation\0" + "3600\0",
                ),
                (999992, "compress-before", "/mnt/vortex-ssd/vortex/bin/compress-before\0"),
            ):
                proc = root / str(pid)
                proc.mkdir()
                fields = ["0"] * 20
                fields[0], fields[1], fields[11], fields[19] = "S", "1", "100", "7"
                (proc / "stat").write_text(f"{pid} ({command}) " + " ".join(fields))
                (proc / "comm").write_text(command)
                (proc / "cmdline").write_bytes(executable.encode())
            with (
                mock.patch.object(
                    BENCH_IO, "Path", side_effect=lambda value: root if value == "/proc" else Path(value)
                ),
                mock.patch.object(BENCH_IO.time, "monotonic", side_effect=[10, 12]),
                mock.patch.dict(BENCH_IO.COORDINATOR_SAMPLES, clear=True),
            ):
                self.assertEqual(
                    BENCH_IO.competing_processes(), [(999991, "datafusion-comp"), (999992, "compress-before")]
                )
                self.assertEqual(BENCH_IO.competing_processes(), [(999992, "compress-before")])

    def test_idle_coordinators_do_not_block_each_others_quiet_gate(self):
        self.assertTrue(BENCH_IO.coordinator_is_idle("S", 7, 1.01, 12, (7, 1.0, 10)))
        for state, started, cpu, now, previous in [
            ("R", 7, 1.01, 12, (7, 1.0, 10)),
            ("D", 7, 1.01, 12, (7, 1.0, 10)),
            ("S", 7, 1.1, 12, (7, 1.0, 10)),
            ("S", 8, 1.01, 12, (7, 1.0, 10)),
            ("S", 7, 1.01, 12, None),
            ("S", 7, 1.01, 10, (7, 1.0, 10)),
        ]:
            with self.subTest(state=state, previous=previous):
                self.assertFalse(BENCH_IO.coordinator_is_idle(state, started, cpu, now, previous))

    def test_pressure_window_uses_totals_and_handles_unavailable_or_reset_counters(self):
        before = {"memory": "some avg10=20.0 total=100\nfull avg10=15.0 total=90"}
        after = {"memory": "some avg10=15.0 total=112\nfull avg10=10.0 total=99"}
        self.assertEqual(
            BENCH_IO.counter_delta(BENCH_IO.pressure_totals(before), BENCH_IO.pressure_totals(after)),
            {"memory/some": 12, "memory/full": 9},
        )
        self.assertIsNone(BENCH_IO.counter_delta({}, {}))
        self.assertIsNone(BENCH_IO.counter_delta({"compact_stall": 5}, {"compact_stall": 2}))
        self.assertIsNone(BENCH_IO.counter_delta({"compact_stall": 5}, {}))

    def test_cold_gate_waits_for_current_device_io_and_ignores_historical_load(self):
        devices = [{"counters": [0] * 8 + [1] + [0] * 2}] + [{"counters": [0] * 11}] * 6
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                mock.patch.object(BENCH_IO.time, "sleep") as sleep,
                mock.patch.object(BENCH_IO.os, "getloadavg", return_value=(80, 40, 20)),
                mock.patch.object(BENCH_IO, "cpu_usage", return_value={"busy_fraction": 0, "iowait_fraction": 0}),
                mock.patch.object(BENCH_IO, "pressure_snapshot", return_value={}),
                mock.patch.object(BENCH_IO, "competing_processes", return_value=[]),
                mock.patch.object(BENCH_IO, "storage_snapshot", side_effect=devices),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                BENCH_IO.wait_for_quiet(root, root, quiet_seconds=12)
                self.assertEqual(sleep.call_count, 7)
                sleep.assert_called_with(2)
            observations = [json.loads(line) for line in (root / "quiet-samples.jsonl").read_text().splitlines()]
        self.assertEqual([item["steady"] for item in observations], [0, 1, 2, 3, 4, 5, 6])
        self.assertTrue(all(not item["historical_load_gate"] for item in observations))
        self.assertTrue(all(item["quiet_seconds"] == 12 for item in observations))
        self.assertFalse(observations[0]["storage_device_idle"])

    def test_cold_gate_uses_current_pressure_instead_of_stale_ten_second_average(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                mock.patch.object(BENCH_IO.time, "sleep"),
                mock.patch.object(BENCH_IO.time, "monotonic", side_effect=range(7)),
                mock.patch.object(BENCH_IO.os, "getloadavg", return_value=(80, 40, 20)),
                mock.patch.object(BENCH_IO, "cpu_usage", return_value={"busy_fraction": 0, "iowait_fraction": 0}),
                mock.patch.object(BENCH_IO, "pressure_snapshot", return_value={"io": "full avg10=90.0 total=100"}),
                mock.patch.object(BENCH_IO, "competing_processes", return_value=[]),
                mock.patch.object(BENCH_IO, "storage_snapshot", return_value={"counters": [0] * 11}),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                BENCH_IO.wait_for_quiet(root, root, quiet_seconds=12)
            observations = [json.loads(line) for line in (root / "quiet-samples.jsonl").read_text().splitlines()]
        self.assertEqual(len(observations), 6)
        self.assertTrue(all(item["io_pressure_gate"] == "current_window" for item in observations))
        self.assertTrue(all(item["io_full_window_fraction"] == 0 for item in observations))

    def test_noise_detection_catches_native_benchmarks_without_bench_in_their_name(self):
        self.assertTrue(
            BENCH_IO.is_competing_process(
                "to_arrow", "/home/ec2-user/vortex-2/target/release/deps/to_arrow-123\0--bench\0"
            )
        )
        self.assertTrue(BENCH_IO.is_competing_process("rustc", "/opt/rustc\0"))
        self.assertTrue(BENCH_IO.is_competing_process("perf", "/usr/bin/perf\0report\0"))
        self.assertTrue(
            BENCH_IO.is_competing_process("compress-before", "/mnt/vortex-ssd/vortex-scanning-v2/bin/compress-before\0")
        )
        self.assertTrue(BENCH_IO.is_competing_process("saved-baseline", "/mnt/vortex-ssd/votex-3/bin/saved-baseline\0"))
        self.assertTrue(BENCH_IO.is_competing_process("compress-after", "/tmp/compress-after\0"))
        self.assertFalse(BENCH_IO.is_competing_process("sshd", "/usr/sbin/sshd\0"))

    def test_storage_metrics_distinguish_cumulative_counters_from_current_queue_gauge(self):
        before = {"device": "disk", "ts_ns": 0, "counters": [10, 0, 100, 200, 5, 0, 20, 20, 99, 600, 1000]}
        after = {
            "device": "disk",
            "ts_ns": 1_000_000_000,
            "counters": [14, 0, 108, 240, 6, 0, 24, 24, 0, 750, 2000],
        }
        result = BENCH_IO.storage_delta(before, after)
        self.assertEqual(result["read_operations"], 4)
        self.assertEqual(result["read_bytes"], 4096)
        self.assertEqual(result["write_bytes"], 2048)
        self.assertEqual(result["read_latency_mean_ms"], 10)
        self.assertEqual(result["queue_depth_mean"], 1)
        self.assertAlmostEqual(result["read_MB_s"], 0.004096)

    def test_storage_counter_reset_or_unavailable_device_is_not_reported_as_zero(self):
        before = {"device": "disk", "ts_ns": 0, "counters": [10] * 11}
        after = {"device": "disk", "ts_ns": 1_000_000, "counters": [0] * 11}
        self.assertIsNone(BENCH_IO.storage_delta(before, after))
        self.assertIsNone(BENCH_IO.storage_delta(None, after))
        self.assertIsNone(BENCH_IO.storage_delta(before, None))

    def test_required_device_rejects_other_storage_and_memory_filesystems(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for snapshot in ({"device": "other-disk"}, None):
                with (
                    self.subTest(snapshot=snapshot),
                    mock.patch.object(BENCH_IO, "storage_snapshot", return_value=snapshot),
                    self.assertRaisesRegex(ValueError, "expected test-ssd"),
                ):
                    BENCH_IO.storage_locations({"temporary": root}, "test-ssd")

    @unittest.skipUnless(sys.platform.startswith("linux"), "Requires Linux page-cache accounting")
    def test_eviction_removes_resident_pages_and_inspection_does_not_reload_them(self):
        # Honor a selected test disk; the repository is the fallback when /tmp is tmpfs.
        with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR", Path(__file__).parents[2])) as directory:
            root = Path(directory)
            path = root / "data.vortex"
            with path.open("wb") as file:
                file.write(b"x" * (2 * os.sysconf("SC_PAGE_SIZE") + 1))
                file.flush()
                os.fsync(file.fileno())
            (root / "empty.vortex").touch()
            self.assertEqual(BENCH_IO.page_cache_residency([path])["resident_pages"], 3)
            result = BENCH_IO.evict_local_files(root)
            self.assertEqual(result["after"]["files"], 2)
            self.assertEqual(result["after"]["resident_pages"], 0)
            self.assertEqual(BENCH_IO.page_cache_residency([path])["resident_pages"], 0)

    def test_eviction_rejects_a_still_warm_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "data.vortex").write_bytes(b"data")
            with (
                mock.patch.object(BENCH_IO, "page_cache_residency", return_value={"resident_fraction": 0.5}),
                mock.patch.object(BENCH_IO.os, "posix_fadvise"),
                self.assertRaisesRegex(ValueError, "dataset pages resident"),
            ):
                BENCH_IO.evict_local_files(root)

    def test_saved_binary_control_uses_the_same_runtime_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(
                binary=root / "candidate",
                baseline_binary=root / "previous",
                data_root=root,
                output=root,
                scale_factor="10.0",
                iterations=3,
                diagnostic_iterations=1,
                discard_iterations=2,
                local_read_concurrency=32,
                driver_trace=False,
                evict_local_files=False,
            )
            commands = []
            environments = []

            def execute(command: list[str], **kwargs: object):
                commands.append(command[0])
                environments.append(kwargs["env"])
                Path(command[command.index("-o") + 1]).write_text(json.dumps({"all_runtimes": [1, 2, 3]}))

            with (
                mock.patch.object(BENCH_IO.subprocess, "run", side_effect=execute),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                BENCH_IO.run(args, "tpch-q19", ("v2-previous", "direct"), "test")
                BENCH_IO.run(args, "tpch-q19", ("v2-unlimited", "direct"), "test")
                BENCH_IO.run(args, "tpch-q19", ("v2-previous-boxed", "direct"), "boxed")
        self.assertEqual(commands, [str(args.baseline_binary), str(args.binary), str(args.baseline_binary)])
        self.assertEqual(environments[0], environments[1])
        self.assertEqual(environments[2]["VORTEX_SCAN_IO_INLINE_FETCH"], "0")
        self.assertEqual(environments[2]["VORTEX_SCAN_GROUP_SPARSE_PROJECTION"], "0")
        self.assertEqual(environments[2]["VORTEX_SCAN_IO_FORGET_PRUNED_PROJECTION"], "0")

    def test_archived_cost_flags_cannot_change_saved_binary_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(
                binary=root / "candidate",
                baseline_binary=root / "previous",
                data_root=root,
                output=root,
                scale_factor="10.0",
                iterations=1,
                diagnostic_iterations=1,
                discard_iterations=0,
                local_read_concurrency=32,
                driver_trace=False,
                evict_local_files=False,
            )
            commands = []
            environments = []

            def execute(command: list[str], **kwargs: object):
                commands.append(command[0])
                environments.append(kwargs["env"])
                Path(command[command.index("-o") + 1]).write_text(json.dumps({"all_runtimes": [1]}))

            ambient = {
                "VORTEX_SCAN_IO_COALESCE_COST_BYTES": "291100",
                "VORTEX_SCAN_IO_COALESCE_USE_PERCENT": "10",
                "VORTEX_SCAN_IO_COALESCE_SPECULATIVE_BYTES": "65536",
            }
            with (
                mock.patch.dict(os.environ, ambient),
                mock.patch.object(BENCH_IO.subprocess, "run", side_effect=execute),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                BENCH_IO.run(args, "tpch-q19", ("v2-previous-demanded", "direct"), "previous")
                BENCH_IO.run(args, "tpch-q19", ("v2-io-file-handle-demanded-native-32", "direct"), "current")
            self.assertEqual(commands, [str(args.baseline_binary), str(args.binary)])
            self.assertEqual(environments[0], environments[1])
            self.assertTrue(all(key not in environments[0] for key in ambient))

    def test_two_variants_alternate_first_position(self):
        configs = [("baseline", "direct"), ("candidate", "direct")]
        rounds = [BENCH_IO.configuration_order(configs, idx) for idx in range(4)]
        self.assertEqual([order[0][0] for order in rounds], ["baseline", "candidate", "candidate", "baseline"])
        self.assertEqual(configs[0][0], "baseline")

    def test_each_variant_visits_every_position(self):
        configs = [(name, "direct") for name in ("a", "b", "c")]
        rounds = [BENCH_IO.configuration_order(configs, idx) for idx in range(6)]
        for position in range(3):
            self.assertEqual(sorted(order[position][0] for order in rounds), ["a", "a", "b", "b", "c", "c"])

    def test_cpu_usage_keeps_iowait_out_of_busy_time_and_records_steal(self):
        usage = BENCH_IO.cpu_usage([0] * 8, [10, 0, 10, 50, 20, 0, 0, 10])
        self.assertAlmostEqual(usage["busy_fraction"], 0.3)
        self.assertAlmostEqual(usage["iowait_fraction"], 0.2)
        self.assertAlmostEqual(usage["steal_fraction"], 0.1)
        self.assertIsNone(BENCH_IO.cpu_usage(None, None))

    def test_contended_round_is_retained_but_excluded_and_diagnostics_follow_timings(self):
        calls = []
        variants = ["v2-project-early", "v2-project-late"]
        observed = iter([[(123, "cargo")], [], []])
        times = iter([100.0, 10.0, 12.0])

        def round_result(*_: object) -> tuple[dict[str, dict[str, Any]], list[tuple[int, str]]]:
            calls.append("timing")
            value = next(times)
            return {f"{v}-direct": {"measured_ms": [value], "median_ms": value} for v in variants}, next(observed)

        def diagnostic(*_: object, **kwargs: object) -> dict[str, int]:
            self.assertTrue(kwargs["diagnostic"])
            calls.append("diagnostic")
            return {"local_reads": 1, "requested_bytes": 4, "repeated_bytes": 0}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "binary"
            binary.write_bytes(b"test binary")
            output = root / "results"
            argv = [
                "bench-io.py",
                "--binary",
                str(binary),
                "--data-root",
                str(root),
                "--output",
                str(output),
                "--workloads",
                "tpch-q19",
                "--variants",
                *variants,
                "--modes",
                "direct",
                "--rounds",
                "2",
                "--iterations",
                "3",
                "--wait-for-quiet",
                "--repeat-contended-rounds",
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(BENCH_IO, "measured_round", side_effect=round_result),
                mock.patch.object(BENCH_IO, "run", side_effect=diagnostic),
                mock.patch.object(BENCH_IO, "competing_processes", return_value=[]),
                mock.patch.object(BENCH_IO, "wait_for_quiet") as wait,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                BENCH_IO.main()
            summary = json.loads((output / "summary.json").read_text())
            with (
                mock.patch.object(sys, "argv", [*argv, "--resume"]),
                mock.patch.object(BENCH_IO, "measured_round") as resumed_round,
                mock.patch.object(BENCH_IO, "run") as resumed_diagnostic,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                BENCH_IO.main()
            resumed_round.assert_not_called()
            resumed_diagnostic.assert_not_called()
            self.assertEqual(json.loads((output / "summary.json").read_text()), summary)
            binary.write_bytes(b"different binary")
            with (
                mock.patch.object(sys, "argv", [*argv, "--resume"]),
                self.assertRaisesRegex(ValueError, "sha256 changed"),
            ):
                BENCH_IO.main()
            self.assertEqual(json.loads((output / "summary.json").read_text()), summary)
        self.assertEqual([a["accepted"] for a in summary["attempts"]], [False, True, True])
        for entry in summary["results"]["tpch-q19"].values():
            self.assertEqual(entry["median_ms"], 11)
            self.assertEqual(len(entry["rounds"]), 2)
        self.assertEqual(calls, ["timing"] * 3 + ["diagnostic"] * 2)
        self.assertEqual(wait.call_count, 2)

    def test_resume_preserves_accepted_rounds_and_advances_attempt_names(self):
        variants = ["v2-project-early", "v2-project-late"]
        samples = {f"{variant}-direct": {"measured_ms": [10.0], "median_ms": 10.0} for variant in variants}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "binary"
            binary.write_bytes(b"test binary")
            output = root / "results"
            argv = [
                "bench-io.py",
                "--binary",
                str(binary),
                "--data-root",
                str(root),
                "--output",
                str(output),
                "--workloads",
                "tpch-q19",
                "--variants",
                *variants,
                "--modes",
                "direct",
                "--rounds",
                "2",
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(BENCH_IO, "measured_round", side_effect=[(samples, []), KeyboardInterrupt]),
                contextlib.redirect_stdout(io.StringIO()),
                self.assertRaises(KeyboardInterrupt),
            ):
                BENCH_IO.main()
            first = json.loads((output / "summary.json").read_text())
            partial = output / "tpch-q19-v2-project-early-direct-round1-attempt1.command.json"
            partial.write_text("{}")
            with (
                mock.patch.object(sys, "argv", [*argv, "--resume"]),
                mock.patch.object(BENCH_IO, "measured_round", return_value=(samples, [])) as resumed,
                mock.patch.object(
                    BENCH_IO, "run", return_value={"local_reads": 1, "requested_bytes": 4, "repeated_bytes": 0}
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                BENCH_IO.main()
            self.assertEqual(resumed.call_count, 1)
            self.assertEqual(resumed.call_args.args[-2:], (1, 2))
            final = json.loads((output / "summary.json").read_text())
            self.assertEqual(final["attempts"][0], first["attempts"][0])
            self.assertEqual([attempt["attempt"] for attempt in final["attempts"]], [0, 2])
            self.assertEqual(partial.read_text(), "{}")
            self.assertTrue(all(len(entry["rounds"]) == 2 for entry in final["results"]["tpch-q19"].values()))


if __name__ == "__main__":
    unittest.main()
