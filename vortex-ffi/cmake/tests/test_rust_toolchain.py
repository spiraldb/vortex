# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Verify configure/build toolchain selection without invoking Rust builds."""

import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from pathlib import Path


@unittest.skipUnless(all(shutil.which(tool) for tool in ("cmake", "ninja")), "CMake and Ninja are required")
class RustToolchainTests(unittest.TestCase):
    def setUp(self):
        self.repo = Path(__file__).resolve().parents[3]
        temporary = tempfile.TemporaryDirectory(prefix="vortex-rust-toolchain-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()
        self.build_dir = self.work / "build"
        self.log = self.work / "tools.jsonl"
        with (self.repo / "rust-toolchain.toml").open("rb") as toolchain:
            self.workspace_toolchain = tomllib.load(toolchain)["toolchain"]["channel"]
        arch = {"arm64": "aarch64", "AMD64": "x86_64"}.get(platform.machine(), platform.machine())
        if sys.platform == "darwin" and arch == "aarch64":
            self.host = "aarch64-apple-darwin"
        elif sys.platform == "linux" and arch in ("aarch64", "x86_64"):
            self.host = f"{arch}-unknown-linux-gnu"
        else:
            self.skipTest("The CMake FFI supports Linux x86_64/aarch64 and macOS arm64")

        self.source = self.work / "parent"
        self.source.mkdir()
        self.trigger = self.source / "reconfigure.cmake"
        self.trigger.write_text("# Reconfigure trigger\n", encoding="utf-8")
        (self.source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.25)\n"
            "project(ToolchainTest LANGUAGES C CXX)\n"
            "include(reconfigure.cmake)\n"
            f'add_subdirectory("{self.repo.as_posix()}/vortex-ffi" ffi)\n',
            encoding="utf-8",
        )
        # Only recording Cargo runs, so exercise sanitizer selection on AppleClang/GCC hosts too.
        self.compiler_ids = self.work / "compiler-ids.cmake"
        self.compiler_ids.write_text(
            "set(CMAKE_C_COMPILER_ID Clang)\nset(CMAKE_CXX_COMPILER_ID Clang)\n",
            encoding="utf-8",
        )
        self.cargo = self.recording_tool("cargo")
        self.rustc = self.recording_tool("rustc")

    def recording_tool(self, name):
        tool = self.work / name
        tool.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, sys, tomllib\n"
            "args = sys.argv[1:]\n"
            "cwd = pathlib.Path.cwd()\n"
            "override = os.environ.get('RUSTUP_TOOLCHAIN')\n"
            "with (cwd / 'rust-toolchain.toml').open('rb') as source:\n"
            "    workspace = tomllib.load(source)['toolchain']['channel']\n"
            "selected = workspace if override is None else override\n"
            f"with open({str(self.log)!r}, 'a', encoding='utf-8') as log:\n"
            f"    log.write(json.dumps(dict(tool={name!r}, override=override, "
            "selected=selected, cwd=str(cwd))) + '\\n')\n"
            + (
                f"assert args == ['-vV'], args\nprint('rustc 1.95.0\\nhost: {self.host}\\nrelease: 1.95.0')\n"
                if name == "rustc"
                else "assert args[0] == 'rustc', args\n"
                "root = pathlib.Path(args[args.index('--target-dir') + 1])\n"
                "target = args[args.index('--target') + 1]\n"
                "archive = root / target / 'debug/libvortex_ffi.a'\n"
                "archive.parent.mkdir(parents=True, exist_ok=True)\n"
                "archive.write_bytes(b'recorded archive')\n"
            ),
            encoding="utf-8",
        )
        tool.chmod(0o755)
        return tool

    def run_cmake(self, *args, toolchain=None):
        env = os.environ.copy()
        env.pop("RUSTUP_TOOLCHAIN", None)
        if toolchain is not None:
            env["RUSTUP_TOOLCHAIN"] = toolchain
        result = subprocess.run(
            ["cmake", *map(str, args)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout + result.stderr

    def configure(self, *options, toolchain=None):
        return self.run_cmake(
            "-S",
            self.source,
            "-B",
            self.build_dir,
            "-G",
            "Ninja",
            "-DCMAKE_BUILD_TYPE=Debug",
            f"-DCMAKE_PROJECT_VortexFFI_INCLUDE={self.compiler_ids}",
            f"-DVORTEX_CARGO_EXECUTABLE={self.cargo}",
            f"-DVORTEX_RUSTC_EXECUTABLE={self.rustc}",
            *options,
            toolchain=toolchain,
        )

    def build(self, *, toolchain=None, reconfigure=False):
        if reconfigure:
            time.sleep(1.1)
            with self.trigger.open("a", encoding="utf-8") as trigger:
                trigger.write("# Changed configure input\n")
        return self.run_cmake("--build", self.build_dir, "--target", "vortex_ffi_cargo_build", toolchain=toolchain)

    def assert_calls(self, expected):
        calls = [json.loads(line) for line in self.log.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([(call["tool"], call["override"]) for call in calls], expected)
        for call, (_, override) in zip(calls, expected, strict=True):
            self.assertEqual(call["selected"], self.workspace_toolchain if override is None else override)
            self.assertEqual(call["cwd"], str(self.repo))

    def assert_cache(self, selected):
        cache = (self.build_dir / "CMakeCache.txt").read_text(encoding="utf-8").splitlines()
        self.assertEqual(
            [line for line in cache if line.startswith("VORTEX_RUSTUP_TOOLCHAIN:")],
            [f"VORTEX_RUSTUP_TOOLCHAIN:STRING={selected}"],
        )

    def new_build(self, name):
        self.build_dir = self.work / name
        self.log.unlink(missing_ok=True)

    def test_environment_selection_survives_implicit_reconfigure(self):
        selected = "nightly-2026-04-01"
        for sanitizer in ("", "asan"):
            with self.subTest(sanitizer=sanitizer):
                self.new_build(f"environment-{sanitizer}")
                self.configure(f"-DVORTEX_SANITIZER={sanitizer}", toolchain=selected)
                self.build()
                expected = [("rustc", selected), ("cargo", selected)]
                self.assert_calls(expected)
                for ambient in (None, "nightly-2026-05-01"):
                    self.build(toolchain=ambient, reconfigure=True)
                    expected.extend([("rustc", selected), ("cargo", selected)])
                    self.assert_calls(expected)
                self.assert_cache(selected)

    def test_workspace_selection_survives_implicit_reconfigure(self):
        self.configure()
        self.build(toolchain="nightly")
        expected = [("rustc", None), ("cargo", None)]
        self.assert_calls(expected)
        self.build(toolchain="nightly", reconfigure=True)
        expected.extend([("rustc", None), ("cargo", None)])
        self.assert_calls(expected)
        self.assert_cache("")

    def test_explicit_override_and_update(self):
        first = "nightly-2026-04-01"
        second = "nightly-2026-05-01"
        output = self.configure("-DVORTEX_SANITIZER=asan", f"-DVORTEX_RUSTUP_TOOLCHAIN={first}", toolchain="ambient")
        self.build(toolchain="ambient")
        expected = [("rustc", first), ("cargo", first)]
        self.assert_calls(expected)
        self.assert_cache(first)
        self.assertIn(f"Vortex Rust toolchain override: {first}", output)

        self.configure(f"-DVORTEX_RUSTUP_TOOLCHAIN={second}", toolchain="ambient")
        self.build()
        expected.extend([("rustc", second), ("cargo", second)])
        self.assert_calls(expected)
        self.assert_cache(second)
        self.build(toolchain="ambient", reconfigure=True)
        expected.extend([("rustc", second), ("cargo", second)])
        self.assert_calls(expected)

        self.configure("-DVORTEX_RUSTUP_TOOLCHAIN=", toolchain="ambient")
        self.build(toolchain="ambient")
        expected.extend([("rustc", None), ("cargo", None)])
        self.assert_calls(expected)
        self.assert_cache("")

    def test_explicit_empty_uses_workspace(self):
        for sanitizer in ("", "asan"):
            with self.subTest(sanitizer=sanitizer):
                self.new_build(f"empty-{sanitizer}")
                output = self.configure(
                    f"-DVORTEX_SANITIZER={sanitizer}", "-DVORTEX_RUSTUP_TOOLCHAIN=", toolchain="ambient"
                )
                self.build(toolchain="ambient")
                self.assert_calls([("rustc", None), ("cargo", None)])
                self.assert_cache("")
                self.assertIn("Vortex Rust toolchain: workspace rust-toolchain.toml", output)

    def test_rust_sanitizers_default_to_nightly(self):
        for sanitizer, ambient in (("asan", None), ("lsan", None), ("tsan", None), ("asan,ubsan", "")):
            with self.subTest(sanitizer=sanitizer, ambient=ambient):
                self.new_build(f"default-{sanitizer}")
                output = self.configure(f"-DVORTEX_SANITIZER={sanitizer}", toolchain=ambient)
                self.build(toolchain="ambient")
                expected = [("rustc", "nightly"), ("cargo", "nightly")]
                self.assert_calls(expected)
                self.assert_cache("nightly")
                self.assertIn("Vortex Rust toolchain override: nightly", output)
                self.build(reconfigure=True)
                expected.extend([("rustc", "nightly"), ("cargo", "nightly")])
                self.assert_calls(expected)

    def test_ubsan_keeps_workspace_toolchain(self):
        self.configure("-DVORTEX_SANITIZER=ubsan")
        self.build(toolchain="ambient")
        self.assert_calls([("rustc", None), ("cargo", None)])
        self.assert_cache("")

    def test_enabling_sanitizer_preserves_cached_workspace_selection(self):
        self.configure()
        self.configure("-DVORTEX_SANITIZER=asan")
        self.build()
        self.assert_calls([("rustc", None), ("rustc", None), ("cargo", None)])
        self.assert_cache("")


if __name__ == "__main__":
    unittest.main()
