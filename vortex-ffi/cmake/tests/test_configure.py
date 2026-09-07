# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Exercise CMake configuration without compiling the Rust archive."""

import platform
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class ConfigureTests(unittest.TestCase):
    def setUp(self):
        self.repo = Path(__file__).resolve().parents[3]
        temporary = tempfile.TemporaryDirectory(prefix="vortex-cmake-configure-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()

    def configure(self, name, *options, source=None):
        return subprocess.run(
            [
                "cmake",
                "-S",
                str(source or self.repo / "vortex-ffi"),
                "-B",
                str(self.work / name),
                "-DCMAKE_BUILD_TYPE=Debug",
                *options,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

    def build(self, name, *options):
        result = subprocess.run(
            ["cmake", "--build", str(self.work / name), *options],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def recording_cargo(self):
        cargo = self.work / "cargo"
        cargo.write_text(
            f"#!{sys.executable}\n"
            "import os, pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "root = pathlib.Path(args[args.index('--target-dir') + 1])\n"
            "target = args[args.index('--target') + 1]\n"
            "archive = root / target / 'debug/libvortex_ffi.a'\n"
            "archive.parent.mkdir(parents=True, exist_ok=True)\n"
            "archive.write_bytes(b'Cargo ran')\n"
            "(root / 'rustflags.txt').write_text(os.environ['CARGO_ENCODED_RUSTFLAGS'])\n",
            encoding="utf-8",
        )
        cargo.chmod(0o755)
        return f"-DVORTEX_CARGO_EXECUTABLE={cargo}"

    def fake_nightly_rustc(self):
        # Native compiler policy tests must not depend on an installed nightly.
        arch = {"arm64": "aarch64", "AMD64": "x86_64"}.get(platform.machine(), platform.machine())
        host = f"{arch}-apple-darwin" if sys.platform == "darwin" else f"{arch}-unknown-linux-gnu"
        rustc = self.work / "rustc"
        rustc.write_text(
            f"#!{sys.executable}\n"
            "import sys\n"
            "assert sys.argv[1:] == ['-vV'], sys.argv\n"
            f"print('rustc 1.95.0-nightly\\nhost: {host}\\nrelease: 1.95.0-nightly')\n",
            encoding="utf-8",
        )
        rustc.chmod(0o755)
        return f"-DVORTEX_RUSTC_EXECUTABLE={rustc}"

    def test_standalone_default_build_runs_cargo(self):
        result = self.configure("standalone", self.recording_cargo())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.build("standalone")
        archive = self.work / "standalone/vortex-artifacts/libvortex_ffi.a"
        self.assertTrue(archive.exists(), "The default standalone build did not produce an FFI archive")
        self.assertEqual(archive.read_bytes(), b"Cargo ran")

    def test_rust_flags_match_workspace_defaults(self):
        result = self.configure("rustflags", self.recording_cargo())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("environment or Cargo configuration rustflags", result.stdout)
        self.build("rustflags")
        actual = (self.work / "rustflags/cargo-target/rustflags.txt").read_text().split("\x1f")
        config = tomllib.loads((self.repo / ".cargo/config.toml").read_text())
        expected = config["target"]['cfg(target_family="unix")']["rustflags"] + ["-C", "relocation-model=pic"]
        self.assertEqual(actual, expected, "Keep CMake's baseline Rust flags in sync with .cargo/config.toml")

    def test_unused_embedded_ffi_does_not_run_cargo(self):
        source = self.work / "parent"
        source.mkdir()
        (source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.25)\n"
            "project(Parent LANGUAGES C CXX)\n"
            f'add_subdirectory("{self.repo.as_posix()}/vortex-ffi" ffi)\n',
            encoding="utf-8",
        )
        result = self.configure("embedded", self.recording_cargo(), source=source)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.build("embedded")
        archive = self.work / "embedded/ffi/vortex-artifacts/libvortex_ffi.a"
        self.assertFalse(archive.exists())
        self.build("embedded", "--target", "vortex_ffi_cargo_build")
        self.assertEqual(archive.read_bytes(), b"Cargo ran")

    def compiler_ids(self, c, cxx):
        # Override IDs after project() so the policy is testable on non-Apple hosts.
        hook = self.work / "compiler-ids.cmake"
        hook.write_text(
            f'set(CMAKE_C_COMPILER_ID "{c}")\nset(CMAKE_CXX_COMPILER_ID "{cxx}")\n',
            encoding="utf-8",
        )
        return f"-DCMAKE_PROJECT_VortexFFI_INCLUDE={hook}"

    def test_rust_sanitizers_require_upstream_clang(self):
        cases = (
            ("AppleClang", "Clang", "asan"),
            ("Clang", "AppleClang", "asan"),
            ("AppleClang", "AppleClang", "asan,ubsan"),
            ("AppleClang", "AppleClang", "lsan"),
            ("AppleClang", "AppleClang", "tsan"),
        )
        for index, (c, cxx, sanitizer) in enumerate(cases):
            with self.subTest(c=c, cxx=cxx, sanitizer=sanitizer):
                result = self.configure(
                    f"apple-rust-{index}",
                    self.compiler_ids(c, cxx),
                    f"-DVORTEX_SANITIZER={sanitizer}",
                )
                output = result.stdout + result.stderr
                self.assertNotEqual(result.returncode, 0, output)
                self.assertIn("Rust sanitizer builds require upstream LLVM Clang", output)

    def test_appleclang_without_rust_instrumentation(self):
        for sanitizer in ("", "ubsan"):
            with self.subTest(sanitizer=sanitizer):
                result = self.configure(
                    f"apple-native-{sanitizer}",
                    self.compiler_ids("AppleClang", "AppleClang"),
                    f"-DVORTEX_SANITIZER={sanitizer}",
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_sanitizer_list_whitespace_and_empty_items(self):
        cases = (
            (" ASAN , UBSAN, ", "-fsanitize=address,undefined"),
            ("\t ubsan; ;\t", "-fsanitize=undefined"),
            (" ; ,\t ", ""),
        )
        for index, (sanitizers, expected) in enumerate(cases):
            with self.subTest(sanitizers=sanitizers):
                hook = self.compiler_ids("Clang", "Clang")
                with (self.work / "compiler-ids.cmake").open("a", encoding="utf-8") as script:
                    script.write(
                        'file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/sanitizer-flags.txt"\n'
                        '    CONTENT "$<TARGET_PROPERTY:vortex_ffi_static,INTERFACE_COMPILE_OPTIONS>")\n'
                    )
                name = f"sanitizer-list-{index}"
                result = self.configure(name, hook, f"-DVORTEX_SANITIZER={sanitizers}", self.fake_nightly_rustc())
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                flags = (self.work / name / "sanitizer-flags.txt").read_text(encoding="utf-8")
                self.assertEqual(flags, expected)

    def test_unknown_sanitizer_reports_trimmed_name(self):
        result = self.configure("unknown-sanitizer", "-DVORTEX_SANITIZER=asan, typo ,")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("got 'typo'", result.stdout + result.stderr)

    def test_upstream_clang_accepts_rust_instrumentation(self):
        result = self.configure(
            "clang-asan",
            self.compiler_ids("Clang", "Clang"),
            self.fake_nightly_rustc(),
            "-DVORTEX_SANITIZER=asan,ubsan",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
