# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Exercise CMake configuration without compiling the Rust archive."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class ConfigureTests(unittest.TestCase):
    def setUp(self):
        self.repo = Path(__file__).resolve().parents[3]
        temporary = tempfile.TemporaryDirectory(prefix="vortex-cmake-configure-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()

    def configure(self, name, *options):
        return subprocess.run(
            [
                "cmake",
                "-S",
                str(self.repo / "vortex-ffi"),
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

    def test_upstream_clang_accepts_rust_instrumentation(self):
        result = self.configure(
            "clang-asan",
            self.compiler_ids("Clang", "Clang"),
            "-DVORTEX_SANITIZER=asan,ubsan",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
