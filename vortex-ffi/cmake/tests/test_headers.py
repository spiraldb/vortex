# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Exercise generated-header dependencies with CMake and a recording Cargo."""

import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from itertools import product
from pathlib import Path


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class HeaderTests(unittest.TestCase):
    generators = ("Ninja", "Unix Makefiles")

    def setUp(self):
        self.repo = Path(__file__).resolve().parents[3]
        temporary = tempfile.TemporaryDirectory(prefix="vortex-cmake-headers-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()

    def run_command(self, *command):
        result = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def configure_fixture(self, cuda, generator):
        tool = "ninja" if generator == "Ninja" else "make"
        if not shutil.which(tool):
            self.skipTest(f"{tool} is required for {generator}")
        self.generator = generator
        self.source = self.work / generator / ("cuda source" if cuda else "cpu source")
        self.build_dir = self.source / "build"
        ffi = self.source / "vortex-ffi"
        shutil.copytree(self.repo / "vortex-ffi/cmake", ffi / "cmake", ignore=shutil.ignore_patterns("tests"))
        shutil.copy2(self.repo / "vortex-ffi/CMakeLists.txt", ffi / "CMakeLists.txt")
        for manifest in ("Cargo.toml", "Cargo.lock", "vortex-ffi/Cargo.toml", "vortex-cuda/ffi/Cargo.toml"):
            path = self.source / manifest
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

        self.source_headers = {
            "CPU": ffi / "cinclude/vortex.h",
            "CUDA": self.source / "vortex-cuda/ffi/cinclude/vortex_cuda.h",
        }
        for name, header in self.source_headers.items():
            header.parent.mkdir(parents=True, exist_ok=True)
            header.write_text(f"#define {name}_HEADER_VERSION 0\n", encoding="utf-8")
        self.versions = dict.fromkeys(self.source_headers, 1)
        self.version_file = self.source / "versions.json"
        self.version_file.write_text(json.dumps(self.versions), encoding="utf-8")

        cargo = self.source / "cargo"
        cargo.write_text(
            f"#!{sys.executable}\n"
            + textwrap.dedent("""\
                import json, pathlib, sys
                root = pathlib.Path.cwd()
                args = sys.argv[1:]
                package = args[args.index('--package') + 1].replace('-', '_')
                versions = json.loads((root / 'versions.json').read_text())
                for name, version in versions.items():
                    if name == 'CUDA' and package != 'vortex_cuda_ffi':
                        continue
                    relative = ('vortex-ffi/cinclude/vortex.h' if name == 'CPU'
                                else 'vortex-cuda/ffi/cinclude/vortex_cuda.h')
                    (root / relative).write_text(f'#define {name}_HEADER_VERSION {version}\\n')
                target_dir = pathlib.Path(args[args.index('--target-dir') + 1])
                target = args[args.index('--target') + 1]
                archive = target_dir / target / 'debug' / f'lib{package}.a'
                archive.parent.mkdir(parents=True, exist_ok=True)
                archive.write_bytes(b'!<arch>\\n')
                """),
            encoding="utf-8",
        )
        cargo.chmod(0o755)
        rustc = self.source / "rustc"
        host = "aarch64-apple-darwin" if sys.platform == "darwin" else "x86_64-unknown-linux-gnu"
        rustc.write_text(f"#!{sys.executable}\nprint('host: {host}')\n", encoding="utf-8")
        rustc.chmod(0o755)

        # Exercise CUDA header selection on any host without requiring a toolkit
        # or changing the native compiler/linker used by the tiny C consumer.
        (self.source / "FindCUDAToolkit.cmake").write_text(
            "set(CUDAToolkit_FOUND TRUE)\n"
            'set(CUDAToolkit_NVCC_EXECUTABLE "${CMAKE_CURRENT_LIST_DIR}/rustc")\n'
            'set(CUDAToolkit_TARGET_DIR "${CMAKE_CURRENT_LIST_DIR}")\n',
            encoding="utf-8",
        )
        (self.source / "CMakeLists.txt").write_text(
            textwrap.dedent("""\
                cmake_minimum_required(VERSION 3.25)
                project(HeaderConsumer LANGUAGES C CXX)
                if(VORTEX_ENABLE_CUDA)
                    set(CMAKE_SYSTEM_NAME Linux)
                    list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")
                endif()
                add_subdirectory(vortex-ffi ffi)
                add_executable(probe probe.c)
                target_link_libraries(probe PRIVATE Vortex::ffi_static)
                file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include-dir.txt"
                    CONTENT "$<TARGET_PROPERTY:Vortex::ffi_static,INTERFACE_INCLUDE_DIRECTORIES>")
                """),
            encoding="utf-8",
        )
        self.include_dir = self.build_dir / "ffi/vortex-artifacts/include"
        self.archive = self.build_dir / "ffi/vortex-artifacts/libvortex_ffi.a"
        self.probe = self.build_dir / "probe"
        self.configure(cuda)

    def configure(self, cuda):
        self.headers = {name: path for name, path in self.source_headers.items() if name == "CPU" or cuda}
        self.staged_headers = [self.include_dir / header.name for header in self.headers.values()]
        includes = "".join(f"#include <{header.name}>\n" for header in self.headers.values())
        formats = " ".join("%d" for _ in self.headers)
        arguments = ", ".join(f"{name}_HEADER_VERSION" for name in self.headers)
        (self.source / "probe.c").write_text(
            f'{includes}#include <stdio.h>\nint main(void) {{ printf("{formats}\\n", {arguments}); }}\n',
            encoding="utf-8",
        )
        self.run_command(
            "cmake",
            "-G",
            self.generator,
            "-S",
            str(self.source),
            "-B",
            str(self.build_dir),
            "-DCMAKE_BUILD_TYPE=Debug",
            f"-DVORTEX_CARGO_EXECUTABLE={self.source / 'cargo'}",
            f"-DVORTEX_RUSTC_EXECUTABLE={self.source / 'rustc'}",
            f"-DVORTEX_ENABLE_CUDA={'ON' if cuda else 'OFF'}",
        )

    def build(self, *options):
        self.run_command("cmake", "--build", str(self.build_dir), *options)

    def assert_versions(self):
        expected = " ".join(str(self.versions[name]) for name in self.headers)
        self.assertEqual(self.run_command(str(self.probe)).strip(), expected)

    def assert_staged_headers(self):
        self.assertEqual(set(self.include_dir.iterdir()), set(self.staged_headers))
        for header, staged in zip(self.headers.values(), self.staged_headers, strict=True):
            self.assertEqual(staged.read_bytes(), header.read_bytes())

    def source_header_state(self):
        return {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in self.source_headers.values()}

    def test_header_changes_recompile_in_first_build(self):
        for generator, cuda in product(self.generators, (False, True)):
            with self.subTest(generator=generator, cuda=cuda):
                self.configure_fixture(cuda, generator)
                self.build()
                self.assert_versions()
                # Only the header contents change; the Cargo archive stays identical.
                # Recompilation must not be deferred until a second invocation.
                for name in reversed(self.headers):
                    self.versions[name] += 1
                    self.version_file.write_text(json.dumps(self.versions), encoding="utf-8")
                    self.build()
                    self.assert_versions()

                outputs = [self.archive, *self.staged_headers, self.probe, *self.build_dir.rglob("*.o")]
                timestamps = {path: path.stat().st_mtime_ns for path in outputs}
                self.build()
                self.assert_versions()
                self.assertEqual({path: path.stat().st_mtime_ns for path in outputs}, timestamps)

    def test_clean_preserves_source_headers_and_rebuilds_staged_headers(self):
        for generator, cuda in product(self.generators, (False, True)):
            with self.subTest(generator=generator, cuda=cuda):
                self.configure_fixture(cuda, generator)
                self.assertTrue(self.include_dir.is_dir(), "The include directory must exist at configure time")
                self.assertEqual((self.build_dir / "include-dir.txt").read_text(), self.include_dir.as_posix())
                self.build()
                self.assert_versions()
                self.assert_staged_headers()
                sources = self.source_header_state()
                self.build("--target", "clean")
                self.assertFalse(self.include_dir.exists())
                self.assertFalse(self.archive.exists())
                self.assertEqual(self.source_header_state(), sources)
                self.build()
                self.assert_versions()
                self.assert_staged_headers()

    def test_cuda_cpu_cuda_reconfiguration(self):
        for generator in self.generators:
            with self.subTest(generator=generator):
                self.configure_fixture(True, generator)
                self.build()
                self.assert_versions()
                self.assert_staged_headers()
                for cuda in (False, True):
                    sources = self.source_header_state()
                    self.configure(cuda)
                    self.assertFalse((self.include_dir / "vortex_cuda.h").exists())
                    self.assertEqual(self.source_header_state(), sources)
                    self.build()
                    self.assert_versions()
                    self.assert_staged_headers()
                    sources = self.source_header_state()
                    self.build("--target", "clean")
                    self.assertFalse(self.include_dir.exists())
                    self.assertFalse(self.archive.exists())
                    self.assertEqual(self.source_header_state(), sources)
                    self.build()
                    self.assert_versions()
                    self.assert_staged_headers()


if __name__ == "__main__":
    unittest.main()
