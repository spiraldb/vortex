# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Check CMake entrypoint defaults offline, without building the Rust FFI."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ENTRYPOINTS = {"root": ".", "ffi": "vortex-ffi", "cpp": "lang/cpp"}


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class EntrypointTests(unittest.TestCase):
    def setUp(self):
        self.repo = Path(__file__).resolve().parents[3]
        temporary = tempfile.TemporaryDirectory(prefix="vortex-cmake-entrypoints-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()
        self.env = os.environ.copy()
        self.env.pop("CMAKE_BUILD_TYPE", None)
        self.env.update(CARGO_NET_OFFLINE="true")
        self.rustc = self.executable("rustc", "print('host: x86_64-unknown-linux-gnu')\n")
        self.cargo = self.executable(
            "cargo",
            "import pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "root = pathlib.Path(args[args.index('--target-dir') + 1])\n"
            "target = args[args.index('--target') + 1]\n"
            "profile = args[args.index('--profile') + 1]\n"
            "archive = root / target / ('debug' if profile == 'dev' else profile) / 'libvortex_ffi.a'\n"
            "archive.parent.mkdir(parents=True, exist_ok=True)\n"
            "archive.write_bytes(b'Cargo ran')\n",
        )
        # project() runs before the defaults. Defer inspection until each directory
        # is complete: CMAKE_BUILD_TYPE is a normal variable, not just a cache entry.
        self.hook = self.work / "record-entrypoint.cmake"
        self.hook.write_text(
            "function(record_entrypoint)\n"
            '    set(compile_options "")\n'
            '    if(PROJECT_NAME STREQUAL "VortexCXX" AND TARGET vortex_cxx)\n'
            "        get_target_property(compile_options vortex_cxx COMPILE_OPTIONS)\n"
            "    endif()\n"
            '    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/entrypoint.txt"\n'
            '        "${CMAKE_BUILD_TYPE}\\n${VORTEX_WARNINGS_AS_ERRORS}\\n${compile_options}\\n")\n'
            "endfunction()\n"
            "cmake_language(DEFER CALL record_entrypoint)\n",
            encoding="utf-8",
        )

    def executable(self, name, body):
        path = self.work / name
        path.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
        path.chmod(0o755)
        return path

    def run_command(self, *command):
        result = subprocess.run(
            list(map(str, command)),
            env=self.env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def configure(self, name, entrypoint, *options, parent=None, parent_marker=None, exclude_from_all=False):
        source = self.repo / ENTRYPOINTS[entrypoint]
        build = self.work / name
        vortex_build = build
        if parent:
            parent_source = self.work / f"{name}-parent"
            parent_source.mkdir()
            marker = f"set(_vortex_top_level {parent_marker})\n" if parent_marker is not None else ""
            exclude = " EXCLUDE_FROM_ALL" if exclude_from_all else ""
            (parent_source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.25)\n"
                f"project({parent} LANGUAGES C CXX)\n"
                f"{marker}"
                'set(parent_build_type "${CMAKE_BUILD_TYPE}")\n'
                'set(parent_top_level "${_vortex_top_level}")\n'
                f'add_subdirectory("{source.as_posix()}" vortex{exclude})\n'
                'if(NOT "${CMAKE_BUILD_TYPE}" STREQUAL "${parent_build_type}"\n'
                '    OR NOT "${_vortex_top_level}" STREQUAL "${parent_top_level}")\n'
                '    message(FATAL_ERROR "Vortex changed parent variables")\n'
                "endif()\n",
                encoding="utf-8",
            )
            source = parent_source
            vortex_build = build / "vortex"
        self.run_command(
            "cmake",
            "-G",
            "Unix Makefiles",
            "-S",
            source,
            "-B",
            build,
            f"-DVORTEX_RUSTC_EXECUTABLE={self.rustc}",
            f"-DVORTEX_CARGO_EXECUTABLE={self.cargo}",
            f"-DCMAKE_PROJECT_INCLUDE={self.hook}",
            "-DVORTEX_BUILD_TESTS=OFF",
            "-DVORTEX_BUILD_EXAMPLES=OFF",
            "-DFETCHCONTENT_FULLY_DISCONNECTED=ON",
            *options,
        )
        self.assertFalse(list(build.rglob("libvortex_ffi.a")), "Configure must not run Cargo")
        return build, vortex_build

    def assert_settings(self, build, entrypoint, build_type, warnings):
        directories = [build if entrypoint == "ffi" else build / "ffi"]
        if entrypoint != "ffi":
            directories.append(build / "cpp" if entrypoint == "root" else build)
        for directory in directories:
            with self.subTest(directory=directory.relative_to(build)):
                settings = (directory / "entrypoint.txt").read_text(encoding="utf-8").splitlines()
                self.assertEqual(settings[:2], [build_type, warnings])
                if directory != directories[0]:
                    self.assertEqual("-Werror" in settings[2].split(";"), warnings == "ON")

    def test_owned_entrypoints_keep_developer_defaults(self):
        checkout = self.work / "checkout"
        checkout.symlink_to(self.repo, target_is_directory=True)
        for repo in (self.repo, checkout):
            self.repo = repo
            for entrypoint in ENTRYPOINTS:
                with self.subTest(checkout=repo, entrypoint=entrypoint):
                    _, build = self.configure(f"{repo.name}-{entrypoint}", entrypoint)
                    self.assert_settings(build, entrypoint, "Debug", "ON")

    def test_downstream_defaults_do_not_depend_on_project_name(self):
        # Exact matches must not confer ownership either.
        for parent in ("Parent", "VortexApp", "Vortex", "VortexNative", "VortexFFI", "VortexCXX"):
            for entrypoint in ENTRYPOINTS:
                with self.subTest(parent=parent, entrypoint=entrypoint):
                    _, build = self.configure(f"{parent}-{entrypoint}", entrypoint, parent=parent)
                    self.assert_settings(build, entrypoint, "", "OFF")

    def test_flattened_parent_sources_do_not_confer_ownership(self):
        flattened = self.work / "flattened"
        # Real directories reproduce the path heuristic bug; symlinks resolve to the checkout.
        for directory in ("vortex-ffi/cmake", "vortex-ffi/cinclude", "lang/cpp/src", "lang/cpp/include"):
            shutil.copytree(
                self.repo / directory, flattened / directory, ignore=shutil.ignore_patterns("tests", "__pycache__")
            )
        for path in (
            "Cargo.toml",
            "Cargo.lock",
            "vortex-ffi/Cargo.toml",
            "vortex-ffi/CMakeLists.txt",
            "lang/cpp/CMakeLists.txt",
        ):
            shutil.copyfile(self.repo / path, flattened / path)
        self.repo = flattened
        for entrypoint in ("ffi", "cpp"):
            with self.subTest(entrypoint=entrypoint):
                (flattened / "CMakeLists.txt").write_text(
                    "cmake_minimum_required(VERSION 3.25)\n"
                    "project(Parent LANGUAGES C CXX)\n"
                    f"add_subdirectory({ENTRYPOINTS[entrypoint]} vortex)\n",
                    encoding="utf-8",
                )
                build, _ = self.configure(f"flattened-{entrypoint}", "root")
                self.assert_settings(build / "vortex", entrypoint, "", "OFF")

    def test_embedded_root_resets_inherited_marker(self):
        for marker in ("ON", "OFF"):
            with self.subTest(marker=marker):
                _, build = self.configure(f"root-{marker}", "root", parent="Parent", parent_marker=marker)
                self.assert_settings(build, "root", "", "OFF")

    def test_explicit_options_override_defaults(self):
        for parent, warnings in ((None, "OFF"), ("Parent", "ON"), ("VortexApp", "ON")):
            for entrypoint in ENTRYPOINTS:
                with self.subTest(parent=parent, entrypoint=entrypoint):
                    _, build = self.configure(
                        f"override-{parent}-{entrypoint}",
                        entrypoint,
                        "-DCMAKE_BUILD_TYPE=Release",
                        f"-DVORTEX_WARNINGS_AS_ERRORS={warnings}",
                        parent=parent,
                    )
                    self.assert_settings(build, entrypoint, "Release", warnings)

    def test_standalone_ffi_default_build_runs_cargo(self):
        build, ffi = self.configure("standalone", "ffi")
        self.run_command("cmake", "--build", build)
        self.assertEqual((ffi / "vortex-artifacts/libvortex_ffi.a").read_bytes(), b"Cargo ran")

    def test_unused_embedded_ffi_keeps_cargo_lazy(self):
        for parent in ("Parent", "VortexApp"):
            for entrypoint in ("ffi", "root"):
                with self.subTest(parent=parent, entrypoint=entrypoint):
                    build, vortex = self.configure(
                        f"{parent}-{entrypoint}",
                        entrypoint,
                        parent=parent,
                        exclude_from_all=entrypoint == "root",
                    )
                    ffi = vortex if entrypoint == "ffi" else vortex / "ffi"
                    self.run_command("cmake", "--build", build)
                    self.assertFalse(list(build.rglob("libvortex_ffi.a")))
                    self.run_command("cmake", "--build", build, "--target", "vortex_ffi_cargo_build")
                    self.assertEqual((ffi / "vortex-artifacts/libvortex_ffi.a").read_bytes(), b"Cargo ran")


if __name__ == "__main__":
    unittest.main()
