# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Build a tiny Cargo/cc-rs fixture using the production CMake compiler handoff."""

import hashlib
import os
import shlex
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

import test_cargo_environment


@unittest.skipUnless(
    all(shutil.which(tool) for tool in ("cmake", "ninja", "cargo", "rustc", "clang", "clang++")),
    "CMake, Ninja, Cargo, rustc, and Clang are required",
)
class CompilerCommandTests(unittest.TestCase):
    def setUp(self):
        cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
        if not any((cargo_home / "registry/src").glob("*/cc-1.4.0")):
            self.skipTest("cc 1.4.0 must be cached for the offline fixture")
        temporary = tempfile.TemporaryDirectory(prefix="vortex-compiler-commands-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()
        self.source = self.work / "source"
        self.ffi = self.source / "vortex-ffi"
        self.ffi.mkdir(parents=True)
        production = Path(__file__).resolve().parents[2]
        shutil.copytree(production / "cmake", self.ffi / "cmake", ignore=shutil.ignore_patterns("tests"))
        shutil.copyfile(production / "CMakeLists.txt", self.ffi / "CMakeLists.txt")
        (self.ffi / "cinclude").mkdir()
        (self.ffi / "cinclude/vortex.h").write_text("/* fixture */\n", encoding="utf-8")
        (self.source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.25)\n"
            "project(CompilerCommands LANGUAGES C CXX)\n"
            "foreach(language IN ITEMS C CXX)\n"
            "    if(DEFINED FIXTURE_${language}_ARG1)\n"
            '        set(CMAKE_${language}_COMPILER_ARG1 "${FIXTURE_${language}_ARG1}")\n'
            "    endif()\n"
            "endforeach()\n"
            "add_subdirectory(vortex-ffi ffi)\n"
            "add_library(cmake_native STATIC vortex-ffi/native.c vortex-ffi/native.cpp)\n",
            encoding="utf-8",
        )
        (self.source / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["vortex-ffi"]\nresolver = "2"\n', encoding="utf-8"
        )
        (self.ffi / "Cargo.toml").write_text(
            '[package]\nname = "vortex-ffi"\nversion = "0.0.0"\nedition = "2021"\n'
            '[lib]\npath = "lib.rs"\n[build-dependencies]\ncc = "=1.4.0"\n',
            encoding="utf-8",
        )
        (self.ffi / "lib.rs").write_text("pub fn value() -> i32 { 7 }\n", encoding="utf-8")
        (self.ffi / "build.rs").write_text(
            "fn main() {\n"
            '    for (cpp, source, name) in [(false, "native.c", "native_c"), (true, "native.cpp", "native_cpp")] {\n'
            "        let mut build = cc::Build::new();\n"
            "        build.cpp(cpp).file(source);\n"
            "        assert!(build.get_compiler().is_like_clang());\n"
            '        assert!(build.is_flag_supported("-fno-omit-frame-pointer").unwrap());\n'
            '        assert!(!build.is_flag_supported("-fdefinitely-not-a-supported-flag").unwrap());\n'
            "        build.compile(name);\n"
            '        println!("cargo:rerun-if-changed={source}");\n'
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        for extension, macro, function in (("c", "REVIEW_ARG1", "native_c"), ("cpp", "REVIEW_CXX_ARG1", "native_cpp")):
            (self.ffi / f"native.{extension}").write_text(
                f'#ifndef {macro}\n#error "CMake compiler mandatory argument was not forwarded to Cargo"\n#endif\n'
                '#ifdef CHECK_QUOTING\n#include "fixture\'s header.h"\n'
                '#ifdef __cplusplus\nstatic_assert(sizeof(ARG1_TEXT) == sizeof("space and apostrophe\'s"));\n'
                '#else\n_Static_assert(sizeof(ARG1_TEXT) == sizeof("space and apostrophe\'s"), "quoting");\n'
                "#endif\n#endif\n"
                f"int {function}(void) {{ return {macro}; }}\n",
                encoding="utf-8",
            )
        self.env = test_cargo_environment.rust_toolchain_environment()
        for key in list(self.env):
            if key.startswith(("CC", "CXX", "CFLAGS", "HOST_", "TARGET_", "SCCACHE_")) or key in (
                "AR",
                "RANLIB",
                "RUSTC_WRAPPER",
                "RUSTC_WORKSPACE_WRAPPER",
                "RUSTFLAGS",
                "CARGO_ENCODED_RUSTFLAGS",
                "CARGO_TARGET_DIR",
            ):
                self.env.pop(key)
        self.env.update(CARGO_NET_OFFLINE="true", CARGO_BUILD_JOBS="2")
        self.compilers = [shutil.which("clang"), shutil.which("clang++")]
        self.run_command(
            ["cargo", "generate-lockfile", "--offline", "--manifest-path", self.source / "Cargo.toml"], self.env
        )
        version = self.run_command(["rustc", "-vV"], self.env)
        self.target = next(line.removeprefix("host: ") for line in version.splitlines() if line.startswith("host: "))

    def run_command(self, command, env):
        result = subprocess.run(
            list(map(str, command)),
            cwd=test_cargo_environment.REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def exercise(self, name, wrapper=None, rust_wrapper=None, quoted=False):
        env = self.env.copy()
        if rust_wrapper:
            env["RUSTC_WRAPPER"] = rust_wrapper
        if wrapper:
            env["CC_KNOWN_WRAPPER_CUSTOM"] = Path(wrapper).stem
        compiler_args = []
        options = []
        include = self.work / "include directory's"
        include.mkdir(exist_ok=True)
        (include / "fixture's header.h").write_text("/* mandatory include path */\n", encoding="utf-8")
        for compiler, language, macro in zip(
            self.compilers, ("C", "CXX"), ("REVIEW_ARG1", "REVIEW_CXX_ARG1"), strict=True
        ):
            args = [f"-D{macro}=7"]
            if quoted:
                args += ["-DCHECK_QUOTING", f"-I{include}", '-DARG1_TEXT="space and apostrophe\'s"']
            if wrapper:
                args.insert(0, compiler)
                compiler = wrapper
            compiler_args.append(shlex.join(args))
            if quoted:
                # CMake's compiler-detection cache does not escape ARG1 quotes.
                # Supply these after project(), as an embedding parent can do.
                options += [f"-DCMAKE_{language}_COMPILER={compiler}", f"-DFIXTURE_{language}_ARG1={shlex.join(args)}"]
            else:
                env["CC" if language == "C" else "CXX"] = " ".join([compiler, *args])
        build = self.work / name
        configure = ["cmake", "-G", "Ninja", "-S", self.source, "-B", build, "-DCMAKE_BUILD_TYPE=Debug", *options]
        self.run_command(configure, env)
        self.run_command(["cmake", "--build", build, "--target", "cmake_native"], env)
        if not quoted:
            # The same environment command already works in ordinary Cargo/cc-rs.
            self.run_command(
                [
                    "cargo",
                    "build",
                    "--locked",
                    "--manifest-path",
                    self.source / "Cargo.toml",
                    "--target",
                    self.target,
                    "--target-dir",
                    self.work / f"{name}-direct",
                ],
                env,
            )
        cargo_build = ["cmake", "--build", build, "--target", "vortex_ffi_cargo_build"]
        self.run_command(cargo_build, env)
        archives = sorted((build / "ffi/cargo-target" / self.target).glob("debug/build/*/out/libnative_*.a"))
        self.assertEqual(len(archives), 2)
        original = [hashlib.sha256(archive.read_bytes()).hexdigest() for archive in archives]
        timestamps = [archive.stat().st_mtime_ns for archive in archives]
        self.run_command(cargo_build, env)
        self.assertEqual([archive.stat().st_mtime_ns for archive in archives], timestamps, "Cargo should remain fresh")
        # Change effective ARG1 in the parent: CMake reloads its original compiler
        # metadata over cache-only edits. Leave sources and ordinary flags untouched.
        self.run_command(
            [
                *configure,
                f"-DFIXTURE_C_ARG1={compiler_args[0].replace('=7', '=8')}",
                f"-DFIXTURE_CXX_ARG1={compiler_args[1].replace('=7', '=9')}",
            ],
            env,
        )
        self.run_command(cargo_build, env)
        for archive, before in zip(archives, original, strict=True):
            self.assertNotEqual(
                hashlib.sha256(archive.read_bytes()).hexdigest(), before, f"ARG1 change did not rebuild {archive.name}"
            )

    def test_uncached_compiler_commands(self):
        for name, wrapper, quoted in (
            ("ordinary", None, False),
            ("env", shutil.which("env"), False),
            ("quoted", None, True),
        ):
            with self.subTest(command=name):
                self.exercise(name, wrapper=wrapper, quoted=quoted)

    def test_sccache_compiler_commands(self):
        sccache = shutil.which("sccache")
        if not sccache:
            self.skipTest("sccache is required for cached compiler commands")
        config = self.work / "sccache.toml"
        config.write_text("", encoding="utf-8")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.env.update(
            SCCACHE_DIR=str(self.work / "cache"),
            SCCACHE_CONF=str(config),
            SCCACHE_SERVER_PORT=str(port),
            SCCACHE_CACHE_SIZE="64M",
        )
        self.run_command([sccache, "--start-server"], self.env)
        self.addCleanup(self.run_command, [sccache, "--stop-server"], self.env)
        for name, wrapper, quoted in (
            ("ordinary-cache", None, False),
            ("env-cache", shutil.which("env"), False),
            ("explicit-cache", sccache, False),
            ("quoted-cache", None, True),
        ):
            with self.subTest(command=name):
                self.exercise(name, wrapper=wrapper, rust_wrapper=sccache, quoted=quoted)


if __name__ == "__main__":
    unittest.main()
