# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Build a tiny Cargo/cc-rs fixture using the production CMake compiler handoff."""

import hashlib
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import test_cargo_environment
from cc_fixture import cached_cc_version
from test_native_flags import ERROR_FLAGS


@unittest.skipUnless(
    all(shutil.which(tool) for tool in ("cmake", "ninja", "cargo", "rustc", "clang", "clang++")),
    "CMake, Ninja, Cargo, rustc, and Clang are required",
)
class CompilerCommandTests(unittest.TestCase):
    def setUp(self):
        self.cc_version = cached_cc_version()
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
            f'[lib]\npath = "lib.rs"\n[build-dependencies]\ncc = "={self.cc_version}"\n',
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

    def test_vendored_warning_and_cargo_freshness(self):
        include = self.work / "parent include's directory"
        include.mkdir()
        (include / "parent.h").write_text("#define HEADER_VALUE 3\n", encoding="utf-8")
        for extension, macro in (("c", "REVIEW_ARG1"), ("cpp", "REVIEW_CXX_ARG1")):
            assertion = "_Static_assert" if extension == "c" else "static_assert"
            (self.ffi / f"native.{extension}").write_text(
                '#include "parent.h"\n'
                '#ifndef __OPTIMIZE__\n#error "parent optimization was lost"\n#endif\n'
                "enum small { zero, one };\n"
                f'{assertion}(sizeof(enum small) == 1, "parent ABI flag was lost");\n'
                f"int native_{extension}(void) {{\n"
                "    int vendored_unused;\n"
                f"    return {macro} + PARENT_VALUE + CONFIG_VALUE + HEADER_VALUE;\n}}\n",
                encoding="utf-8",
            )
        build = self.work / "native-flags"
        configure = ["cmake", "-G", "Ninja", "-S", self.source, "-B", build, "-DCMAKE_BUILD_TYPE=Debug"]
        globals_by_language = {}
        for compiler, language, macro, extension in zip(
            self.compilers, ("C", "CXX"), ("REVIEW_ARG1", "REVIEW_CXX_ARG1"), ("c", "cpp"), strict=True
        ):
            configure += [f"-DCMAKE_{language}_COMPILER={compiler}", f"-DFIXTURE_{language}_ARG1=-D{macro}=7"]
            globals_by_language[language] = ["-Wunused-variable", "-fshort-enums", f"-I{include}"]
            # Establish the root cause independently: the parent policy rejects this
            # otherwise compilable third-party warning in both native languages.
            result = subprocess.run(
                [
                    compiler,
                    *globals_by_language[language],
                    "-O1",
                    "-DCONFIG_VALUE=1",
                    "-DPARENT_VALUE=7",
                    f"-D{macro}=7",
                    "-Werror",
                    "-c",
                    str(self.ffi / f"native.{extension}"),
                    "-o",
                    str(self.work / f"control-{extension}.o"),
                ],
                env=self.env,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("error: unused variable 'vendored_unused'", result.stderr)

        def reconfigure(value, policy):
            options = []
            for language, flags in globals_by_language.items():
                options += [
                    f"-DCMAKE_{language}_FLAGS={shlex.join([*flags, f'-DPARENT_VALUE={value}', *policy])}",
                    f"-DCMAKE_{language}_FLAGS_DEBUG={shlex.join(['-O1', '-DCONFIG_VALUE=1', *policy])}",
                ]
            self.run_command([*configure, *options], self.env)

        cargo_build = ["cmake", "--build", build, "--target", "vortex_ffi_cargo_build"]
        reconfigure(7, ERROR_FLAGS)
        self.run_command(cargo_build, self.env)
        archives = sorted((build / "ffi/cargo-target" / self.target).glob("debug/build/*/out/libnative_*.a"))
        self.assertEqual(len(archives), 2)
        output = (archives[0].parent.parent / "output").read_text(encoding="utf-8")
        self.assertEqual(output.count("warning: unused variable 'vendored_unused'"), 2)
        timestamps = [archive.stat().st_mtime_ns for archive in archives]
        digests = [hashlib.sha256(archive.read_bytes()).hexdigest() for archive in archives]
        self.run_command(cargo_build, self.env)
        self.assertEqual([archive.stat().st_mtime_ns for archive in archives], timestamps)
        reconfigure(7, [])
        self.run_command(cargo_build, self.env)
        self.assertEqual(
            [archive.stat().st_mtime_ns for archive in archives],
            timestamps,
            "Changing only the parent's warning-as-error policy must leave Cargo fresh",
        )
        reconfigure(8, [])
        self.run_command(cargo_build, self.env)
        for archive, timestamp, digest in zip(archives, timestamps, digests, strict=True):
            self.assertNotEqual(archive.stat().st_mtime_ns, timestamp)
            self.assertNotEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), digest)

    def test_uncached_compiler_commands(self):
        for name, wrapper, quoted in (
            ("ordinary", None, False),
            ("env", shutil.which("env"), False),
            ("quoted", None, True),
        ):
            with self.subTest(command=name):
                self.exercise(name, wrapper=wrapper, quoted=quoted)

    @unittest.skipUnless(sys.platform == "darwin", "Apple SDK fixture requires macOS")
    @unittest.skipUnless(shutil.which("xcrun"), "xcrun is required for the Apple SDK fixture")
    def test_osx_sysroot_headers_and_cargo_freshness(self):
        real_sdk = Path(self.run_command(["xcrun", "--sdk", "macosx", "--show-sdk-path"], self.env).strip())
        self.env["SDKROOT"] = str(real_sdk)
        sdks = []
        for value in (7, 9):
            sdk = self.work / f"SDK directory's {value}" / "MacOSX.sdk"
            # Overlay only usr/include; use the installed SDK's headers and libraries.
            for relative, exclude in ((".", "usr"), ("usr", "include"), ("usr/include", None)):
                (sdk / relative).mkdir(parents=True, exist_ok=True)
                for entry in (real_sdk / relative).iterdir():
                    if entry.name != exclude:
                        (sdk / relative / entry.name).symlink_to(entry)
            (sdk / "usr/include/vortex_fixture_sdk.h").write_text(
                f"#define FIXTURE_SDK_VALUE {value}\n", encoding="utf-8"
            )
            sdks.append(sdk)
        for extension, header in (("c", "stdlib.h"), ("cpp", "cstdlib")):
            (self.ffi / f"native.{extension}").write_text(
                f"#include <{header}>\n#include <vortex_fixture_sdk.h>\n"
                f"int native_{extension}(void) {{ return FIXTURE_SDK_VALUE; }}\n",
                encoding="utf-8",
            )
        build = self.work / "sdk-build"
        configure = ["cmake", "-G", "Ninja", "-S", self.source, "-B", build, "-DCMAKE_BUILD_TYPE=Debug"]
        for language, compiler in zip(("C", "CXX"), self.compilers, strict=True):
            configure.append(f"-DCMAKE_{language}_COMPILER={compiler}")
        cargo_build = ["cmake", "--build", build, "--target", "vortex_ffi_cargo_build"]
        previous = {}
        for sdk in sdks:
            with self.subTest(sdk=sdk):
                # Only the SDK selection changes, not sources or ordinary flags.
                self.run_command([*configure, f"-DCMAKE_OSX_SYSROOT={sdk}"], self.env)
                self.run_command(["cmake", "--build", build, "--target", "cmake_native"], self.env)
                self.run_command(cargo_build, self.env)
                objects = sorted((build / "ffi/cargo-target" / self.target).glob("debug/build/*/out/*-native.o"))
                self.assertEqual(len(objects), 2)
                output = (objects[0].parent.parent / "output").read_text(encoding="utf-8")
                for language in ("CFLAGS", "CXXFLAGS"):
                    key = f"{language}_{self.target.replace('-', '_')}"
                    self.assertIn(f"cargo:rerun-if-env-changed={key}", output)
                    prefix = f"{key} = Some("
                    flags = shlex.split(
                        next(
                            line.removeprefix(prefix).removesuffix(")")
                            for line in output.splitlines()
                            if line.startswith(prefix)
                        )
                    )
                    self.assertEqual(flags[flags.index("-isysroot") + 1], str(sdk))
                digests = {obj: hashlib.sha256(obj.read_bytes()).hexdigest() for obj in objects}
                if previous:
                    self.assertEqual(digests.keys(), previous.keys())
                    for obj, digest in digests.items():
                        self.assertNotEqual(digest, previous[obj], f"SDK change did not rebuild {obj.name}")
                previous = digests
                timestamps = [obj.stat().st_mtime_ns for obj in objects]
                self.run_command(cargo_build, self.env)
                self.assertEqual([obj.stat().st_mtime_ns for obj in objects], timestamps, "Cargo should remain fresh")

        self.run_command([*configure, "-DCMAKE_OSX_SYSROOT=macosx"], self.env)
        commands = shlex.split(
            self.run_command(["ninja", "-C", build, "-t", "commands", "vortex_ffi_cargo_build"], self.env)
        )
        for language in ("CFLAGS", "CXXFLAGS"):
            flags = next(arg for arg in commands if arg.startswith(f"-DVORTEX_{language}="))
            self.assertIn(f"-isysroot;{real_sdk}", flags, "CMake must resolve the named SDK before forwarding it")

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
