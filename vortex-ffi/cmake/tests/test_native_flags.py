# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Keep parent warning policy out of Cargo's vendored native dependencies."""

import hashlib
import json
import shlex
import shutil
import subprocess
import unittest
from pathlib import Path

import test_cargo_environment
import test_compiler_commands

ERROR_FLAGS = ["-Werror", "-Werror=unused-variable", "-pedantic-errors"]


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class NativeFlagsTests(test_cargo_environment.CargoEnvironmentFixture):
    def native_flags(self, configuration="DEBUG", **inputs):
        # Extract the production function, not a second implementation of its policy.
        cmake = Path(__file__).resolve().parents[1]
        source = (cmake / "Configure.cmake").read_text(encoding="utf-8")
        function = source[source.index("function(_vortex_native_flags\n") :].split("endfunction()", 1)[0]
        output = self.work / "flags.txt"
        script = self.work / "flags.cmake"
        script.write_text(
            "cmake_minimum_required(VERSION 3.25)\n"
            f'include("{cmake.as_posix()}/Helpers.cmake")\n'
            f"{function}endfunction()\n"
            f'_vortex_native_flags("{configuration}" "${{DEPLOYMENT_TARGET}}" "${{SANITIZER}}" c cxx)\n'
            f'file(WRITE "{output.as_posix()}" "${{c}}\n${{cxx}}\n")\n',
            encoding="utf-8",
        )
        result = subprocess.run(
            ["cmake", *(f"-D{name}={value}" for name, value in inputs.items()), "-P", str(script)],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return [line.split(";") for line in output.read_text(encoding="utf-8").splitlines()]

    def test_global_and_configuration_flags_preserve_everything_else(self):
        common = [
            "-O2",
            "-g",
            "-Wall",
            "-Wno-error",
            "-Wno-error=unused-variable",
            "-fshort-enums",
            "-D_GLIBCXX_USE_CXX11_ABI=0",
            "-I/sdk's include",
            "--sysroot=/sdk with spaces",
            '-DPOLICY="-Werror -pedantic-errors"',
            "-Werrorish",
            "-pedantic-errors-extra",
        ]
        for configuration in ("DEBUG", "RELEASE", "RELWITHDEBINFO", "MINSIZEREL"):
            with self.subTest(configuration=configuration):
                inputs = {"DEPLOYMENT_TARGET": "13.0", "SANITIZER": "-fsanitize=undefined"}
                expected = []
                for language, standard in (("C", "c11"), ("CXX", "c++17")):
                    specific = ["-O1", f"-std={standard}", "-fno-omit-frame-pointer"]
                    inputs[f"CMAKE_{language}_FLAGS"] = shlex.join([*ERROR_FLAGS, *common])
                    inputs[f"CMAKE_{language}_FLAGS_{configuration}"] = shlex.join(
                        [*specific, *ERROR_FLAGS, "-Werror=another-warning"]
                    )
                    expected.append([*common, *specific, "-mmacosx-version-min=13.0", "-fsanitize=undefined", "-fPIC"])
                actual = self.native_flags(configuration, **inputs)
                self.assertEqual(actual, expected)
                self.assert_native_environment(self.run_driver(*actual), *expected)

    def test_explicit_compiler_arguments_and_ambient_flags_remain_untouched(self):
        policy = shlex.join(ERROR_FLAGS)
        flags = self.native_flags(CMAKE_C_FLAGS=policy, CMAKE_CXX_FLAGS_DEBUG=policy)
        self.assertEqual(flags, [["-fPIC"], ["-fPIC"]])
        self.compiler_arg1 = {language: policy for language in ("CC", "CXX")}
        ambient = {name: policy for name in ("CFLAGS", "CXXFLAGS")}
        env = self.run_driver(*flags, ambient)
        for language, name in (("CC", "CFLAGS"), ("CXX", "CXXFLAGS")):
            self.assertEqual(env[name], policy)
            effective = [flag for key in reversed(self.cc_names(name)) for flag in shlex.split(env.get(key, ""))]
            for host in (False, True):
                with self.subTest(language=language, host=host):
                    selected = dict(env, CARGO_ENCODED_RUSTFLAGS="" if host else env["CARGO_ENCODED_RUSTFLAGS"])
                    result = subprocess.run(
                        [*self.native_command(env, language), *effective],
                        env=selected,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        check=True,
                    )
                    self.assertEqual(json.loads(result.stdout)["flags"], [*ERROR_FLAGS, *ERROR_FLAGS, "-fPIC"])


@unittest.skipUnless(
    all(shutil.which(tool) for tool in ("cmake", "ninja", "cargo", "rustc", "clang", "clang++")),
    "CMake, Ninja, Cargo, rustc, and Clang are required",
)
class NativeFlagsBuildTests(unittest.TestCase):
    def test_vendored_warning_and_cargo_freshness(self):
        # Reuse the tiny real Cargo workspace without rerunning its unrelated tests.
        fixture = test_compiler_commands.CompilerCommandTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        include = fixture.work / "parent include's directory"
        include.mkdir()
        (include / "parent.h").write_text("#define HEADER_VALUE 3\n", encoding="utf-8")
        for extension, macro in (("c", "REVIEW_ARG1"), ("cpp", "REVIEW_CXX_ARG1")):
            assertion = "_Static_assert" if extension == "c" else "static_assert"
            (fixture.ffi / f"native.{extension}").write_text(
                '#include "parent.h"\n'
                '#ifndef __OPTIMIZE__\n#error "parent optimization was lost"\n#endif\n'
                "enum small { zero, one };\n"
                f'{assertion}(sizeof(enum small) == 1, "parent ABI flag was lost");\n'
                f"int native_{extension}(void) {{\n"
                "    int vendored_unused;\n"
                f"    return {macro} + PARENT_VALUE + CONFIG_VALUE + HEADER_VALUE;\n}}\n",
                encoding="utf-8",
            )
        build = fixture.work / "native-flags"
        configure = ["cmake", "-G", "Ninja", "-S", fixture.source, "-B", build, "-DCMAKE_BUILD_TYPE=Debug"]
        globals_by_language = {}
        for compiler, language, macro, extension in zip(
            fixture.compilers, ("C", "CXX"), ("REVIEW_ARG1", "REVIEW_CXX_ARG1"), ("c", "cpp"), strict=True
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
                    str(fixture.ffi / f"native.{extension}"),
                    "-o",
                    str(fixture.work / f"control-{extension}.o"),
                ],
                env=fixture.env,
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
            fixture.run_command([*configure, *options], fixture.env)

        cargo_build = ["cmake", "--build", build, "--target", "vortex_ffi_cargo_build"]
        reconfigure(7, ERROR_FLAGS)
        fixture.run_command(cargo_build, fixture.env)
        archives = sorted((build / "ffi/cargo-target" / fixture.target).glob("debug/build/*/out/libnative_*.a"))
        self.assertEqual(len(archives), 2)
        output = (archives[0].parent.parent / "output").read_text(encoding="utf-8")
        self.assertEqual(output.count("warning: unused variable 'vendored_unused'"), 2)
        timestamps = [archive.stat().st_mtime_ns for archive in archives]
        digests = [hashlib.sha256(archive.read_bytes()).hexdigest() for archive in archives]
        fixture.run_command(cargo_build, fixture.env)
        self.assertEqual([archive.stat().st_mtime_ns for archive in archives], timestamps)
        reconfigure(7, [])
        fixture.run_command(cargo_build, fixture.env)
        self.assertEqual(
            [archive.stat().st_mtime_ns for archive in archives],
            timestamps,
            "Changing only the parent's warning-as-error policy must leave Cargo fresh",
        )
        reconfigure(8, [])
        fixture.run_command(cargo_build, fixture.env)
        for archive, timestamp, digest in zip(archives, timestamps, digests, strict=True):
            self.assertNotEqual(archive.stat().st_mtime_ns, timestamp)
            self.assertNotEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), digest)


if __name__ == "__main__":
    unittest.main()
