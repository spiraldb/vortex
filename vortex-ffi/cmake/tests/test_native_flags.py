# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Keep parent warning policy out of Cargo's vendored native dependencies."""

import json
import shlex
import shutil
import subprocess
import unittest
from pathlib import Path

import test_cargo_environment

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


if __name__ == "__main__":
    unittest.main()
