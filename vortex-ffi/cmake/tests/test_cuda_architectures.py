# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Exercise CUDA architecture policy and Cargo forwarding without a CUDA toolkit."""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import test_cargo_environment

CMAKE_DIR = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class CudaArchitectureTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="vortex-cuda-architectures-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()

    def policy(self, architectures=None, *, cached_architectures=None, succeeds=True):
        setup = ""
        if cached_architectures is not None:
            setup += f'set(CMAKE_CUDA_ARCHITECTURES [==[{cached_architectures}]==] CACHE STRING "Parent policy")\n'
        if architectures is not None:
            setup += f"set(CMAKE_CUDA_ARCHITECTURES [==[{architectures}]==])\n"
        for name in ("flags", "parent", "cache"):
            (self.work / f"{name}.txt").unlink(missing_ok=True)
        script = self.work / "policy.cmake"
        script.write_text(
            "cmake_minimum_required(VERSION 3.25)\n"
            f'include("{CMAKE_DIR.as_posix()}/Cuda.cmake")\n'
            f"{setup}"
            'set(flags "stale output")\n'
            "_vortex_resolve_cuda_architectures(flags)\n"
            f'file(WRITE "{self.work.as_posix()}/flags.txt" "${{flags}}")\n'
            "if(DEFINED CMAKE_CUDA_ARCHITECTURES)\n"
            f'    file(WRITE "{self.work.as_posix()}/parent.txt" "${{CMAKE_CUDA_ARCHITECTURES}}")\n'
            "endif()\n"
            "if(DEFINED CACHE{CMAKE_CUDA_ARCHITECTURES})\n"
            f'    file(WRITE "{self.work.as_posix()}/cache.txt" "$CACHE{{CMAKE_CUDA_ARCHITECTURES}}")\n'
            "endif()\n",
            encoding="utf-8",
        )
        result = subprocess.run(["cmake", "-P", str(script)], capture_output=True, text=True, timeout=30, check=False)
        output = result.stdout + result.stderr
        if not succeeds:
            self.assertNotEqual(result.returncode, 0, output)
            self.assertIn("CMAKE_CUDA_ARCHITECTURES", output)
            return output
        self.assertEqual(result.returncode, 0, output)
        parent = cached_architectures if architectures is None else architectures
        for name, expected in (("parent", parent), ("cache", cached_architectures)):
            path = self.work / f"{name}.txt"
            self.assertEqual(path.exists(), expected is not None, f"Policy changed {name} definedness")
            if expected is not None:
                self.assertEqual(path.read_text(encoding="utf-8"), expected, f"Policy changed {name} value")
        flags = (self.work / "flags.txt").read_text(encoding="utf-8")
        return flags.split(";") if flags else []

    def test_undefined_defaults_to_native_without_setting_parent_or_cache(self):
        self.assertEqual(self.policy(), ["-arch=native"])

    def test_standalone_keywords(self):
        for architectures in ("native", "all", "all-major"):
            with self.subTest(architectures=architectures):
                self.assertEqual(self.policy(architectures), [f"-arch={architectures}"])

    def test_numeric_architectures(self):
        self.assertEqual(
            self.policy("80;86-real;90-virtual;80"),
            [
                "--generate-code=arch=compute_80,code=[compute_80,sm_80]",
                "--generate-code=arch=compute_86,code=[sm_86]",
                "--generate-code=arch=compute_90,code=[compute_90]",
                "--generate-code=arch=compute_80,code=[compute_80,sm_80]",
            ],
        )

    def test_architecture_and_family_specific_suffixes(self):
        self.assertEqual(
            self.policy("90a;100f;90a-real;100f-virtual"),
            [
                "--generate-code=arch=compute_90a,code=[compute_90a,sm_90a]",
                "--generate-code=arch=compute_100f,code=[compute_100f,sm_100f]",
                "--generate-code=arch=compute_90a,code=[sm_90a]",
                "--generate-code=arch=compute_100f,code=[compute_100f]",
            ],
        )

    def test_capability_support_is_left_to_nvcc(self):
        self.assertEqual(self.policy("999"), ["--generate-code=arch=compute_999,code=[compute_999,sm_999]"])

    def test_false_constants_disable_architecture_flags(self):
        for architectures in ("OFF", "off", "0", "NO", "N", "FALSE", "false", "IGNORE", "NOTFOUND", "gpu-NOTFOUND"):
            with self.subTest(architectures=architectures):
                self.assertEqual(self.policy(architectures), [])

    def test_cache_and_parent_override_remain_unchanged(self):
        self.assertEqual(self.policy(cached_architectures="80-real"), ["--generate-code=arch=compute_80,code=[sm_80]"])
        self.assertEqual(self.policy("OFF", cached_architectures="80-real"), [])
        self.assertEqual(self.policy(cached_architectures="OFF"), [])

    def test_explicit_empty_is_not_a_default_or_false_constant(self):
        for inputs in ({"architectures": ""}, {"cached_architectures": ""}):
            with self.subTest(inputs=inputs):
                self.assertIn("must be non-empty", self.policy(**inputs, succeeds=False))

    def test_invalid_forms(self):
        for architectures in (
            ";",
            ";80",
            "80;",
            "80;;90",
            "native;80",
            "80;native",
            "all;80",
            "80;all-major",
            "native;all",
            "native;native",
            "OFF;80",
            "80;OFF",
            "ON",
            "TRUE",
            "sm_80",
            "compute_80",
            "8.0",
            "80,90",
            "80 90",
            " 80",
            "80 ",
            "80-REAL",
            "80-real-virtual",
            "90b",
            "90af",
            "-arch=sm_80",
            "$<IF:$<BOOL:1>,80,90>",
        ):
            with self.subTest(architectures=architectures):
                self.assertIn("must be used alone", self.policy(architectures, succeeds=False))


@unittest.skipUnless(all(shutil.which(tool) for tool in ("cmake", "ninja")), "CMake and Ninja are required")
class CudaArchitectureForwardingTests(test_cargo_environment.CargoEnvironmentFixture):
    def setUp(self):
        super().setUp()
        self.source = self.work / "source"
        self.source.mkdir()
        self.cuda_root = self.work / "fake CUDA toolkit's"
        nvcc = self.executable(
            "fake CUDA toolkit's/bin/nvcc", "raise SystemExit('NVCC must not run in this fixture')\n"
        )
        rustc = self.executable(
            "rustc",
            "import sys\n"
            "assert sys.argv[1:] == ['-vV'], sys.argv\n"
            "print('rustc 1.95.0\\nhost: x86_64-unknown-linux-gnu\\nrelease: 1.95.0')\n",
        )
        (self.source / "FindCUDAToolkit.cmake").write_text(
            'if(NOT VORTEX_ENABLE_CUDA)\n    message(FATAL_ERROR "CPU build must not discover CUDA")\nendif()\n'
            "set(CUDAToolkit_FOUND TRUE)\n"
            f'set(CUDAToolkit_NVCC_EXECUTABLE "{Path(nvcc).as_posix()}")\n'
            f'set(CUDAToolkit_TARGET_DIR "{self.cuda_root.as_posix()}")\n',
            encoding="utf-8",
        )
        # Include the real configure module without enabling compiler languages.
        # Stub discovery only: the production custom command and Cargo driver run unchanged.
        (self.source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.25)\n"
            "project(CudaArchitectureFixture LANGUAGES NONE)\n"
            "set(CMAKE_SYSTEM_NAME Linux)\n"
            "set(APPLE FALSE)\n"
            "set(CMAKE_BUILD_TYPE Debug)\n"
            'list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")\n'
            "add_library(Threads::Threads INTERFACE IMPORTED)\n"
            f'set(VORTEX_CARGO_EXECUTABLE "{Path(self.cargo).as_posix()}")\n'
            f'set(VORTEX_RUSTC_EXECUTABLE "{Path(rustc).as_posix()}")\n'
            f'set(CMAKE_C_COMPILER "{Path(self.tools["CC"]).as_posix()}")\n'
            f'set(CMAKE_CXX_COMPILER "{Path(self.tools["CXX"]).as_posix()}")\n'
            f'set(CMAKE_AR "{Path(self.tools["AR"]).as_posix()}")\n'
            f'set(CMAKE_RANLIB "{Path(self.tools["RANLIB"]).as_posix()}")\n'
            f'include("{CMAKE_DIR.as_posix()}/Configure.cmake")\n',
            encoding="utf-8",
        )

    def configure_and_record(self, name, architectures=None, *, cuda=True, ambient=None):
        build = self.work / name
        env = os.environ.copy()
        env.pop("VORTEX_CUDA_ARCH_FLAGS", None)
        env.pop("CUDA_PATH", None)
        env.update(ambient or {})
        options = [f"-DVORTEX_ENABLE_CUDA={'ON' if cuda else 'OFF'}"]
        if architectures is not None:
            options.append(f"-DCMAKE_CUDA_ARCHITECTURES={architectures}")
        for command in (
            ["cmake", "-G", "Ninja", "-S", str(self.source), "-B", str(build), *options],
            ["cmake", "--build", str(build), "--target", "vortex_ffi_cargo_build"],
        ):
            result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=30, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((build / "vortex-artifacts/libvortex_ffi.a").read_bytes(), b"recorded archive")
        recording = json.loads((build / "cargo-target/environment.json").read_text(encoding="utf-8"))
        args = recording["args"]
        self.assertEqual(args[args.index("--package") + 1], "vortex-cuda-ffi" if cuda else "vortex-ffi")
        self.assertEqual(recording["env"]["CARGO_ENCODED_RUSTFLAGS"], "\x1f".join(self.rustflags))
        return recording["env"]

    def test_semicolon_list_survives_custom_command_and_uses_unit_separator(self):
        expected = [
            "--generate-code=arch=compute_80,code=[compute_80,sm_80]",
            "--generate-code=arch=compute_86,code=[sm_86]",
            "--generate-code=arch=compute_90a,code=[compute_90a]",
            "--generate-code=arch=compute_100f,code=[compute_100f,sm_100f]",
        ]
        env = self.configure_and_record(
            "numeric", "80;86-real;90a-virtual;100f", ambient={"VORTEX_CUDA_ARCH_FLAGS": "-arch=ambient"}
        )
        self.assertEqual(env["VORTEX_CUDA_ARCH_FLAGS"], "\x1f".join(expected))
        self.assertEqual(env["CUDA_PATH"], str(self.cuda_root))

    def test_default_and_false_override_ambient_architecture_flags(self):
        for architectures, expected in ((None, "-arch=native"), ("OFF", ""), ("FALSE", "")):
            with self.subTest(architectures=architectures):
                env = self.configure_and_record(
                    f"default-or-false-{architectures}",
                    architectures,
                    ambient={"VORTEX_CUDA_ARCH_FLAGS": "-arch=ambient"},
                )
                self.assertIn("VORTEX_CUDA_ARCH_FLAGS", env)
                self.assertEqual(env["VORTEX_CUDA_ARCH_FLAGS"], expected)

    def test_cpu_build_does_not_resolve_or_set_cuda_architectures(self):
        for index, architectures in enumerate(("", "invalid;native")):
            with self.subTest(architectures=architectures):
                env = self.configure_and_record(f"cpu-{index}", architectures, cuda=False)
                self.assertNotIn("VORTEX_CUDA_ARCH_FLAGS", env)
                self.assertNotIn("CUDA_PATH", env)
        env = self.configure_and_record("cpu-ambient", cuda=False, ambient={"VORTEX_CUDA_ARCH_FLAGS": "-arch=ambient"})
        self.assertEqual(env["VORTEX_CUDA_ARCH_FLAGS"], "-arch=ambient")


if __name__ == "__main__":
    unittest.main()
