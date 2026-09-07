# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Check example artifact paths without building Vortex or fetching dependencies."""

import json
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class ExampleOutputTests(unittest.TestCase):
    def test_shared_runtime_output_directory(self):
        repo = Path(__file__).resolve().parents[3]
        examples = {
            "ffi": ("vortex-ffi/examples", ("scan", "scan_to_arrow", "dtype", "write_sample")),
            "cpp": ("lang/cpp/examples", ("reader", "writer", "dtype", "scan", "scan_to_arrow")),
        }
        expected = {
            f"{layer}_{name}": (directory, name) for layer, (directory, names) in examples.items() for name in names
        }

        with tempfile.TemporaryDirectory(prefix="vortex-example-outputs-") as temporary:
            source = Path(temporary).resolve()
            build = source / "build"
            query = build / ".cmake/api/v1/query"
            query.mkdir(parents=True)
            (query / "codemodel-v2").touch()
            (source / "CMakeLists.txt").write_text(
                textwrap.dedent("""\
                    cmake_minimum_required(VERSION 3.20)
                    project(ExampleOutputs LANGUAGES C CXX)
                    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
                    add_library(Vortex::ffi_static INTERFACE IMPORTED)
                    add_library(Vortex::cpp_static INTERFACE IMPORTED)
                    add_library(nanoarrow_shared INTERFACE IMPORTED)
                    add_subdirectory("${VORTEX_SOURCE_DIR}/vortex-ffi/examples" vortex-ffi/examples)
                    add_subdirectory("${VORTEX_SOURCE_DIR}/lang/cpp/examples" lang/cpp/examples)
                    """),
                encoding="utf-8",
            )
            result = subprocess.run(
                ["cmake", "-S", str(source), "-B", str(build), f"-DVORTEX_SOURCE_DIR={repo.as_posix()}"],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            reply = build / ".cmake/api/v1/reply"
            indexes = list(reply.glob("index-*.json"))
            self.assertEqual(len(indexes), 1)
            index = json.loads(indexes[0].read_text(encoding="utf-8"))
            codemodel = json.loads((reply / index["reply"]["codemodel-v2"]["jsonFile"]).read_text(encoding="utf-8"))
            self.assertTrue(codemodel["configurations"])
            for configuration in codemodel["configurations"]:
                with self.subTest(configuration=configuration["name"]):
                    targets = {target["name"]: target for target in configuration["targets"]}
                    self.assertTrue(expected.keys() <= targets.keys())
                    artifacts = {}
                    for target_name in expected:
                        target = json.loads((reply / targets[target_name]["jsonFile"]).read_text(encoding="utf-8"))
                        self.assertEqual(len(target["artifacts"]), 1, target_name)
                        artifacts[target_name] = (build / target["artifacts"][0]["path"]).resolve()
                    self.assertEqual(
                        len(set(artifacts.values())),
                        len(expected),
                        f"Example artifacts collide: {artifacts}",
                    )
                    for target_name, (directory, name) in expected.items():
                        with self.subTest(target=target_name):
                            expected_directory = build / directory
                            # Multi-config generators append the selected configuration to runtime output directories.
                            if index["cmake"]["generator"]["multiConfig"]:
                                expected_directory /= configuration["name"]
                            self.assertEqual(artifacts[target_name].parent, expected_directory)
                            self.assertIn(artifacts[target_name].name, (name, f"{name}.exe"))


if __name__ == "__main__":
    unittest.main()
