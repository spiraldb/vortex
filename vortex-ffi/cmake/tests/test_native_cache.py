# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Real host/target instrumentation and optional sccache regressions with private cache state."""

import hashlib
import json
import shlex
import shutil
import socket
import subprocess
import unittest
from pathlib import Path

import test_cargo_environment


@unittest.skipUnless(all(shutil.which(tool) for tool in ("cmake", "nm")), "CMake and nm are required")
class NativeCacheTests(test_cargo_environment.CargoEnvironmentFixture):
    def run_command(self, command, env):
        result = subprocess.run(
            list(map(str, command)),
            cwd=self.work,
            env=env,
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def cache_stats(self, env):
        return json.loads(self.run_command([env["RUSTC_WRAPPER"], "--show-stats", "--stats-format=json"], env))["stats"]

    @unittest.skipUnless(all(shutil.which(tool) for tool in ("ar", "ranlib")), "ar and ranlib are required")
    def test_coverage_build_dependency_links_and_target_stays_instrumented(self):
        self.prepare_native_toolchain()
        self.tools.update(AR=shutil.which("ar"), RANLIB=shutil.which("ranlib"))
        self.compiler_arg1["CC"] = "--coverage"
        env = self.run_driver(["-O0", "-fPIC"], ["-O0", "-fPIC"], {"CXXFLAGS": "--coverage"})
        env["RUSTC"] = shutil.which("rustc")
        fixture = self.work / "coverage"
        helper = fixture / "host-helper"
        helper.mkdir(parents=True)
        (fixture / "Cargo.toml").write_text(
            '[workspace]\n[package]\nname = "coverage-fixture"\nversion = "0.0.0"\nedition = "2021"\n'
            '[lib]\npath = "lib.rs"\ncrate-type = ["staticlib"]\n'
            '[dependencies]\nhost-helper = { path = "host-helper" }\n'
            '[build-dependencies]\nhost-helper = { path = "host-helper" }\n',
            encoding="utf-8",
        )
        (fixture / "lib.rs").write_text("pub fn value(x: i32) -> i32 { host_helper::value(x) }\n", encoding="utf-8")
        (fixture / "build.rs").write_text("fn main() { assert_eq!(host_helper::value(5), 24); }\n", encoding="utf-8")
        (helper / "Cargo.toml").write_text(
            '[package]\nname = "host-helper"\nversion = "0.0.0"\nedition = "2021"\n'
            f'[lib]\npath = "lib.rs"\n[build-dependencies]\ncc = "={self.cc_version}"\n',
            encoding="utf-8",
        )
        (helper / "lib.rs").write_text(
            'extern "C" { fn native_c(x: i32) -> i32; fn native_cpp(x: i32) -> i32; }\n'
            "pub fn value(x: i32) -> i32 { unsafe { native_c(x) + native_cpp(x) } }\n",
            encoding="utf-8",
        )
        (helper / "build.rs").write_text(
            "fn main() {\n"
            '    cc::Build::new().file("native_c.c").compile("native_c");\n'
            '    cc::Build::new().cpp(true).file("native_cpp.cpp").compile("native_cpp");\n'
            "}\n",
            encoding="utf-8",
        )
        (helper / "native_c.c").write_text("int native_c(int x) { return x + 7; }\n", encoding="utf-8")
        (helper / "native_cpp.cpp").write_text('extern "C" int native_cpp(int x) { return x + 7; }\n', encoding="utf-8")
        target_dir = fixture / "target"
        # Cargo, not the test, clears host Rust flags. Calling both native functions
        # forces their objects into the build-script link, exposing missing runtimes.
        self.run_command(
            [
                "cargo",
                "build",
                "--offline",
                "--manifest-path",
                fixture / "Cargo.toml",
                "--target",
                self.target,
                "--target-dir",
                target_dir,
            ],
            env,
        )
        for host, directory in ((True, target_dir), (False, target_dir / self.target)):
            objects = sorted(directory.glob("debug/build/host-helper-*/out/*native_*.o"))
            self.assertEqual(len(objects), 2, objects)
            for obj in objects:
                with self.subTest(host=host, object=obj.name):
                    symbols = self.run_command(["nm", "-u", obj], env)
                    self.assertEqual("llvm_gcda" in symbols, not host, symbols)
                    self.assertEqual(obj.with_suffix(".gcno").exists(), not host)

    def test_host_target_objects_and_rust_cache_hits(self):
        sccache = shutil.which("sccache")

        if not sccache:
            self.skipTest("sccache is required for the real native-cache regression")
        probe = self.prepare_cc_probe()
        wrapper = self.work / "cache tools' directory" / "sccache"
        wrapper.parent.mkdir()
        wrapper.symlink_to(sccache)
        include = self.work / "include directory"
        include.mkdir()
        (include / "fixture's header.h").write_text("#define HEADER_VALUE 1\n", encoding="utf-8")
        flags = ["-O0", "-fPIC", "-fsanitize=undefined", "--coverage"]
        required = [f"-I{include}", '-DCACHE_TEXT="hello world"', "-fsanitize=undefined", "--coverage"]
        self.compiler_arg1 = {language: shlex.join(required) for language in ("CC", "CXX")}
        env = self.run_driver(flags, flags, {"RUSTC_WRAPPER": str(wrapper)})
        self.assertEqual(env["RUSTC_WRAPPER"], str(wrapper))
        # Never connect to the developer's server, inherit remote-cache settings,
        # or let the test's Rust requests enter an ambient cache.
        env = {key: value for key, value in env.items() if not key.startswith("SCCACHE_")}
        config = self.work / "sccache.toml"
        config.write_text("", encoding="utf-8")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env.update(
            SCCACHE_DIR=str(self.work / "cache"),
            SCCACHE_CONF=str(config),
            SCCACHE_SERVER_PORT=str(port),
            SCCACHE_CACHE_SIZE="64M",
        )
        started = False
        try:
            self.run_command([wrapper, "--start-server"], env)
            started = True
            for language, extension in (("CC", "c"), ("CXX", "cpp")):
                for order in (("host", "target"), ("target", "host")):
                    with self.subTest(language=language, first=order[0]):
                        source = self.work / f"{language} {order[0]} source.{extension}"
                        assertion = "_Static_assert" if language == "CC" else "static_assert"
                        source.write_text(
                            '#include "fixture\'s header.h"\n'
                            f'{assertion}(sizeof(CACHE_TEXT) == sizeof("hello world"), "flag quoting");\n'
                            f"int probe_{order[0]}(int *pointer, int index) {{\n"
                            "    return pointer[index] + HEADER_VALUE;\n}\n",
                            encoding="utf-8",
                        )
                        obj = self.work / f"{language} {order[0]} object.o"
                        reference = {}
                        before = self.cache_stats(env)
                        # Matching argv and output paths are essential: inherited
                        # Rust flags can otherwise hide the old outer-cache collision.
                        for cached, sequence in ((False, order), (True, order * 2)):
                            compiler_args = None
                            for mode in sequence:
                                selected = env.copy()
                                if not cached:
                                    selected.pop("RUSTC_WRAPPER")
                                if mode == "host":
                                    selected["CARGO_ENCODED_RUSTFLAGS"] = ""
                                obj.unlink(missing_ok=True)
                                output = self.run_cc_probe(probe, selected, language, source, obj)
                                args = next(line for line in output.splitlines() if line.startswith("args="))
                                if compiler_args is None:
                                    compiler_args = args
                                self.assertEqual(args, compiler_args)
                                digest = hashlib.sha256(obj.read_bytes()).hexdigest()
                                symbols = self.run_command(["nm", "-u", obj], selected)
                                for runtime in ("ubsan", "llvm_gcda"):
                                    self.assertEqual(
                                        runtime in symbols, mode == "target", f"{cached=} {mode=}: {symbols}"
                                    )
                                if cached:
                                    self.assertEqual(digest, reference[mode], f"wrong cached {mode} object")
                                else:
                                    reference[mode] = digest
                            if not cached:
                                self.assertNotEqual(reference["host"], reference["target"])
                                self.assertEqual(self.cache_stats(env)["compile_requests"], before["compile_requests"])
                        after = self.cache_stats(env)
                        self.assertGreaterEqual(
                            after["cache_hits"]["counts"].get("C/C++", 0)
                            - before["cache_hits"]["counts"].get("C/C++", 0),
                            2,
                            after,
                        )

            # The very same RUSTC_WRAPPER must still produce Rust cache hits.
            sysroot = self.run_command(["rustc", "--print", "sysroot"], env).strip()
            rustc = Path(sysroot) / "bin/rustc"
            source = self.work / "rust probe.rs"
            source.write_text("pub fn plus_one(value: i32) -> i32 { value + 1 }\n", encoding="utf-8")
            output_dir = self.work / "rust output"
            output_dir.mkdir()
            before = self.cache_stats(env)
            command = [
                env["RUSTC_WRAPPER"],
                rustc,
                "--crate-name",
                "rust_probe",
                "--crate-type=rlib",
                "--emit=link",
                "--out-dir",
                output_dir,
                source,
            ]
            for _ in range(2):
                (output_dir / "librust_probe.rlib").unlink(missing_ok=True)
                self.run_command(command, env)
                self.assertTrue((output_dir / "librust_probe.rlib").is_file())
            after = self.cache_stats(env)
            self.assertGreaterEqual(
                after["cache_hits"]["counts"].get("Rust", 0) - before["cache_hits"]["counts"].get("Rust", 0),
                1,
                after,
            )
        finally:
            if started:
                self.run_command([wrapper, "--stop-server"], env)


if __name__ == "__main__":
    unittest.main()
