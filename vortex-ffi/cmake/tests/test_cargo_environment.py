# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Exercise the real Cargo driver with recording Cargo and native tools."""

import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class CargoEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.driver = Path(__file__).resolve().parents[1] / "CargoBuild.cmake"
        temporary = tempfile.TemporaryDirectory(prefix="vortex-cargo-environment-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()
        self.target = "aarch64-apple-darwin"
        self.target_dir = self.work / "cargo target"
        self.archive = self.target_dir / self.target / "debug" / "libvortex_ffi.a"
        self.staged = self.work / "staged" / "libvortex_ffi.a"
        self.cargo = self.executable(
            "recording cargo",
            "import json, os, pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "root = pathlib.Path(args[args.index('--target-dir') + 1])\n"
            "archive = root / args[args.index('--target') + 1] / 'debug/libvortex_ffi.a'\n"
            "archive.parent.mkdir(parents=True, exist_ok=True)\n"
            "archive.write_bytes(b'recorded archive')\n"
            "(root / 'environment.json').write_text(json.dumps(\n"
            "    {'args': args, 'env': dict(os.environ)}))\n",
        )
        self.tools = {
            name: self.executable(
                f"selected {name.lower()}",
                "import json, sys\nprint(json.dumps({'tool': sys.argv[0], 'flags': sys.argv[1:]}))\n",
            )
            for name in ("CC", "CXX", "AR", "RANLIB")
        }
        self.cflags = [
            "-O1",
            "-g",
            "-fPIC",
            "--sysroot=/sdk with spaces",
            '-DC_LABEL="space and apostrophe\'s"',
        ]
        self.cxxflags = [
            "-O2",
            "-fPIC",
            "-std=c++20",
            "--gcc-toolchain=/toolchain with spaces",
            '-DCXX_LABEL="two words"',
            "-DVALUE=-fsanitize=address",
        ]
        self.rustflags = ["-C", "force-frame-pointers=yes", "-C", "relocation-model=pic"]

    def executable(self, name, body):
        path = self.work / name
        path.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
        path.chmod(0o755)
        return str(path)

    def env_names(self, name):
        return (
            name,
            f"HOST_{name}",
            f"TARGET_{name}",
            f"{name}_{self.target.replace('-', '_')}",
            f"{name}_{self.target}",
        )

    def run_driver(self, cflags, cxxflags, ambient=None):
        env = os.environ.copy()
        for name in ("CFLAGS", "CXXFLAGS", "CC", "CXX", "AR", "RANLIB"):
            for key in self.env_names(name):
                env.pop(key, None)
        env.update(ambient or {})
        inputs = {
            "VORTEX_CARGO_EXECUTABLE": self.cargo,
            "VORTEX_RUSTC_EXECUTABLE": sys.executable,
            "VORTEX_RUST_TARGET": self.target,
            "VORTEX_CARGO_TARGET_DIR": self.target_dir,
            "VORTEX_CARGO_PROFILE": "dev",
            "VORTEX_FFI_PACKAGE": "vortex-ffi",
            "VORTEX_CARGO_FFI_ARCHIVE": self.archive,
            "VORTEX_CMAKE_FFI_ARCHIVE": self.staged,
            "VORTEX_RUSTFLAGS": ";".join(self.rustflags),
            "VORTEX_CFLAGS": ";".join(cflags),
            "VORTEX_CXXFLAGS": ";".join(cxxflags),
            "VORTEX_C_COMPILER": self.tools["CC"],
            "VORTEX_CXX_COMPILER": self.tools["CXX"],
            "VORTEX_AR": self.tools["AR"],
            "VORTEX_RANLIB": self.tools["RANLIB"],
        }
        result = subprocess.run(
            ["cmake", *(f"-D{k}={v}" for k, v in inputs.items()), "-P", str(self.driver)],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.staged.read_bytes(), b"recorded archive")
        recording = json.loads((self.target_dir / "environment.json").read_text())
        self.assertEqual(recording["args"][recording["args"].index("--target") + 1], self.target)
        self.assertEqual(recording["env"]["CARGO_ENCODED_RUSTFLAGS"], "\x1f".join(self.rustflags))
        return recording["env"]

    def cc_names(self, name):
        # cc 1.4.0 target_envs(): native HOST == TARGET uses HOST_* for BOTH
        # host build dependencies and target libraries, even with cargo --target.
        return (
            f"{name}_{self.target}",
            f"{name}_{self.target.replace('-', '_')}",
            f"HOST_{name}",
            name,
        )

    def effective_tool(self, env, name):
        return next(env[key] for key in self.cc_names(name) if key in env)

    def compile_flags(self, cargo_env, language, host):
        env = cargo_env.copy()
        env.update(HOST=self.target, TARGET=self.target)
        if host:
            # Cargo clears target Rust flags for host build dependencies when
            # --target is explicit, including when it equals the host triple.
            env["CARGO_ENCODED_RUSTFLAGS"] = ""
        self.assertEqual(env["CC_SHELL_ESCAPED_FLAGS"], "1")
        name = "CFLAGS" if language == "CC" else "CXXFLAGS"
        # cc envflags() accumulates ALL matching flag variables, low to high
        # priority; unlike tool selection, setting a higher-priority one is not enough.
        flags = [flag for key in reversed(self.cc_names(name)) for flag in shlex.split(env.get(key, ""))]
        arguments = ["-O0", "-c", "source with spaces", "-o", "object file"]
        result = subprocess.run(
            [self.effective_tool(env, language), *arguments, *flags],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        recording = json.loads(result.stdout)
        self.assertEqual(recording["tool"], self.tools[language])
        self.assertEqual(recording["flags"][: len(arguments)], arguments)
        return recording["flags"][len(arguments) :]

    def assert_native_environment(self, env, cflags, cxxflags):
        for name in ("AR", "RANLIB"):
            self.assertEqual(self.effective_tool(env, name), self.tools[name])
        for language, expected in (("CC", cflags), ("CXX", cxxflags)):
            with self.subTest(language=language):
                self.assertEqual(self.compile_flags(env, language, host=False), expected)
                self.assertEqual(
                    self.compile_flags(env, language, host=True),
                    [flag for flag in expected if not flag.startswith("-fsanitize=")],
                )

    def test_only_sanitizer_flags_are_target_only(self):
        for flags in (
            ["-fsanitize=address,undefined"],
            ["-fsanitize=undefined", "-fsanitize=address"],
            [],
        ):
            with self.subTest(flags=flags):
                cflags = [*flags, *self.cflags]
                cxxflags = [*self.cxxflags[:1], *flags, *self.cxxflags[1:]]
                env = self.run_driver(cflags, cxxflags)
                self.assert_native_environment(env, cflags, cxxflags)

    def test_flag_changes_invalidate_cc_builds(self):
        original = self.run_driver(self.cflags, self.cxxflags)
        unchanged = self.run_driver(self.cflags, self.cxxflags)
        for name in ("CC", "CXX"):
            self.assertEqual(self.effective_tool(original, name), self.effective_tool(unchanged, name))
        cflags = [*self.cflags, "-fsanitize=undefined"]
        changed = self.run_driver(cflags, self.cxxflags)
        for name in ("CC", "CXX"):
            self.assertEqual(self.effective_tool(original, name), self.effective_tool(changed, name))
        key = f"CFLAGS_{self.target.replace('-', '_')}"
        self.assertEqual(shlex.split(original[key]), self.cflags)
        self.assertEqual(shlex.split(changed[key]), cflags)
        self.assert_native_environment(changed, cflags, self.cxxflags)

    def test_ambient_flags_accumulate_without_bypassing_host_filter(self):
        global_flags = ["-DORDER=global", "-fsanitize=address"]
        host_flags = ["-DORDER=host", "-fsanitize=leak"]
        literal_flags = ["-DORDER=literal", "-fsanitize=undefined"]
        ambient = {}
        for name in ("CFLAGS", "CXXFLAGS"):
            ambient.update(
                {
                    name: shlex.join(global_flags),
                    f"HOST_{name}": shlex.join(host_flags),
                    f"TARGET_{name}": "-DUNUSED=target -fsanitize=thread",
                    f"{name}_{self.target.replace('-', '_')}": "-DOVERWRITTEN=1 -fsanitize=memory",
                    f"{name}_{self.target}": shlex.join(literal_flags),
                }
            )
        for name in self.tools:
            for key in self.env_names(name):
                ambient[key] = "/ambient/tool-must-not-run"
        cflags = [*self.cflags, "-fsanitize=undefined"]
        cxxflags = [*self.cxxflags, "-fsanitize=undefined"]
        env = self.run_driver(cflags, cxxflags, ambient)
        self.assert_native_environment(
            env,
            [*global_flags, *host_flags, *cflags, *literal_flags],
            [*global_flags, *host_flags, *cxxflags, *literal_flags],
        )
        for name in ("CFLAGS", "CXXFLAGS"):
            for key in self.env_names(name):
                if key != f"{name}_{self.target.replace('-', '_')}":
                    self.assertEqual(env[key], ambient[key])

    def test_launcher_does_not_inject_flags_into_probes(self):
        env = self.run_driver(["-v", "-fsanitize=undefined"], ["-Werror", "-fsanitize=undefined"])
        for language in ("CC", "CXX"):
            with self.subTest(language=language):
                arguments = ["-E", "detect_compiler_family.c"]
                result = subprocess.run(
                    [self.effective_tool(env, language), *arguments],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True,
                )
                self.assertEqual(json.loads(result.stdout)["flags"], arguments)

    @unittest.skipUnless(
        all(shutil.which(tool) for tool in ("cargo", "rustc", "clang", "clang++")),
        "Cargo, rustc, and Clang are required for real cc-rs probes",
    )
    def test_real_cc_compiler_family_and_flag_support(self):
        cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
        if not any((cargo_home / "registry/src").glob("*/cc-1.4.0")):
            self.skipTest("cc 1.4.0 must be cached for the offline probe test")
        # A tiny executable exercises cc-rs itself, without compiling Vortex or
        # downloading dependencies. Each run starts with a fresh cc probe cache.
        probe = self.work / "probe"
        probe.mkdir()
        (probe / "Cargo.toml").write_text(
            '[workspace]\n[package]\nname = "cc-probe"\nversion = "0.0.0"\n'
            'edition = "2021"\n[[bin]]\nname = "cc-probe"\npath = "main.rs"\n'
            '[dependencies]\ncc = "=1.4.0"\n',
            encoding="utf-8",
        )
        (probe / "main.rs").write_text(
            "fn main() -> Result<(), Box<dyn std::error::Error>> {\n"
            '    let target = std::env::var("TARGET")?;\n'
            "    let mut build = cc::Build::new();\n"
            "    build.target(&target).host(&target).opt_level(0).debug(false)\n"
            "        .cargo_metadata(false).inherit_rustflags(false)\n"
            '        .cpp(std::env::args().nth(1).as_deref() == Some("CXX"));\n'
            "    let clang = build.try_get_compiler()?.is_like_clang();\n"
            '    let supported = build.is_flag_supported("-fno-omit-frame-pointer")?;\n'
            '    println!("{{\\"clang\\": {clang}, \\"supported\\": {supported}}}");\n'
            "    Ok(())\n}\n",
            encoding="utf-8",
        )
        result = subprocess.run(
            [
                "cargo",
                "build",
                "--offline",
                "--manifest-path",
                str(probe / "Cargo.toml"),
                "--target-dir",
                str(probe / "target"),
            ],
            cwd=probe,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        version = subprocess.run(
            ["rustc", "-vV"],
            cwd=probe,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout
        self.target = next(line.removeprefix("host: ") for line in version.splitlines() if line.startswith("host: "))
        self.archive = self.target_dir / self.target / "debug" / "libvortex_ffi.a"
        self.tools.update(CC=shutil.which("clang"), CXX=shutil.which("clang++"))
        for language, flag in (("CXX", "-Werror"), ("CC", "-v"), ("CXX", "-v")):
            env = self.run_driver([flag, "-fsanitize=undefined"], [flag, "-fsanitize=undefined"])
            env.update(HOST=self.target, TARGET=self.target, OUT_DIR=str(probe / "out"))
            for host in (False, True):
                with self.subTest(language=language, flag=flag, host=host):
                    build_env = env.copy()
                    if host:
                        build_env["CARGO_ENCODED_RUSTFLAGS"] = ""
                    result = subprocess.run(
                        [str(probe / "target/debug/cc-probe"), language],
                        env=build_env,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        check=False,
                    )
                    output = result.stdout + result.stderr
                    self.assertEqual(result.returncode, 0, output)
                    self.assertEqual(
                        json.loads(result.stdout.splitlines()[-1]),
                        {"clang": True, "supported": True},
                        output,
                    )


if __name__ == "__main__":
    unittest.main()
