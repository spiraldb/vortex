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

REPO_ROOT = Path(__file__).resolve().parents[3]


def rust_toolchain_environment():
    """Keep the repository's Rust selection when fixtures run outside its tree."""
    env = os.environ.copy()
    if not env.get("RUSTUP_TOOLCHAIN") and shutil.which("rustup"):
        result = subprocess.run(
            ["rustup", "show", "active-toolchain"],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        env["RUSTUP_TOOLCHAIN"] = result.stdout.split()[0]
    return env


class CargoEnvironmentFixture(unittest.TestCase):
    def setUp(self):
        self.driver = Path(__file__).resolve().parents[1] / "CargoBuild.cmake"
        temporary = tempfile.TemporaryDirectory(prefix="vortex-cargo-environment-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()
        self.target = "aarch64-apple-darwin"
        self.target_dir = self.work / "cargo target's"
        self.archive = self.target_dir / self.target / "debug" / "libvortex_ffi.a"
        self.staged = self.work / "staged" / "libvortex_ffi.a"
        self.header = self.work / "vortex.h"
        self.header.write_text("/* recorded header */\n", encoding="utf-8")
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
                f"selected {name.lower()}'s",
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
        self.compiler_arg1 = {"CC": "", "CXX": ""}
        self.rustup_toolchain = os.environ.get("RUSTUP_TOOLCHAIN", "")

    def executable(self, name, body):
        path = self.work / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
        path.chmod(0o755)
        return str(path)

    def recording_cache(self, name="sccache"):
        self.cache_log = self.work / "native cache.jsonl"
        return self.executable(
            f"cache tools' directory/{name}",
            "import json, os, sys\n"
            f"with open({str(self.cache_log)!r}, 'a', encoding='utf-8') as log:\n"
            "    log.write(json.dumps({'compiler': sys.argv[1], 'flags': sys.argv[2:]}) + '\\n')\n"
            "os.execv(sys.argv[1], sys.argv[1:])\n",
        )

    def cache_calls(self):
        if not self.cache_log.exists():
            return []
        return [json.loads(line) for line in self.cache_log.read_text(encoding="utf-8").splitlines()]

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
        for key in ("RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CC_KNOWN_WRAPPER_CUSTOM"):
            env.pop(key, None)
        for name in ("CFLAGS", "CXXFLAGS", "CC", "CXX", "AR", "RANLIB"):
            for key in self.env_names(name):
                env.pop(key, None)
        env.update(ambient or {})
        inputs = {
            "VORTEX_CARGO_EXECUTABLE": self.cargo,
            "VORTEX_RUSTC_EXECUTABLE": sys.executable,
            "VORTEX_RUSTUP_TOOLCHAIN": self.rustup_toolchain,
            "VORTEX_RUST_TARGET": self.target,
            "VORTEX_CARGO_TARGET_DIR": self.target_dir,
            "VORTEX_CARGO_PROFILE": "dev",
            "VORTEX_FFI_PACKAGE": "vortex-ffi",
            "VORTEX_CARGO_FFI_ARCHIVE": self.archive,
            "VORTEX_CMAKE_FFI_ARCHIVE": self.staged,
            "VORTEX_FFI_HEADERS": self.header,
            "VORTEX_CMAKE_FFI_INCLUDE_DIR": self.staged.parent / "include",
            "VORTEX_RUSTFLAGS": ";".join(self.rustflags),
            "VORTEX_CFLAGS": ";".join(cflags),
            "VORTEX_CXXFLAGS": ";".join(cxxflags),
            "VORTEX_C_COMPILER": self.tools["CC"],
            "VORTEX_CXX_COMPILER": self.tools["CXX"],
            "VORTEX_C_COMPILER_ARG1": self.compiler_arg1["CC"],
            "VORTEX_CXX_COMPILER_ARG1": self.compiler_arg1["CXX"],
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
        self.assertEqual((self.staged.parent / "include/vortex.h").read_bytes(), self.header.read_bytes())
        recording = json.loads((self.target_dir / "environment.json").read_text())
        self.assertEqual(recording["args"][recording["args"].index("--target") + 1], self.target)
        self.assertEqual(recording["env"]["CARGO_ENCODED_RUSTFLAGS"], "\x1f".join(self.rustflags))
        self.assertEqual(recording["env"].get("RUSTUP_TOOLCHAIN", ""), self.rustup_toolchain)
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

    def native_command(self, env, language):
        value = self.effective_tool(env, language)
        # Match cc-rs: an existing path is one word; wrapper strings use
        # split_whitespace(), not the shell parsing used for CFLAGS/CXXFLAGS.
        return [value] if Path(value).is_file() else value.split()

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
            [*self.native_command(env, language), *arguments, *flags],
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

    def prepare_cc_probe(self):
        if not all(shutil.which(tool) for tool in ("cargo", "rustc", "clang", "clang++")):
            self.skipTest("Cargo, rustc, and Clang are required for real cc-rs probes")
        cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
        if not any((cargo_home / "registry/src").glob("*/cc-1.4.0")):
            self.skipTest("cc 1.4.0 must be cached for the offline probe test")
        # Each invocation starts with fresh cc-rs compiler/flag probe caches.
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
            "    let args: Vec<_> = std::env::args().collect();\n"
            "    let mut build = cc::Build::new();\n"
            "    build.target(&target).host(&target).opt_level(0).debug(false)\n"
            "        .cargo_metadata(false).inherit_rustflags(false)\n"
            '        .cpp(args[1] == "CXX");\n'
            "    let tool = build.try_get_compiler()?;\n"
            "    let clang = tool.is_like_clang();\n"
            "    let mut command = tool.to_command();\n"
            '    println!("program={:?}", command.get_program());\n'
            '    println!("args={:?}", command.get_args().collect::<Vec<_>>());\n'
            "    if args.len() == 4 {\n"
            '        let status = command.args(["-c", &args[2], "-o", &args[3]]).status()?;\n'
            '        if !status.success() { return Err(format!("compiler failed: {status}").into()); }\n'
            "    } else {\n"
            '        let supported = build.is_flag_supported("-fno-omit-frame-pointer")?;\n'
            '        let unsupported = build.is_flag_supported("-fdefinitely-not-a-supported-flag")?;\n'
            '        println!("{{\\"clang\\": {clang}, \\"supported\\": {supported}, '
            '\\"unsupported\\": {unsupported}}}");\n'
            "    }\n"
            "    Ok(())\n}\n",
            encoding="utf-8",
        )
        build_env = rust_toolchain_environment()
        self.rustup_toolchain = build_env.get("RUSTUP_TOOLCHAIN", "")
        for key in ("RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"):
            build_env.pop(key, None)
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
            cwd=REPO_ROOT,
            env=build_env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        version = subprocess.run(
            ["rustc", "-vV"],
            cwd=REPO_ROOT,
            env=build_env,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout
        self.target = next(line.removeprefix("host: ") for line in version.splitlines() if line.startswith("host: "))
        self.archive = self.target_dir / self.target / "debug" / "libvortex_ffi.a"
        for language, name in (("CC", "clang"), ("CXX", "clang++")):
            compiler = shutil.which(name)
            if sys.platform == "darwin":
                # /usr/bin/clang is Apple's dispatch tool, which cannot be renamed.
                compiler = subprocess.run(
                    ["xcrun", "--find", name], capture_output=True, text=True, timeout=30, check=True
                ).stdout.strip()
            path = self.work / f"real {name}'s compiler"
            path.symlink_to(compiler)
            self.tools[language] = str(path)
        return probe / "target/debug/cc-probe"

    def run_cc_probe(self, probe, env, language, *arguments):
        build_env = dict(env, HOST=self.target, TARGET=self.target, OUT_DIR=str(self.work / "probe out"))
        result = subprocess.run(
            [str(probe), language, *map(str, arguments)],
            env=build_env,
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        return result.stdout


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class CargoEnvironmentTests(CargoEnvironmentFixture):
    def test_native_cache_receives_real_compiler_after_host_filter(self):
        for name in ("sccache", "cachepot", "buildcache", "kache"):
            for suffix in ("", ".exe"):
                with self.subTest(wrapper=name + suffix):
                    wrapper = self.recording_cache(name + suffix)
                    self.cache_log.unlink(missing_ok=True)
                    cflags = [*self.cflags, "-fsanitize=address,undefined"]
                    cxxflags = ["-fsanitize=undefined", *self.cxxflags]
                    env = self.run_driver(cflags, cxxflags, {"RUSTC_WRAPPER": wrapper})
                    self.assertEqual(env["RUSTC_WRAPPER"], wrapper)
                    self.assert_native_environment(env, cflags, cxxflags)
                    self.assertEqual(
                        self.cache_calls(),
                        [
                            {
                                "compiler": self.tools[language],
                                "flags": ["-O0", "-c", "source with spaces", "-o", "object file"]
                                + [flag for flag in flags if not host or not flag.startswith("-fsanitize=")],
                            }
                            for language, flags in (("CC", cflags), ("CXX", cxxflags))
                            for host in (False, True)
                        ],
                    )

    def test_no_native_cache_for_absent_or_unknown_rust_wrapper(self):
        for name in (None, "rust-only", "sccache-other", "sccache.exe.bak"):
            with self.subTest(wrapper=name):
                wrapper = self.recording_cache(name or "unused")
                self.cache_log.unlink(missing_ok=True)
                ambient = {"RUSTC_WRAPPER": wrapper} if name else {}
                cflags = [*self.cflags, "-fsanitize=undefined"]
                cxxflags = [*self.cxxflags, "-fsanitize=undefined"]
                env = self.run_driver(cflags, cxxflags, ambient)
                self.assertEqual(env.get("RUSTC_WRAPPER"), ambient.get("RUSTC_WRAPPER"))
                self.assert_native_environment(env, cflags, cxxflags)
                self.assertEqual(self.cache_calls(), [])

    def test_explicit_cc_wrapper_prevents_outer_rust_wrapper_fallback(self):
        wrapper = self.recording_cache()
        env = self.run_driver(
            self.cflags,
            self.cxxflags,
            {"RUSTC_WRAPPER": wrapper, "CC_KNOWN_WRAPPER_CUSTOM": "ambient-wrapper"},
        )
        self.assertEqual(env["RUSTC_WRAPPER"], wrapper)
        self.assertEqual(env["CC_KNOWN_WRAPPER_CUSTOM"], "env")
        directory = self.target_dir / "cmake-native-tools"
        self.assertEqual(env["PATH"].split(os.pathsep)[0], str(directory))
        for language in ("CC", "CXX"):
            with self.subTest(language=language):
                command = self.native_command(env, language)
                self.assertEqual(len(command), 2)
                self.assertEqual(command[0], "env")
                self.assertRegex(command[1], r"^cc-[0-9a-f]{64}$")
                self.assertEqual(shutil.which(command[1], path=env["PATH"]), str(directory / command[1]))
                for target in (self.target, self.target.replace("-", "_")):
                    self.assertEqual(env[f"{language}_{target}"], " ".join(command))

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

    def test_compiler_arguments_reach_probes_and_cache(self):
        required = {
            "CC": ["", "--sysroot=/sdk with spaces", '-DC_LABEL="apostrophe\'s"', "-fsanitize=undefined"],
            "CXX": ["-std=c++20", '-DCXX_LABEL="two words"', "-fsanitize=address", "-DVALUE=-fsanitize=address", ""],
        }
        self.compiler_arg1 = {language: shlex.join(args) for language, args in required.items()}
        wrapper = self.recording_cache()
        for cached in (False, True):
            env = self.run_driver(self.cflags, self.cxxflags, {"RUSTC_WRAPPER": wrapper} if cached else {})
            for language, args in required.items():
                for host in (False, True):
                    with self.subTest(cached=cached, language=language, host=host):
                        self.cache_log.unlink(missing_ok=True)
                        selected = env.copy()
                        if host:
                            selected["CARGO_ENCODED_RUSTFLAGS"] = ""
                        # Mandatory compiler arguments, unlike CFLAGS, also belong in probes.
                        result = subprocess.run(
                            [*self.native_command(selected, language), "-E", "detect_compiler_family.c"],
                            env=selected,
                            capture_output=True,
                            text=True,
                            timeout=30,
                            check=True,
                        )
                        expected = [arg for arg in args if not host or not arg.startswith("-fsanitize=")]
                        expected += ["-E", "detect_compiler_family.c"]
                        self.assertEqual(json.loads(result.stdout), {"tool": self.tools[language], "flags": expected})
                        self.assertEqual(
                            self.cache_calls(),
                            [{"compiler": self.tools[language], "flags": expected}] if cached else [],
                        )

    def test_compiler_argument_changes_invalidate_cc_builds(self):
        self.compiler_arg1 = {"CC": "-DC_REQUIRED=1", "CXX": "-DCXX_REQUIRED=1"}
        original = self.run_driver(self.cflags, self.cxxflags)
        unchanged = self.run_driver(self.cflags, self.cxxflags)
        for language in ("CC", "CXX"):
            self.assertEqual(self.effective_tool(original, language), self.effective_tool(unchanged, language))
        for language in ("CC", "CXX"):
            with self.subTest(language=language):
                self.compiler_arg1[language] += " -DCHANGED=1"
                changed = self.run_driver(self.cflags, self.cxxflags)
                for name in ("CC", "CXX"):
                    self.assertEqual(
                        self.effective_tool(original, name) == self.effective_tool(changed, name), name != language
                    )
                original = changed

    def test_configured_wrappers_prevent_rust_wrapper_fallback(self):
        compilers = self.tools.copy()
        fallback = self.recording_cache()
        for name in ("env", "ccache", "distcc", "sccache", "icecc", "cachepot", "buildcache", "kache", "custom"):
            configured = self.executable(
                f"configured tools' directory/{name}", "import os, sys\nos.execv(sys.argv[1], sys.argv[1:])\n"
            )
            for language in ("CC", "CXX"):
                self.tools[language] = configured
                self.compiler_arg1[language] = shlex.join([compilers[language], "-DREQUIRED=1", "-fsanitize=undefined"])
            env = self.run_driver(
                self.cflags, self.cxxflags, {"RUSTC_WRAPPER": fallback, "CC_KNOWN_WRAPPER_CUSTOM": "custom"}
            )
            for language in ("CC", "CXX"):
                for host in (False, True):
                    with self.subTest(wrapper=name, language=language, host=host):
                        selected = env.copy()
                        if host:
                            selected["CARGO_ENCODED_RUSTFLAGS"] = ""
                        result = subprocess.run(
                            [*self.native_command(selected, language), "-E", "probe.c"],
                            env=selected,
                            capture_output=True,
                            text=True,
                            timeout=30,
                            check=True,
                        )
                        expected = ["-DREQUIRED=1", *([] if host else ["-fsanitize=undefined"]), "-E", "probe.c"]
                        self.assertEqual(json.loads(result.stdout), {"tool": compilers[language], "flags": expected})
            self.assertEqual(self.cache_calls(), [], "An explicit compiler wrapper must suppress the fallback")

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
                    [*self.native_command(env, language), *arguments],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True,
                )
                self.assertEqual(json.loads(result.stdout)["flags"], arguments)

    def test_real_cc_compiler_family_and_flag_support(self):
        probe = self.prepare_cc_probe()
        for name in (None, "sccache", "rust-only"):
            wrapper = self.recording_cache(name or "unused")
            self.cache_log.unlink(missing_ok=True)
            ambient = {"RUSTC_WRAPPER": wrapper} if name else {}
            for language, flag in (("CXX", "-Werror"), ("CC", "-v"), ("CXX", "-v")):
                env = self.run_driver([flag, "-fsanitize=undefined"], [flag, "-fsanitize=undefined"], ambient)
                for host in (False, True):
                    with self.subTest(wrapper=name, language=language, flag=flag, host=host):
                        build_env = env.copy()
                        if host:
                            build_env["CARGO_ENCODED_RUSTFLAGS"] = ""
                        output = self.run_cc_probe(probe, build_env, language)
                        self.assertIn('program="env"', output.splitlines(), output)
                        compiler = self.native_command(env, language)[1]
                        self.assertTrue(
                            any(line.startswith(f'args=["{compiler}"') for line in output.splitlines()), output
                        )
                        self.assertEqual(
                            json.loads(output.splitlines()[-1]),
                            {"clang": True, "supported": True, "unsupported": False},
                            output,
                        )
            calls = self.cache_calls()
            if name == "sccache":
                self.assertTrue(calls)
                for call in calls:
                    self.assertIn(call["compiler"], (self.tools["CC"], self.tools["CXX"]))
            else:
                self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
