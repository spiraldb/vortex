# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Run real probe fixtures with rustup proxies but no global default toolchain."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import test_cargo_environment


@unittest.skipUnless(
    all(shutil.which(tool) for tool in ("rustup", "cmake", "ninja", "clang", "clang++")),
    "Rustup, CMake, Ninja, and Clang are required",
)
class ProbeToolchainTests(unittest.TestCase):
    def setUp(self):
        cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")).resolve()
        if not any((cargo_home / "registry/src").glob("*/cc-1.4.0")):
            self.skipTest("cc 1.4.0 must be cached for the offline fixtures")
        self.tests = Path(__file__).resolve().parent
        self.repo = self.tests.parents[2]
        temporary = tempfile.TemporaryDirectory(prefix="vortex-probe-toolchain-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name).resolve()
        rustup_home = self.work / "rustup"
        toolchains = rustup_home / "toolchains"
        toolchains.mkdir(parents=True)
        # Link individual installations, not their writable parent or settings.
        # Disable auto-installation so this test can never update these toolchains.
        installed = Path(os.environ.get("RUSTUP_HOME", Path.home() / ".rustup")).resolve() / "toolchains"
        for toolchain in installed.iterdir():
            if toolchain.is_dir():
                (toolchains / toolchain.name).symlink_to(toolchain, target_is_directory=True)
        (rustup_home / "settings.toml").write_text('version = "12"\n', encoding="utf-8")
        proxies = self.work / "bin"
        proxies.mkdir()
        for tool in ("rustup", "cargo", "rustc"):
            (proxies / tool).symlink_to(shutil.which("rustup"))
        self.env = dict(
            os.environ,
            RUSTUP_HOME=str(rustup_home),
            RUSTUP_AUTO_INSTALL="0",
            CARGO_HOME=str(cargo_home),
            CARGO_NET_OFFLINE="true",
            PATH=str(proxies) + os.pathsep + os.environ.get("PATH", ""),
            PYTHONPATH=str(self.tests),
            PYTHONDONTWRITEBYTECODE="1",
        )
        self.env.pop("RUSTUP_TOOLCHAIN", None)
        for tool in ("cargo", "rustc"):
            result = self.run_command([tool, "--version"])
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("no default is configured", result.stdout)
        result = self.run_command(["rustup", "show", "active-toolchain"], cwd=self.repo)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.selected = result.stdout.split()[0]
        self.assertTrue((toolchains / self.selected).is_dir(), result.stdout)
        # A distinct local name proves an inherited selection takes precedence over
        # rust-toolchain.toml without requiring a second installed Rust version.
        self.inherited = "vortex-fixture-inherited"
        (toolchains / self.inherited).symlink_to((toolchains / self.selected).resolve(), target_is_directory=True)

    def run_command(self, command, *, cwd=None, env=None, timeout=30):
        return subprocess.run(
            command,
            cwd=cwd or self.work,
            env=self.env if env is None else env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )

    def check_fixture(self, test):
        for toolchain in (None, self.inherited):
            with self.subTest(toolchain=toolchain):
                env = self.env.copy()
                if toolchain:
                    env["RUSTUP_TOOLCHAIN"] = toolchain
                with mock.patch.dict(os.environ, env, clear=True):
                    self.assertEqual(
                        test_cargo_environment.rust_toolchain_environment()["RUSTUP_TOOLCHAIN"],
                        toolchain or self.selected,
                    )
                    self.assertEqual(dict(os.environ), env, "selection must not mutate the process environment")
                result = self.run_command([sys.executable, "-m", "unittest", "-v", test], env=env, timeout=600)
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertNotIn("skipped", result.stdout)

    def test_cc_probe(self):
        self.check_fixture("test_cargo_environment.CargoEnvironmentTests.test_real_cc_compiler_family_and_flag_support")

    def test_compiler_commands(self):
        self.check_fixture("test_compiler_commands.CompilerCommandTests.test_uncached_compiler_commands")

    @unittest.skipUnless(shutil.which("sccache"), "sccache is required for cached compiler commands")
    def test_sccache_compiler_commands(self):
        self.check_fixture("test_compiler_commands.CompilerCommandTests.test_sccache_compiler_commands")

    @unittest.skipUnless(all(shutil.which(tool) for tool in ("sccache", "nm")), "sccache and nm are required")
    def test_native_cache(self):
        self.check_fixture("test_native_cache.NativeCacheTests.test_host_target_objects_and_rust_cache_hits")


if __name__ == "__main__":
    unittest.main()
