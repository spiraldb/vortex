# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Small regressions for lockfile selection and the CI runner's skip policy."""

import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import cc_fixture
import run_tests


class SupportTests(unittest.TestCase):
    def test_lockfile_selection_and_cache(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lockfile = root / "Cargo.lock"
            package = '[[package]]\nname = "cc"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n'
            with mock.patch.object(cc_fixture, "LOCKFILE", lockfile), mock.patch.dict(os.environ, CARGO_HOME=temporary):
                for version in ("1.2.3", "1.2.4"):
                    with self.subTest(version=version):
                        lockfile.write_text(package + f'version = "{version}"\n', encoding="utf-8")
                        self.assertEqual(cc_fixture.locked_cc_version(), version)
                        with self.assertRaisesRegex(unittest.SkipTest, f"cc {version} .*cargo fetch --locked"):
                            cc_fixture.cached_cc_version()
                        cached = root / "registry/src/fixture" / f"cc-{version}"
                        cached.mkdir(parents=True)
                        (cached / "Cargo.toml").touch()
                        self.assertEqual(cc_fixture.cached_cc_version(), version)
                for contents, message in (
                    ("version = 4\n", "exactly one cc package"),
                    (package, "Missing cc version"),
                    (package + 'version = "1.2.3"\n' + package + 'version = "1.2.4"\n', "exactly one cc package"),
                ):
                    with self.subTest(contents=contents):
                        lockfile.write_text(contents, encoding="utf-8")
                        with self.assertRaisesRegex(ValueError, message):
                            cc_fixture.cached_cc_version()

    def test_ci_skip_policy(self):
        sdk_id = "test_compiler_commands.CompilerCommandTests.test_osx_sysroot_headers_and_cargo_freshness"
        sdk_reason = "Apple SDK fixture requires macOS"
        for platform, test_id, reason, allowed in (
            ("linux", "dummy.test_skip", "sccache is required", False),
            ("linux", sdk_id, sdk_reason, True),
            ("darwin", sdk_id, sdk_reason, False),
            ("linux", sdk_id, "xcrun is required for the Apple SDK fixture", False),
            ("linux", "dummy.test_skip", sdk_reason, False),
        ):
            for kind in ("test", "class", "setUpClass", "subtest"):
                with self.subTest(platform=platform, test_id=test_id, reason=reason, kind=kind):

                    @unittest.skipIf(kind == "class", reason)
                    class Dummy(unittest.TestCase):
                        @classmethod
                        def setUpClass(cls):
                            if kind == "setUpClass":
                                raise unittest.SkipTest(reason)

                        def test_skip(self):
                            if kind == "subtest":
                                with self.subTest(part=1):
                                    self.skipTest(reason)
                            else:
                                self.skipTest(reason)

                        def id(self):
                            return test_id

                    output = io.StringIO()
                    with mock.patch.object(run_tests.sys, "platform", platform):
                        success = run_tests.run_tests(
                            unittest.defaultTestLoader.loadTestsFromTestCase(Dummy), ci=True, stream=output
                        )
                    expected = allowed and kind == "test"
                    self.assertEqual(success, expected, output.getvalue())
                    if not expected:
                        self.assertIn("Unexpected skip in CI", output.getvalue())

    def test_empty_suite_fails_even_without_ci(self):
        for ci in (False, True):
            with self.subTest(ci=ci):
                output = io.StringIO()
                self.assertFalse(run_tests.run_tests(unittest.TestSuite(), ci=ci, stream=output))
                self.assertIn("No tests ran", output.getvalue())


if __name__ == "__main__":
    unittest.main()
