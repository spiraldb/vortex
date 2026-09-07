# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Run CMake integration tests; --ci rejects unexpected skips and empty suites."""

import argparse
import sys
import unittest
from pathlib import Path


class CIResult(unittest.TextTestResult):
    def addSkip(self, test, reason):
        # Match both the whole test ID and its platform-only reason. Missing tools,
        # class setup skips, and subtest skips must never use this exception.
        if (
            sys.platform != "darwin"
            and test.id() == "test_compiler_commands.CompilerCommandTests.test_osx_sysroot_headers_and_cargo_freshness"
            and reason == "Apple SDK fixture requires macOS"
            and not getattr(type(test), "__unittest_skip__", False)
        ):
            super().addSkip(test, reason)
        else:
            error = AssertionError(f"Unexpected skip in CI: {reason}")
            self.addFailure(test, (AssertionError, error, None))


def run_tests(suite, *, ci=False, stream=None):
    stream = stream if stream is not None else sys.stderr
    result = unittest.TextTestRunner(
        verbosity=2, stream=stream, resultclass=CIResult if ci else unittest.TextTestResult
    ).run(suite)
    if result.testsRun == 0:
        print("ERROR: No tests ran; check test discovery.", file=stream)
        return False
    return result.wasSuccessful()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci", action="store_true", help="fail unexpected skips")
    parser.add_argument("tests", nargs="*", help="optional unittest names (default: discover this directory)")
    args = parser.parse_args()
    loader = unittest.TestLoader()
    suite = (
        loader.loadTestsFromNames(args.tests) if args.tests else loader.discover(str(Path(__file__).resolve().parent))
    )
    return 0 if run_tests(suite, ci=args.ci) else 1


if __name__ == "__main__":
    sys.exit(main())
