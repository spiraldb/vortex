# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Keep offline cc-rs fixtures aligned with the repository's lockfile."""

import os
import tomllib
import unittest
from pathlib import Path

LOCKFILE = Path(__file__).resolve().parents[3] / "Cargo.lock"


def locked_cc_version():
    with LOCKFILE.open("rb") as file:
        packages = [package for package in tomllib.load(file).get("package", []) if package.get("name") == "cc"]
    if len(packages) != 1:
        raise ValueError(f"Expected exactly one cc package in {LOCKFILE}, found {len(packages)}: {packages}")
    package = packages[0]
    version = package.get("version")
    if not isinstance(version, str) or not version:
        raise ValueError(f"Missing cc version in {LOCKFILE}: {package}")
    if package.get("source") != "registry+https://github.com/rust-lang/crates.io-index":
        raise ValueError(f"Offline fixtures require a crates.io cc package in {LOCKFILE}: {package}")
    return version


def cached_cc_version():
    version = locked_cc_version()
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    if not any((cargo_home / "registry/src").glob(f"*/cc-{version}/Cargo.toml")):
        raise unittest.SkipTest(f"cc {version} from {LOCKFILE} must be cached; run cargo fetch --locked")
    return version
