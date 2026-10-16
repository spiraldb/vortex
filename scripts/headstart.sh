#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# Run cargo with headstart (https://github.com/PowderworksCode/headstart), an experimental
# patched rustc and cargo that start dependent crates as soon as a dependency's interface is
# checked, instead of waiting for its whole metadata.
#
#   scripts/headstart.sh setup               # once: fetch and build the patched toolchain
#   scripts/headstart.sh check -p vortex     # any cargo command, with headstart on
#   HEADSTART=0 scripts/headstart.sh check   # same toolchain, headstart off (a fair baseline)
#
# The patched rustc is a dev build newer than rust-toolchain.toml, so its artifacts go to their
# own target directory (target/headstart unless CARGO_TARGET_DIR is set).
set -euo pipefail

HEADSTART_REPO=https://github.com/PowderworksCode/headstart
HEADSTART_REV=ff81bfecda2116d54776a7fec538fc2f4e349d1f
HEADSTART_DIR=${HEADSTART_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/vortex-headstart}

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
rustc_bin=$HEADSTART_DIR/rustc/build/host/stage1/bin/rustc
cargo_bin=$HEADSTART_DIR/cargo/target/release/cargo

if [ "${1:-}" = setup ]; then
  if [ ! -d "$HEADSTART_DIR/.git" ]; then
    git clone "$HEADSTART_REPO" "$HEADSTART_DIR"
  fi
  git -C "$HEADSTART_DIR" fetch --quiet origin "$HEADSTART_REV"
  git -C "$HEADSTART_DIR" checkout --quiet "$HEADSTART_REV"
  # Builds a stage 1 rustc from source: allow about an hour on 4 cores and 20 GB of disk.
  "$HEADSTART_DIR/scripts/setup.sh"
  exit
fi

if [ $# -eq 0 ]; then
  echo "usage: $0 setup | <cargo arguments>" >&2
  exit 2
fi
if [ ! -x "$rustc_bin" ] || [ ! -x "$cargo_bin" ]; then
  echo "headstart toolchain not found in $HEADSTART_DIR; run: $0 setup" >&2
  exit 1
fi

# A rustc wrapper that buffers output until rustc exits (sccache does) hides the early-metadata
# notification, so nothing would start early.
unset RUSTC_WRAPPER
export RUSTC=$rustc_bin
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$repo_root/target/headstart}
if [ "${HEADSTART:-1}" = 0 ]; then
  export CARGO_UNSTABLE_HEADSTART=false
else
  export CARGO_UNSTABLE_HEADSTART=true
fi
exec "$cargo_bin" "$@"
