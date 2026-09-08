#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# Downloads one shard of the partitioned ClickBench dataset (~1M rows of the
# `hits` table), converts it to Vortex, and checks that the Parquet and Vortex
# files hold identical data before the Vortex file is used by the SLT suite.

set -ex -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
PARQUET_FILE="${DATA_DIR}/hits.parquet"
VORTEX_FILE="${DATA_DIR}/hits.vortex"
SHARD_URL="https://pub-3ba949c0f0354ac18db1f0f14f0a2c52.r2.dev/clickbench/parquet_many/hits_0.parquet"

mkdir -p "${DATA_DIR}"

# 1. Download the first ClickBench shard.
if [ ! -f "${PARQUET_FILE}" ]; then
  echo "Downloading ClickBench shard 0 to ${PARQUET_FILE}..."
  curl -sSL --fail -o "${PARQUET_FILE}.part" "${SHARD_URL}"
  mv "${PARQUET_FILE}.part" "${PARQUET_FILE}"
fi

# 2. Convert the Parquet file to Vortex.
echo "Converting hits.parquet to Vortex..."
rm -f "${VORTEX_FILE}"
cargo run --release --package vortex-tui --bin vx -- convert "${PARQUET_FILE}"

# 3. Check that the Parquet and Vortex files match. The parity test reads both
#    files through DuckDB and fails if any row differs, so a bad conversion is
#    caught before the query result files are trusted. The Parquet file is kept
#    so the parity test keeps running as part of the regular suite. Set
#    VORTEX_SLT_PROFILE to reuse an existing cargo profile (CI uses `ci`).
echo "Checking that hits.parquet and hits.vortex match..."
(
  cd "${CRATE_DIR}"
  cargo test --profile "${VORTEX_SLT_PROFILE:-release}" -p vortex-sqllogictest --test sqllogictests \
    -- --exact 'slt::duckdb::clickbench/duckdb/parity.slt'
)

echo "Done."
