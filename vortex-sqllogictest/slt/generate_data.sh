#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# Generates the git-ignored fixtures used by the generated-data SLT suites.
#
# Usage: generate_data.sh [DATASET...]
#
# Datasets:
#   tpch        TPC-H at scale factor 0.1, written to slt/tpch/data/.
#   clickbench  One shard (~1M rows) of the partitioned ClickBench `hits` table,
#               written to slt/clickbench/data/ and checked for Parquet/Vortex
#               parity after conversion.
#
# With no arguments every dataset is generated. Each dataset is produced as
# Parquet and then converted to Vortex with `vx convert`, so both formats are
# available to the suites. Set VORTEX_SLT_PROFILE to reuse an existing cargo
# profile for the parity test (CI uses `ci`).

set -e -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ALL_DATASETS=(tpch clickbench)

CLICKBENCH_SHARD_URL="https://pub-3ba949c0f0354ac18db1f0f14f0a2c52.r2.dev/clickbench/parquet_many/hits_0.parquet"

usage() {
  echo "Usage: $(basename "${BASH_SOURCE[0]}") [DATASET...]"
  echo
  echo "Datasets: ${ALL_DATASETS[*]} (default: all)"
}

convert_to_vortex() {
  local parquet_file="$1"
  echo "Converting $(basename "${parquet_file}") to Vortex..."
  rm -f "${parquet_file%.parquet}.vortex"
  (cd "${CRATE_DIR}" && cargo run --release --package vortex-tui --bin vx -- convert "${parquet_file}")
}

generate_tpch() {
  local data_dir="${SCRIPT_DIR}/tpch/data"

  if ! command -v uvx &> /dev/null; then
    echo "Error: uvx not found. Install uv first: https://docs.astral.sh/uv/" >&2
    exit 1
  fi

  mkdir -p "${data_dir}"

  echo "Generating TPC-H data (SF=0.1)..."
  uvx tpchgen-cli -s 0.1 --format=parquet --output-dir "${data_dir}/"

  for f in "${data_dir}"/*.parquet; do
    convert_to_vortex "$f"
  done
}

generate_clickbench() {
  local data_dir="${SCRIPT_DIR}/clickbench/data"
  local parquet_file="${data_dir}/hits.parquet"

  mkdir -p "${data_dir}"

  if [ ! -f "${parquet_file}" ]; then
    echo "Downloading ClickBench shard 0 to ${parquet_file}..."
    curl -sSL --fail -o "${parquet_file}.part" "${CLICKBENCH_SHARD_URL}"
    mv "${parquet_file}.part" "${parquet_file}"
  fi

  convert_to_vortex "${parquet_file}"

  # The parity test reads both files through DuckDB and fails if any row
  # differs, so a bad conversion is caught before the query result files are
  # trusted. The Parquet file is kept so the parity test keeps running as part
  # of the regular suite.
  echo "Checking that hits.parquet and hits.vortex match..."
  (
    cd "${CRATE_DIR}"
    cargo test --profile "${VORTEX_SLT_PROFILE:-release}" -p vortex-sqllogictest --test sqllogictests \
      -- --exact 'slt::duckdb::clickbench/duckdb/parity.slt'
  )
}

datasets=()
for arg in "$@"; do
  case "${arg}" in
    -h|--help)
      usage
      exit 0
      ;;
    tpch|clickbench)
      datasets+=("${arg}")
      ;;
    *)
      echo "Error: unknown dataset '${arg}'" >&2
      usage >&2
      exit 1
      ;;
  esac
done
if [ "${#datasets[@]}" -eq 0 ]; then
  datasets=("${ALL_DATASETS[@]}")
fi

for dataset in "${datasets[@]}"; do
  echo "==> ${dataset}"
  "generate_${dataset}"
done

echo "Done."
