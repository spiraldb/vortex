#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# Download the parquet files the leaf corpus is extracted from.
set -euo pipefail
out="${1:?usage: fetch_data.sh <dir>}"
mkdir -p "$out"
for i in 0 1; do
  curl -sSfo "$out/hits_$i.parquet" "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_$i.parquet"
done
curl -sSfo "$out/taxi.parquet" "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
