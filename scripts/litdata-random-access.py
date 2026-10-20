#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Converts and times random access into LitData optimized datasets.

LitData (https://github.com/Lightning-AI/litdata) has no Rust binding, so
`random-access-bench` drives it through this script:

- `prepare PARQUET OUTPUT` writes the Parquet file as a LitData optimized dataset, one item
  per row, in Parquet row order so that item `i` is row `i`.
- `bench DATASET --indices ...` times `StreamingDataset` lookups of those rows and prints the
  per-iteration wall times as one JSON object on stdout.

The timed loop mirrors the Rust harness: cached mode warms one reused `StreamingDataset` before
timing, reopen mode constructs a new one inside every timed iteration, and iterations repeat
until the time limit has elapsed. Interpreter start and `import litdata` are not timed.
"""

import argparse
import contextlib
import json
import random
import shutil
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from litdata import StreamingDataset, optimize
from litdata.streaming.serializers import JsonLeaf

# LitData stores a row as a tree of typed leaves and picks each leaf's serializer from the
# first item only, so a null in a column whose first value is a float is silently written as
# 0.0. Columns with nulls, and variable-length lists, are therefore stored as `JsonLeaf`
# values, LitData's own encoding for leaves whose type or shape varies between items.
ENCODE_NATIVE = "native"
ENCODE_JSON = "json"
ENCODE_NUMPY = "numpy"
ENCODE_STRUCT = "struct"

# A plan is one of the encodings above, `(ENCODE_NUMPY, dtype)` or `(ENCODE_STRUCT, field plans)`.
Plan = str | tuple[str, object]

CHUNK_BYTES = "64MB"
VERIFY_SAMPLES = 1_000


def column_plan(field: pa.Field, column: pa.ChunkedArray) -> Plan:
    """Chooses how one Arrow column is stored as a LitData leaf."""
    if column.null_count > 0:
        return ENCODE_JSON
    if pa.types.is_struct(field.type):
        return (
            ENCODE_STRUCT,
            {
                field.type.field(i).name: column_plan(field.type.field(i), struct_field(column, i))
                for i in range(field.type.num_fields)
            },
        )
    if pa.types.is_fixed_size_list(field.type) and is_numeric(field.type.value_type):
        if list_values(column).null_count == 0:
            return (ENCODE_NUMPY, field.type.value_type.to_pandas_dtype())
        return ENCODE_JSON
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type) or pa.types.is_fixed_size_list(field.type):
        return ENCODE_JSON
    return ENCODE_NATIVE


def is_numeric(arrow_type: pa.DataType) -> bool:
    return pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type)


def struct_field(column: pa.ChunkedArray, index: int) -> pa.ChunkedArray:
    return pa.chunked_array([chunk.field(index) for chunk in column.chunks], type=column.type.field(index).type)


def list_values(column: pa.ChunkedArray) -> pa.ChunkedArray:
    return pa.chunked_array([chunk.flatten() for chunk in column.chunks], type=column.type.value_type)


def plain_table(table: pa.Table) -> pa.Table:
    """Casts temporal columns to their integer representation.

    Timestamps and dates come back from `to_pylist` as `datetime` objects, which LitData can only
    pickle. Their physical integers keep every value and use LitData's integer serializer.
    """
    columns = []
    for field, column in zip(table.schema, table.columns):
        if pa.types.is_timestamp(field.type) or pa.types.is_date64(field.type) or pa.types.is_time64(field.type):
            column = column.cast(pa.int64())
        elif pa.types.is_date32(field.type) or pa.types.is_time32(field.type):
            column = column.cast(pa.int32())
        columns.append(column)
    return pa.table(columns, names=table.column_names)


def encode(value: object, plan: Plan) -> object:
    if plan == ENCODE_NATIVE:
        return value
    if plan == ENCODE_JSON:
        return JsonLeaf(value)
    kind, detail = plan
    if kind == ENCODE_NUMPY:
        return np.asarray(value, dtype=detail)
    return {name: encode(value[name], field_plan) for name, field_plan in detail.items()}


def normalize(value: object) -> object:
    """Converts a decoded item to plain Python values for comparison with `to_pylist`."""
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, dict):
        return {name: normalize(field) for name, field in value.items()}
    if isinstance(value, list):
        return [normalize(element) for element in value]
    return value


def write_row_group(task: tuple[str, int, dict[str, Plan]]) -> Iterator[dict[str, object]]:
    """Yields the encoded rows of one Parquet row group, in row order."""
    path, row_group, plan = task
    table = plain_table(pq.ParquetFile(path).read_row_group(row_group))
    for row in table.to_pylist():
        yield {name: encode(row[name], column_plan) for name, column_plan in plan.items()}


def prepare(parquet: Path, output: Path) -> None:
    parquet_file = pq.ParquetFile(parquet)
    table = plain_table(parquet_file.read())
    plan = {field.name: column_plan(field, column) for field, column in zip(table.schema, table.columns)}

    shutil.rmtree(output, ignore_errors=True)
    # A single worker writes the row groups in order, so item `i` of the dataset is row `i`.
    optimize(
        fn=write_row_group,
        inputs=[(str(parquet), row_group, plan) for row_group in range(parquet_file.num_row_groups)],
        output_dir=str(output),
        chunk_bytes=CHUNK_BYTES,
        num_workers=1,
    )

    dataset = StreamingDataset(str(output))
    if len(dataset) != table.num_rows:
        raise RuntimeError(f"LitData dataset has {len(dataset)} items, expected {table.num_rows} rows")
    rng = random.Random(42)
    samples = {0, table.num_rows - 1, *(rng.randrange(table.num_rows) for _ in range(VERIFY_SAMPLES))}
    for index in sorted(samples):
        expected = table.slice(index, 1).to_pylist()[0]
        actual = normalize(dataset[index])
        if actual != expected:
            raise RuntimeError(f"LitData item {index} does not match the Parquet row: {actual!r} != {expected!r}")


def take(dataset: StreamingDataset, indices: list[int]) -> list[object]:
    return [dataset[index] for index in indices]


def bench(path: Path, indices: list[int], time_limit: float, warmup: float, reopen: bool) -> list[int]:
    # Match the Rust harness, which opens the file once before timing in both modes.
    dataset = StreamingDataset(str(path))
    take(dataset, indices)

    if not reopen:
        warmup_start = time.perf_counter()
        while True:
            take(dataset, indices)
            if time.perf_counter() - warmup_start >= warmup:
                break

    runs_ns = []
    overall_start = time.perf_counter()
    while True:
        start = time.perf_counter_ns()
        if reopen:
            rows = take(StreamingDataset(str(path)), indices)
        else:
            rows = take(dataset, indices)
        runs_ns.append(time.perf_counter_ns() - start)
        del rows

        if time.perf_counter() - overall_start >= time_limit:
            break
    return runs_ns


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    prepare_parser = commands.add_parser("prepare", help="convert a Parquet file into a LitData dataset")
    prepare_parser.add_argument("parquet", type=Path)
    prepare_parser.add_argument("output", type=Path)

    bench_parser = commands.add_parser("bench", help="time random access into a LitData dataset")
    bench_parser.add_argument("dataset", type=Path)
    bench_parser.add_argument("--indices", required=True, help="comma-separated row indices")
    bench_parser.add_argument("--time-limit-secs", type=float, required=True)
    bench_parser.add_argument("--warmup-secs", type=float, required=True)
    bench_parser.add_argument("--reopen", action="store_true", help="open the dataset in every iteration")

    args = parser.parse_args()
    # LitData logs progress on stdout, which carries only the JSON report.
    with contextlib.redirect_stdout(sys.stderr):
        if args.command == "prepare":
            prepare(args.parquet, args.output)
            return 0
        indices = [int(index) for index in args.indices.split(",")]
        runs_ns = bench(args.dataset, indices, args.time_limit_secs, args.warmup_secs, args.reopen)
    json.dump({"runs_ns": runs_ns}, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
