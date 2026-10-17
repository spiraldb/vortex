<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# rowfn-arrow

[Overview](../ROWFN.md) · [Rust interfaces](../rowfn/INTERFACES.md)

`rowfn-arrow` runs shared row functions directly on Arrow arrays. It owns decoding, validity,
allocation, and output fields. It has no Vortex dependency, including through its tests.

## Invoke a function

Each `ArrowOperand` supplies an array, its `Field`, and an explicit `scalar` flag. Arrays must match
the logical batch length. Scalars must contain exactly one value, even for a zero-row batch.
A length-one array does not become a scalar automatically.

```rust,ignore
let output = rowfn_arrow::plan(&function, &options, &input_fields)?;
let result = rowfn_arrow::invoke(&function, &options, &operands, batch_rows, output)?;
```

The result contains an array and its planned field. Empty and all-null batches preserve that field,
including nullability and semantic metadata. The [complete Arrow example](../rowfn-functions/examples/arrow.rs)
shows checked addition and trimming.

## Storage support

| Input | Binding |
| --- | --- |
| 8–64-bit integers and `Float32` / `Float64`. | Retains `ScalarBuffer` and borrows typed slices. |
| `Boolean`. | Retains bitmap storage and slice offsets. |
| `Utf8`, `LargeUtf8`, `Utf8View`. | Borrows offsets or views, retaining lengths and prefixes. |
| Primitive fixed-size lists. | Borrows children and retains width. Nullable children are rejected. |
| Primitive or string dictionaries. | Materializes referenced logical rows through `take`. |
| Timestamp ticks. | Uses the external domain mapping and retains units and timezone. |

Ordinary bindings reject unknown extensions. A dedicated domain binding must establish additional
semantics. `TextBinding` chooses concrete storage before traversal. The fixed `Utf8` binding also
supports all three layouts through a generic view.

Dictionary decoding preserves null keys and null values. Unused entries are not evaluated. Zero-width
lists retain an explicit row count. Timestamp adjustment changes ticks only, without unit, calendar,
or timezone conversion.

## Output ownership

Primitive publication retains the payload allocation. Dense Boolean output packs directly into
Arrow-owned storage. Strings copy into owned `Utf8View` storage, so results outlive their inputs.
View-size limits and publication failures are terminal adapter errors.

<details>
<summary>Validation and unchecked publication</summary>

The string writer checks view limits and constructs valid UTF-8 from string parts. Its private state
retains every referenced buffer, so finishing does not repeat UTF-8 validation. Publication checks
storage compatibility, row count, validity length, and planned nullability before changing metadata.
Only the same storage type or the supported Int64-to-Timestamp label is accepted.

Arrow guarantees valid string payloads at null positions. Dense reads use that guarantee, while
result validity follows RowFn's strict contract. This does not authorize unchecked Vortex decoding,
whose adapter retains validation and null sanitation.

</details>

## Evidence

Arrow-only tests live in this package. Cross-host tests and benchmarks live in
[`rowfn-examples`](../rowfn-examples/README.md). The adapter is unpublished and uses locked Arrow
59.3.0. The [comparison guide](../rowfn/COMPARING.md) explains baseline qualifications, and the
[project status](../ROWFN.md#status) identifies the latest unverified changes.
