<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# rowfn-functions

[Overview](../ROWFN.md) · [Author guide](../rowfn/AUTHORING.md)

Shared function definitions for Arrow and Vortex. Each definition keeps its semantic dispatch and
row operation together. These are examples of the framework, not a replacement scalar catalog.

## Choose a host

| Feature | Enables |
| --- | --- |
| Default | The framework and pattern libraries, with no host dependency. |
| `arrow` | Arrow domain mappings, with no Vortex dependency. |
| `vortex` | Mappings for the adapter in this modified Vortex checkout. |

```sh
cargo run --locked -p rowfn-functions --features arrow --example arrow
```

The example performs checked addition with a scalar operand and trims nullable strings into owned
`Utf8View` output. The command has not run against the latest changes.

## Examples

| Definition | What it demonstrates |
| --- | --- |
| `Add<false>`, `Add<true>`, `Multiply` | Runtime integer dispatch, owned output, constants, and deferred errors. |
| `Divide` | Immediate errors and explicit initialization of an uninitialized scalar sink. |
| `PositiveSum` | Direct Boolean packing and nullable dense retry. |
| `Trim`, `Concat` | Borrowed strings and independently owned output. |
| `Scale` | Runtime list width, prepared constants, and initialization of an entire output row. |
| `AdjustTicks` | A downstream row domain that retains timestamp units and timezone metadata. |
| `Seven` | Nullary execution with an explicit logical row count. |
| Comparisons, substring, LIKE, and regex | Storage-aware text, row errors, and invocation-local pattern preparation. |

The package owns its domain traits and `TimestampTicks` row kind. Optional host mappings live here
so downstream implementations satisfy Rust's orphan rules. Concrete text families retain native
offsets or view headers, while function dispatch and callbacks remain shared.

`Divide` and `Scale` use uninitialized sinks with unsafe exact-row tokens. The
[author guide](../rowfn/AUTHORING.md) also shows `ElementSink`, which writes defaults before lending
safe mutable scalar rows.

## Compare or extend

Functions are not registered automatically. [Cross-host fixtures](../rowfn-examples/README.md)
exercise the implementations and external Vortex registration. The [comparison guide](../rowfn/COMPARING.md)
describes semantic differences and measurement boundaries.

This package is unpublished. The [project status](../ROWFN.md#status) distinguishes current source
from recorded test and performance evidence.
