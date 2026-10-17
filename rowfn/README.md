<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# rowfn

`rowfn` is an experimental framework for typed row functions on columnar arrays. Functions select
inputs, outputs, and callbacks through a visitor. The framework executes those callbacks through
host bindings, so Arrow and Vortex keep their own storage and allocation.

The [component diagram](ARCHITECTURE.md) separates types, visitors, planning, execution, and bindings.
The [Rust interface diagrams](INTERFACES.md) show their traits, associated types, and signatures.
The [project overview](../ROWFN.md) introduces the experiment for Arrow maintainers.

## Function authors

Inside `RowFn::dispatch`, a wrapping addition selects signed 64-bit inputs and output:

```rust,ignore
visitor.visit::<(i64, i64), i64>(|(lhs, rhs)| lhs.wrapping_add(rhs))
```

The visitor uses that signature to validate a plan or execute a batch. Runtime overload selection
also belongs in `dispatch`. The [author guide](AUTHORING.md) contains complete definitions for owned
output, preparation, errors, and sinks.

Every function follows the same contract:

- Null inputs imply null output. Valid inputs cannot produce null.
- Callbacks cannot panic or have observable side effects outside their supplied output row.
- Constants and retries can change callback counts. Preparation belongs to one invocation.
- Decoder, allocation, validation, and publication errors remain terminal.

`ElementSink` supplies safe initialized scalar rows. Uninitialized scalar and list sinks require
unsafe evidence for the exact callback row. Those [initialization contracts](SAFETY.md) still apply
when a function returns an error.

## Find the interface

| Need | Read |
| --- | --- |
| Understand the components. | [Architecture](ARCHITECTURE.md). |
| Inspect the actual API. | [Rust interfaces](INTERFACES.md). |
| Explore host integrations. | [Current and potential backends](BACKENDS.md). |
| Write a function. | [Author guide](AUTHORING.md). |
| Bind a host. | [Adapter guide](ADAPTERS.md). |
| Compare a native kernel. | [Comparison guide](COMPARING.md). |
| Review unsafe invariants. | [Safety record](SAFETY.md). |

`rowfn` depends on `rowfn-kernels`, with no Arrow or Vortex dependency. Registration, serialization,
optimizer rules, and whole-batch functions remain host concerns. The Rust interfaces do not define
a stable binary ABI.

This package is unpublished. Historical measurements describe earlier source. The package split,
initialized scalar sink, concrete text bindings, and rename have source review only.
