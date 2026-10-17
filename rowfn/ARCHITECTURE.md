<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Architecture

[Overview](../ROWFN.md) · [Rust interfaces](INTERFACES.md)

A function selects typed work through `RowVisitor`. Planning checks that selection. Execution runs
it through the host bindings. Types describe the values and metadata passed across those interfaces.

## The components

```mermaid
flowchart TB
    subgraph Types["Types"]
        K["RowKind::Value<br/>Rust callback inputs"]
        M["H::NativeType<br/>Arrow Field or Vortex DType"]
    end
    subgraph Definition["Function and visitor"]
        F["RowFn::dispatch"] -->|selects a visit| V["RowVisitor"]
    end
    subgraph Framework["Planning and execution"]
        P["Planner<br/>validates the signature"] -->|Plan| E["Execute<br/>runs the callbacks"]
    end
    K --> V
    M --> F
    V -. planning visitor .-> P
    V -. execution visitor .-> E
    E --> H["Host binding traits<br/>decode, allocate, publish"]
    E --> L["rowfn-kernels<br/>traverse and pack"]
    class K,M data
    class F function
    class V,P,E,L core
    class H host
    classDef function fill:#fff7ed,stroke:#c2410c,color:#431407
    classDef core fill:#eff6ff,stroke:#2563eb,color:#1e3a8a
    classDef host fill:#f0fdfa,stroke:#0f766e,color:#134e4a
    classDef data fill:#f8fafc,stroke:#64748b,color:#334155
```

`Planner` and `Execute` are private implementations of `RowVisitor`. The diagram separates their
responsibilities, rather than listing every call. Both use host capabilities. Planning does not
decode inputs or run row callbacks.

| Part | Owns |
| --- | --- |
| `RowKind` | The Rust input value family, such as `i64`, `&str`, or `&[f64]`. |
| `RowFn` | Semantic dispatch, preparation callbacks, and row operations. |
| `RowVisitor` | The typed interface for owned, sink, prepared, deferred, and Boolean visits. |
| `Planner` / `Plan` | Signature validation, output metadata, and execution policy. |
| `Execute` | Constant handling, traversal, nulls, and eligible retries. |
| Host bindings | Native types, decoded owners, allocation, validity, and publication. |

## One successful batch

```mermaid
flowchart LR
    P["plan"] --> E["execute"]
    E --> D["Decode"]
    D --> R["Prepare and run"]
    R --> O["Publish"]
    class P,E,R core
    class D,O host
    classDef function fill:#fff7ed,stroke:#c2410c,color:#431407
    classDef core fill:#eff6ff,stroke:#2563eb,color:#1e3a8a
    classDef host fill:#f0fdfa,stroke:#0f766e,color:#134e4a
    classDef data fill:#f8fafc,stroke:#64748b,color:#334155
```

Execution repeats the function's dispatch with an execution visitor. Decoded owners remain alive
through preparation and traversal. Concrete text bindings select physical storage before the loop.
Empty, all-null, and retry paths can take different routes.

Only rejected deferred row evidence can trigger a null-based retry. Decoder and allocation failures
remain terminal. Preparation belongs to the invocation and can run again during a retry.

## Source map

| Location | Contains |
| --- | --- |
| [`rowfn-functions`](../rowfn-functions/README.md) | Shared definitions and optional domain mappings. |
| [`rowfn`](README.md) | Types, visitors, planning, execution, and sinks. |
| [`rowfn-kernels`](../rowfn-kernels/README.md) | Lane sources, borrowed bitmaps, and Boolean packing. |
| [`rowfn-arrow`](../rowfn-arrow/README.md) | Arrow bindings and invocation. |
| [Vortex adapter](../vortex-array/src/scalar_fn/unstable/rowfn/mod.rs) | Vortex bindings and the existing registry wrapper. |
| [`rowfn-examples`](../rowfn-examples/README.md) | Fixtures, registration examples, and benchmarks. |

The framework and kernels have no host dependency. Arrow bindings have no Vortex dependency.
Registration and whole-batch functions remain host concerns.

[Backends](BACKENDS.md) separates the current adapters from potential DataFusion and DuckDB paths.
