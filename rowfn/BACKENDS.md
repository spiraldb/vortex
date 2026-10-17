<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Backends

[Overview](../ROWFN.md) · [Architecture](ARCHITECTURE.md)

Arrow and Vortex have implementations in this checkout. DataFusion and DuckDB are potential
integrations. Solid arrows below show existing invocation paths. Dashed arrows show proposals.

```mermaid
flowchart TB
    A["Arrow caller"] --> RA["rowfn-arrow"]
    V["Vortex registry"] --> RV["VortexRowFn + VortexHost"]
    DF["DataFusion<br/>potential"] -.-> RA
    D["DuckDB<br/>potential"] -.-> DB["Batch adapter<br/>proposed"]
    RA --> R["rowfn"]
    RV --> R
    DB -.-> R
    class A,V,RA,RV host
    class R core
    class DF,D,DB proposed
    classDef core fill:#eff6ff,stroke:#2563eb,color:#1e3a8a
    classDef host fill:#f0fdfa,stroke:#0f766e,color:#134e4a
    classDef proposed fill:#f8fafc,stroke:#64748b,color:#334155,stroke-dasharray:5 4
```

| Backend | Status | Boundary |
| --- | --- | --- |
| [Arrow](../rowfn-arrow/README.md) | Implemented, with latest changes unverified. | Fields, explicit scalar operands, and Arrow arrays. |
| [Vortex](../vortex-array/src/scalar_fn/unstable/rowfn/mod.rs) | Implemented in this modified checkout. | Native arrays, execution context, and existing function registry. |
| DataFusion | Potential. | A UDF wrapper over `rowfn-arrow`. |
| DuckDB | Potential. | Native vector bindings with a batch invocation boundary. |

## DataFusion: reuse Arrow bindings

DataFusion scalar UDFs receive columnar arguments and can return Arrow arrays. A RowFn wrapper could
use that existing boundary and delegate execution to `rowfn-arrow`. See the
[DataFusion UDF interface](https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html).

The wrapper would still own registration, signatures, coercion, scalar markers, output metadata,
and error mapping. Its Arrow version must match the adapter. No wrapper has been implemented.

## DuckDB: bind batches of vectors

DuckDB exposes native vectors through its [data chunk interface](https://duckdb.org/docs/current/clients/c/data_chunk).
A RowFn adapter could retain a chunk, lend typed views, and publish into DuckDB-owned output.

The adapter must define supported vector representations, validity, allocation, errors, and buffer
lifetimes. Any C/Rust boundary must carry a whole batch, rather than call Rust for every row.
Rust traits are not a stable binary ABI. Arrow C Data could exchange arrays, but would not define
function registration or invocation by itself. No DuckDB adapter or ABI has been implemented.

Both integrations must preserve RowFn's strict null contract. Functions that need whole-batch access
or custom null behavior can remain on the host's broader function interface.
