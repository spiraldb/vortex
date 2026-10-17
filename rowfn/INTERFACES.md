<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Rust interfaces

[Overview](../ROWFN.md) · [Architecture](ARCHITECTURE.md)

These diagrams use the actual API names. Generic arguments are shown where they help. Only selected
methods are listed. Source links contain the full contracts.

## 1. Select typed work

```mermaid
flowchart TB
    F["RowFn&lt;H&gt;<br/>dispatch(options, args, visitor)"] --> V["RowVisitor&lt;H&gt;<br/>visit · visit_into · visit_deferred"]
    V -. implemented by .-> P["Planner<br/>private"]
    V -. implemented by .-> E["Execute<br/>private"]
    P --> Plan["Plan&lt;H&gt;<br/>output type and policy"]
    E --> Outcome["Outcome&lt;H&gt;<br/>Values · Deferred · Unsupported"]
    class F function
    class V,P,E core
    class Plan,Outcome data
    classDef function fill:#fff7ed,stroke:#c2410c,color:#431407
    classDef core fill:#eff6ff,stroke:#2563eb,color:#1e3a8a
    classDef host fill:#f0fdfa,stroke:#0f766e,color:#134e4a
    classDef data fill:#f8fafc,stroke:#64748b,color:#334155
```

[`RowFn` and `RowVisitor`](src/visitor.rs) are public traits. [`Planner`](src/plan.rs) returns a
`Plan<H>` without running callbacks. [`Execute`](src/execute/visitor.rs) returns an internal
`Outcome<H>`. The public `execute` function checks the plan and ultimately returns `H::Column`.

<details>
<summary>Dispatch and owned-output signatures</summary>

```rust,ignore
// Selected members of RowFn<H> and RowVisitor<H>.
fn dispatch<V: RowVisitor<H>>(
    &self,
    options: &Self::Options,
    args: &[H::NativeType],
    visitor: V,
) -> HostResult<H, V::VisitResult>;

fn visit<Args, Out>(
    self,
    apply: impl Fn(Args::Elems<'_>) -> Out,
) -> HostResult<H, Self::VisitResult>
where
    Args: InputTuple<H>,
    Out: Default + 'static,
    H: OutputBinding<Out>;
```

</details>

## 2. Borrow typed input

```mermaid
flowchart TB
    K["RowKind<br/>Value&lt;'a&gt;"] -. selects the value family .-> I["InputBinding&lt;K&gt;: Host<br/>Decoded · View&lt;'a&gt;"]
    I -->|decode| D["Decoded<br/>retained storage owner"]
    D -->|borrow| V["RowView&lt;'a, K&gt;<br/>len · get_unchecked"]
    class K,D data
    class I host
    class V core
    classDef function fill:#fff7ed,stroke:#c2410c,color:#431407
    classDef core fill:#eff6ff,stroke:#2563eb,color:#1e3a8a
    classDef host fill:#f0fdfa,stroke:#0f766e,color:#134e4a
    classDef data fill:#f8fafc,stroke:#64748b,color:#334155
```

[`InputBinding`](src/input.rs) retains the decoded owner and lends a `RowView`. The view returns
`K::Value<'a>` and cannot outlive its owner. `i64` lends a value, `Utf8` lends `&str`, and
`FixedSizeList<f64>` lends `&[f64]`. A row kind does not describe the native array layout.

`RowView` is unsafe to implement. Every in-range read must return a valid value, including reads at
null slots after decoding. Bounds and ownership requirements are in its source contract.

## 3. Allocate and finish output

```mermaid
flowchart LR
    O["OutputBinding&lt;T&gt;: Host<br/>allocate · build_from"] -->|allocate| B["OutputBuffer&lt;T&gt;<br/>slots · finish"]
    B -->|initialized prefix| C["H::Column<br/>native array"]
    class O host
    class B core
    class C data
    classDef function fill:#fff7ed,stroke:#c2410c,color:#431407
    classDef core fill:#eff6ff,stroke:#2563eb,color:#1e3a8a
    classDef host fill:#f0fdfa,stroke:#0f766e,color:#134e4a
    classDef data fill:#f8fafc,stroke:#64748b,color:#334155
```

[`OutputBinding`](src/output.rs) chooses storage whose `Finished` type is `H::Column`.
`OutputBuffer` is unsafe to implement. Slot contents must remain stable, partial initialization must
be safe to abandon, and `finish` may read only the initialized prefix.

<details>
<summary>Borrowed views and finished-output associated types</summary>

```rust,ignore
trait RowKind: 'static {
    type Value<'a>;
}

// Selected members of InputBinding<K>.
trait InputBinding<K: RowKind>: Host {
    type Decoded: 'static;
    type View<'a>: RowView<'a, K>;

    fn view(decoded: &Self::Decoded) -> Self::View<'_>;
}

// Selected members of OutputBinding<T>.
trait OutputBinding<T: Default + 'static>: Host {
    type Buffer: OutputBuffer<T, Finished = Self::Column> + 'static;
}
```

</details>

## Concrete hosts

[`Host`](src/host.rs) supplies these associated types. Binding traits provide the operations.

| Type | [`ArrowHost`](../rowfn-arrow/src/lib.rs) | [`VortexHost`](../vortex-array/src/scalar_fn/unstable/rowfn/mod.rs) |
| --- | --- | --- |
| `Column` | Arrow `ArrayRef`. | Vortex `ArrayRef`. |
| `NativeType` | `Field`. | `DType`. |
| `Context` | `()`. | `ExecutionCtx`. |
| `Error` | `ArrowError`. | `VortexError`. |

The `i64` bindings retain different storage and lend the same Rust input view:

| Binding | [Arrow](../rowfn-arrow/src/input.rs) | [Vortex](../vortex-array/src/scalar_fn/unstable/rowfn/input.rs) |
| --- | --- | --- |
| `Decoded` | `ScalarBuffer<i64>`. | `Buffer<i64>`. |
| `View<'a>` | `&'a [i64]`. | `&'a [i64]`. |
| Output `Buffer` | [`PrimitiveOutput<i64>`](../rowfn-arrow/src/output.rs). | [`VortexBuffer<i64>`](../vortex-array/src/scalar_fn/unstable/rowfn/output.rs). |

Other interfaces have focused jobs: [`TypeBinding` and `BatchBinding`](src/host.rs) handle metadata
and batch operations, [`OutputSink`](src/sink/mod.rs) lends writable rows, and
[`BooleanOutput`](src/output.rs) packs Boolean results. See the [adapter guide](ADAPTERS.md) for
those contracts and the [safety record](SAFETY.md) for their invariants.
