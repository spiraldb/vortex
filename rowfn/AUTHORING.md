<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Writing a row function

A `RowFn<H>` defines semantic dispatch once. A visitor then selects typed inputs, output storage,
and the callback. The host supplies native metadata, decoding, allocation, validity, and publication.
Start with a fixed signature. Add runtime type dispatch only when the function needs it.

[Architecture](ARCHITECTURE.md) shows the execution path. [Rust interfaces](INTERFACES.md) shows the
actual traits and types.

## Choose a visit

| The function needs | Use |
| --- | --- |
| An owned result. | `visit`. |
| State derived from constants. | `visit_prepared`. |
| A writable row or an immediate error. | `visit_into`. |
| Compact failure evidence checked after traversal. | `visit_deferred`. |
| Direct Boolean packing. | `visit_bool` or `visit_deferred_bool`. |

## Return an owned value

A wrapping addition returns one signed 64-bit value per row. Integer overflow is not a row error.

```rust,ignore
visitor.visit::<(i64, i64), i64>(|(lhs, rhs)| lhs.wrapping_add(rhs))
```

<details>
<summary>Complete WrappingAdd definition</summary>

```rust
use rowfn::{Host, HostResult, InputBinding, OutputBinding, RowFn, RowVisitor};

#[derive(Clone)]
struct WrappingAdd;

impl<H> RowFn<H> for WrappingAdd
where
    H: Host + InputBinding<i64> + OutputBinding<i64>,
{
    type Options = ();
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &(),
        _: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        visitor.visit::<(i64, i64), i64>(|(lhs, rhs)| lhs.wrapping_add(rhs))
    }
}
```

</details>

The framework validates the selected input types. A portable overloaded function also checks its
cross-argument rules in `dispatch`, then selects a concrete typed visitor. Planning calls dispatch
without running its callbacks. Execution repeats dispatch and requires the same output and policy.

## Prepare constants

`visit_prepared` receives explicit scalar values as `Some(value)` and array inputs as `None`.
Preparation runs within the current invocation. It can borrow decoded values, but cannot outlive
their owners. The prepared result cannot retain a borrow obtained from the preparation callback's
arguments. A retry can prepare again.

For a floating-point multiply, dispatch can select this visitor:

```rust,ignore
visitor.visit_prepared::<(f64, f64), f64, Option<f64>>(
    |(_, factor)| factor,
    |constant, (value, factor)| value * constant.unwrap_or(factor),
)
```

Use preparation for work such as an owned searcher or a constant vector's magnitude. Do not depend
on a particular number of callbacks. The executor can fold scalar-only batches and repeat row work
after rejected deferred evidence.

## Return an immediate error safely

`ElementSink` initializes each output slot before traversal and lends an ordinary `&mut T`.
This lets a fallible callback use safe Rust throughout. Default initialization is additional work,
so the low-level `UninitElementSink` remains available when that work matters.

```rust,ignore
visitor.visit_into::<(i64, i64), ElementSink<H, i64>, _>((), |(lhs, rhs), row| {
    *row = lhs.checked_div(rhs).ok_or_else(|| H::error("invalid integer division"))?;
    Ok::<(), H::Error>(())
})
```

<details>
<summary>Complete CheckedDivide definition</summary>

```rust
use rowfn::{Host, HostResult, InputBinding, OutputBinding, RowFn, RowVisitor};
use rowfn::sink::ElementSink;

#[derive(Clone)]
struct CheckedDivide;

impl<H> RowFn<H> for CheckedDivide
where
    H: Host + InputBinding<i64> + OutputBinding<i64>,
{
    type Options = ();
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &(),
        _: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        visitor.visit_into::<(i64, i64), ElementSink<H, i64>, _>((), |(lhs, rhs), row| {
            *row = lhs.checked_div(rhs).ok_or_else(|| H::error("invalid integer division"))?;
            Ok::<(), H::Error>(())
        })
    }
}
```

</details>

Nullable input selects valid-only execution for this fallible visitor. A zero divisor in a null
row cannot become an observable row error. Decode, allocation, and publication failures remain
terminal. The sink can be dropped safely after an error or unwind.

## Accumulate deferred failure evidence

Some cheap operations can return a value and compact evidence before constructing a row error.
The finish callback accepts or rejects the OR-reduced evidence after traversal.

```rust,ignore
visitor.visit_deferred::<(i64, i64), i64, bool>(
    |(lhs, rhs)| lhs.overflowing_add(rhs),
    |failed| {
        if failed {
            Err(H::error("integer overflow"))
        } else {
            Ok(())
        }
    },
)
```

Default evidence must mean success, including for zero rows. Evidence cannot be wider than the
output value. Only rejection by this finish callback permits null-based suppression or retry.
This visitor does not guarantee that a particular checked operation vectorizes on every target.

## Write strings

String sinks start with valid empty rows and copy successful results into output-owned storage.
The callback can borrow its input without making the result depend on that input's lifetime.

```rust,ignore
visitor.visit_into::<(rowfn::Utf8,), H::Sink, ()>((), |(text,), row| {
    row.write(text.trim());
})
```

This visitor requires `H: InputBinding<Utf8> + Utf8Output` and
`for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8`. `write_parts` can concatenate borrowed
parts directly into a host's output arena. `TextBinding` is available when a predicate needs stored
lengths, prefixes, or inline values instead of only `&str`.

`TextBinding::text_layout` validates native text semantics and selects offset or view storage before
traversal. Its `Offset32`, `Offset64`, and `Text` families all provide `TextValue`. A shared binder
selects the matching family, then calls one generic implementation of the row operation.
Binary functions select each operand independently. The companion functions demonstrate this for
trimming, concatenation, comparisons, patterns, substring, and lengths.

The fixed `Utf8` signature remains useful for simple authoring. On Arrow it uses a generic layout
view, so a row read still selects the storage variant. Use concrete text families when removing
that dispatch or reading stored lengths and prefixes matters. This distinction does not change
null propagation, callback restrictions, or output ownership.

## Invoke on Arrow

An `ArrowOperand` includes the array, its complete input field, and an explicit scalar flag.
Supply the logical row count separately. A length-one array does not broadcast unless marked scalar.

```rust,ignore
let fields = operands.iter().map(|input| input.dtype.clone()).collect::<Vec<_>>();
let output = rowfn_arrow::plan(&function, &options, &fields)?;
let result = rowfn_arrow::invoke(&function, &options, &operands, batch_rows, output)?;
```

`result` contains an Arrow array and its planned field. Empty and all-null batches retain that field.
The `rowfn-functions` package supplies a complete Arrow example with checked addition and trimming.
It also demonstrates runtime integer dispatch, prepared list scaling, and a downstream timestamp
domain. Its optional Arrow mapping does not require Vortex.

## Choose the appropriate boundary

All RowFn callbacks obey strict null propagation. Valid inputs cannot produce null. Callbacks cannot
panic or have observable side effects outside their supplied output row. Use a host's broader
scalar-function interface for custom null behavior, bitmap operations, encoding transforms, or
whole-batch work that is simpler than row execution.

Uninitialized scalar and list sinks require unsafe initialization-token constructors. The token
must refer to the entire exact row supplied to the current callback, and initialization must remain
intact until return. A token from another row cannot authorize publication. The
[safety review](SAFETY.md) records the source review and its limits.
