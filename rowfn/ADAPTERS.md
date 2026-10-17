<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Binding a host

A host keeps its native arrays, runtime types, allocator, and errors. Implement focused binding
traits on a local host marker. Row loops use typed views and do not call the registry or inspect
native array encodings per row.

[Architecture](ARCHITECTURE.md) shows the execution path. [Rust interfaces](INTERFACES.md) shows the
actual traits and types.

| Capability | Responsibility |
| --- | --- |
| `Host` | Associated column, native type, context, and error types. |
| `TypeBinding` | Outer nullability and value-preserving output labels. |
| `InputBinding<K>` | Semantic validation, decoded owners, and stable typed views. |
| `OutputBinding<T>` | Output metadata, allocation, and bulk collection. |
| `OutputBuffer<T>` | Stable slots, safe partial abandonment, and initialized-prefix publication. |
| `OutputSink<H>` | Row identity, initialization, and host-native finalization. |
| `BooleanOutput` | Packed Boolean collection into host-owned storage. |
| `BatchBinding` | Validity, selection, filtering, broadcasting, and publication. |

## Input: validate, then borrow

A row kind defines a value family such as `i64`, `&str`, or `&[f64]`. Its host binding validates
native semantics before decoding. Matching storage width is insufficient: unknown extensions must
not enter ordinary primitive bindings. Keep nested nullability, list shape, timestamp units, and
timezone metadata distinct from outer nullability.

The decoded owner retains every buffer used by a borrowed view. A successful `RowView` must return
valid row values throughout its stable index domain. If ordinary decoding cannot establish that
for null payloads, decline null-tolerant decoding so the executor filters first, or decode safe
placeholders. Never expose a partly validated view and rely on callers to avoid its unsafe rows.

`DENSE_SAFE` governs whether ordinary nullable decoding is allowed. `DECODE_INFALLIBLE` describes
semantic decoder fallibility on legal inputs. Neither flag turns a decoder error into deferred row
evidence. Resource, validation, and other adapter failures remain terminal.

Text hosts can implement `TextBinding` to select physical storage during semantic dispatch.
Its three associated row families provide `TextValue`. Each concrete view borrows matching offset,
header, and byte slices once. Length operations can read metadata without constructing a string.
`text_layout` rejects unknown semantic mappings before a function selects its typed visitor.
Vortex maps all three families to its existing view representation. Arrow supplies separate
32-bit offsets, 64-bit offsets, and view headers. A host need not convert storage into another layout.

## Output: allocate, then publish

`OutputBuffer` is an unsafe contract. Its slot count, mapping, and initialized contents stay stable
across views and moves. Partial initialization can be abandoned safely. `finish` reads only the
initialized prefix that its caller establishes. A primitive host can reuse the allocation directly.
No universal owned-buffer abstraction is required.

`OutputSink` retains stable row identity across borrowed views. Its initializer makes every skipped
row safe to publish. A consuming string writer owns or retains all output bytes and cannot invalidate
another row. An uninitialized writer needs evidence tied to its exact supplied callback row.
`ElementSink` handles scalar defaults in the framework while retaining the host's output buffer.

`BooleanOutput` is also an unsafe contract because its callback can perform unchecked input reads.
Invoke that callback exactly once for each in-range index in order, with no calls for zero rows.
Keep allocation and publication under host resources.

## Batches: keep shape and validity

Validate operand lengths, fields, and scalar markers before dispatch. Array operands cover the
logical row count. Scalar operands contain one row, including when the logical count is zero.
Preserve output semantics and metadata when no valid callback runs.

Validity can remain lazy during successful dense execution. Resolve a selection only when required.
`Selection` implementations retain a stable length and count, visit unique bounded indices in
increasing order, and stop immediately on the first callback error. Filtering preserves scalar
markers and semantic metadata, then scatter traversal writes into the original output row domain.

## Custom domains and registration

Implement primitive bindings on the adapter's local marker, with bounds limited to supported
primitives. Avoid blanket implementations that occupy bindings for downstream custom row kinds.
A downstream package can own a custom row kind and implement its binding for existing host markers.

A semantic capability implemented for a foreign host must belong to the implementing package,
unless another local type makes the implementation legal. `rowfn-functions` owns its domain traits
and timestamp kind, then supplies optional mappings for Arrow and Vortex in that same package.
Its default dependency graph has no host dependencies. This arrangement avoids host wrappers and
mandatory function catalogs in the adapters.

Registration, IDs, serialization, optimizer rules, and statistics rules belong to the host or
function package. Vortex's `VortexRowFn<F>` uses the existing scalar registry. Whole-batch functions
can use that registry directly. Rust trait objects are not a stable FFI, so future foreign hosts
need an explicitly defined batch invocation boundary.
