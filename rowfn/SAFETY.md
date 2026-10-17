<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Source review of unsafe contracts

This record describes the cleanup's source review. It does not certify memory safety. Existing test
and benchmark reports concern the source revisions recorded in those reports. The cleanup has not
run tests, doctests, Miri, builds, linting, formatting, codegen experiments, or benchmarks.

| Boundary | Invariant and enforcement inspected |
| --- | --- |
| Borrowed input | `RowView` is unsafe. Decoded owners remain live through preparation and traversal. Sources validate exact view lengths before unchecked access. A successful view must provide valid values at every in-range index. |
| Concrete text layouts | Arrow offset views borrow the decoded owner's matching offsets and bytes. The offset slice has one sentinel beyond the row domain. Arrow view rows borrow matching headers and backing buffers. These immutable borrows retain valid null payloads and the original slice offsets. |
| Scalar input | Decoding checks one retained row. Source construction checks that count again. Scalar-only execution can reduce logical evaluation to one row before host broadcasting. |
| Selection | The unsafe trait guarantees stable bounds, order, uniqueness, count, and immediate error propagation. Executors check the input and output domains before selected or filtered traversal. |
| Owned output | The unsafe buffer contract preserves slots and contents. Owned traversal excludes types that require destruction and publishes only initialized prefixes. Errors abandon partial storage. |
| Initialized scalar output | `ElementSink` writes defaults before lending `&mut T`. The conversion from `MaybeUninit<T>` covers only that initialized prefix. Safe mutable values cannot remove initialization. It uses `()` rather than an unsafe token. |
| Uninitialized scalar and list output | Token constructors remain unsafe. They require the current callback's exact entire row and preservation of initialization. The tokens themselves do not encode identity. |
| Fixed-size lists | Allocation checks row-count multiplication. The explicit row count survives width zero. Each non-empty row lends its own bounded child range. |
| Strings | Output writers retain owned bytes, check representation limits, and preserve initialized empty placeholders. Arrow publication uses an unchecked constructor only after those invariants. Vortex input decoding retains validation and null sanitation. |
| Boolean packing | Collectors can invoke closures containing unchecked reads. The unsafe host contract bounds callback indices, and shared kernels handle sliced borrowed bitmaps without assuming padding. |
| Deferred errors | Only rejected row evidence enters validity-based suppression or retry. Decoder, allocation, shape-validation, and publication errors return directly. |

The low-level initialization tokens remain intentionally unsafe. Making only their constructors
safe would permit a callback to return evidence for an unrelated slot. `ElementSink` offers a safe
alternative by establishing initialization at allocation, with an explicit cost in default writes.
The cleanup does not add runtime row identities or change existing uninitialized sink behavior.

The existing deferred Boolean code retains its combined-state mutable borrow and separate terminal
and retry loops. The lane and packing algorithms remain unchanged. Moving shared function definitions
to another crate can affect compiled code even when callbacks are unchanged, so historical timings
do not measure the cleanup.

## Focused follow-up checks

These commands are references for an explicitly requested verification run:

```sh
cargo nextest run -p rowfn -p rowfn-arrow -p rowfn-examples
cargo test --doc -p rowfn -p rowfn-arrow
cargo check --locked -p rowfn-functions --no-default-features
cargo check --locked -p rowfn-functions --features arrow --example arrow
```

The new initialized-sink fixtures cover preserved defaults across views and moves, empty publication,
valid-only division, and abandonment after an error following a successful write. Existing fixtures
cover lifetime and token compile failures, partially initialized abandonment, dictionary nulls,
metadata, sliced bitmaps, custom bindings, and cross-host registration.

For compiler-sensitive changes, compare primitive and scalar specialization, deferred arithmetic,
Boolean packing, and nullable retry with a matching compiler, target, CGU count, and LTO mode.
The current boundary benchmarks use 16 CGUs and no LTO. A vector operation found in one loop does
not establish that all relevant branches vectorize. Full timing and codegen checks remain opt-in.

The text dispatch follow-up changes `TextBinding` and moves shared function selection to concrete
layout families. It adds Arrow-only binding fixtures, mixed-layout function fixtures, and direct
versus shared length collection controls. No tests, compiler checks, or benchmarks ran for this
follow-up. Increased layout specialization can change code size and inlining, so its generated code
and performance remain unresolved.
