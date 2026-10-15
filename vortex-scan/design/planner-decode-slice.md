<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Handoff: minimal planner-driven decode slice

Status: handoff for a new implementation session. Verified against `origin/develop` at
`18aebeaac8` (which includes #10462) and source commit `42b29cc459`.

## Goal

Make the `vortex-scan` planning driver (`Run` / `Planner` / `Morsel`) execute develop's sans-IO
`pipeline::Scan` decode/read path, with all IO going through the planning protocol
(`IoRequest` / `Completion`), as the first mergeable step towards planner-driven V2 scans.

Source material:

- `42b29cc459`, PR [#10464](https://github.com/vortex-data/vortex/pull/10464) "feat(scan):
  planner-driven V2 scans on the physical plan pipelines" (+11367 lines). Not an ancestor of
  develop; its merge base is `1a9ed685d1`.
- Full-change reference: PR [#10466](https://github.com/vortex-data/vortex/pull/10466), branch
  `origin/ji/vibrant-sagan-2w7pov`.
- PR [#10462](https://github.com/vortex-data/vortex/pull/10462) (Zoned pruning, `Share`,
  `ValueCache`) is **already merged into develop** as `18aebeaac8`.
- PR [#10463](https://github.com/vortex-data/vortex/pull/10463) (`3c37da1a2a`, coalescing IO
  service, `IoIntent::Forget`, `vortex_io::request::trace`) is **not** on develop and not needed.

## Scope

In scope: driver, a protocol adapter from `pipeline::Scan` to `IoRequest`s, a projection-only
planner, segment IO via the existing `SegmentSource`, and an opt-in hook behind
`VORTEX_SCAN_V2=1` for scans with no filter and no list columns.

Non-goals (later PRs): filter evaluation (`FilterPlanner`, conjunct ordering), zone pruning in
the planner, IO announcements and prefetch, the coalescing IO service, plan caching, repeated
scans, DuckDB integration, DataFusion diagnostics, and any performance work.

## What develop already has

| Piece | Location on develop |
| --- | --- |
| `WorkScope`, `State`, `PlannerOutput`, `Planner` | `vortex-scan/src/planning/planner.rs` (81 lines) |
| `Morsel`, `MorselOutput` | `vortex-scan/src/planning/morsel.rs` (54) |
| `Next`, `next_fn` | `vortex-scan/src/planning/next.rs` (74); module root `planning/mod.rs` (11) |
| `IoRequest`, `IoBatch`, `IoIntent::{Fetch,Prefetch,Announce}`, `IoTarget`, `IoResult`, `Completion`, `IoSource`, `IoService`, `IoConsumer` | `vortex-io/src/request/mod.rs` (223); no `Forget`, no `trace` module |
| `pipeline::Scan` with `try_new`, `with_value_cache` (L712), `step() -> Turn::{Read,Output,Waiting,Done}` (L718), `deliver(ReadId, BufferHandle)` (L755) | `vortex-layout/src/plan/physical/pipeline/scan.rs`; `Turn` at L82 |
| `ValueCache` | `vortex-layout/src/plan/physical/pipeline/values.rs` L22 |
| `lower(&LayoutRef) -> PlanRef`, public, handles Flat/Chunked/Struct/Dict/List/Zoned/LegacyStats | `vortex-layout/src/plan/physical/lower.rs` L50 |
| `optimize(PlanRef)` | `vortex-layout/src/plan/physical/optimize.rs` L28 |
| `plan_row_idx_expression` | `vortex-layout/src/plan/physical/plans/row_idx.rs` L135 |
| `attempt_split_ranges` | `vortex-layout/src/scan/splits.rs` L33 |

## What lives only in `42b29cc459`

Driver (`vortex-scan`):

- `src/planning/driver/mod.rs` (644): `RootId` L56, `Batch` L59, `Progress` L69, `Run` L194 with
  `new` L222, `admit` L241, `cancel` L262, `advance` L282, `complete` L331, `poll_completion`
  L372; blocking `Driver` L600-644 (`run` L624).
- `src/planning/driver/trace.rs` (75), `driver/tests.rs` (814), `planning/tests/mod.rs` (6),
  `planning/tests/scripted.rs` (418).
- `planner.rs` +12 / `morsel.rs` +6: only `trace_name()` / `trace_selection()` default methods,
  used solely by tracing.
- `Cargo.toml` +6: `vortex-utils` dependency; dev-deps `parking_lot`, `rstest`,
  `vortex-array` (`_test-harness`).

Hidden dependency on #10463: `driver/mod.rs` L41 imports `vortex_io::request::trace::timestamp_ns`
and L254 calls `IoSource::trace_id()`; `trace.rs` imports `trace::{next_id, timestamp_ns}`.

## PR plan

Extract each file with `git show 42b29cc459:<path> > <path>` and then trim as described.

### PR A: driver in `vortex-scan` (~600 + ~1.2k tests)

Standalone on develop.

1. Copy `vortex-scan/src/planning/driver/{mod.rs,tests.rs}`,
   `vortex-scan/src/planning/tests/{mod.rs,scripted.rs}`; add `pub mod driver;` and
   `#[cfg(test)] mod tests;` to `planning/mod.rs`.
2. Strip tracing: drop `trace.rs`, the `trace` field on `Run` (L195) and its setup in `new`
   (L224), every `tracing::debug!(ts_ns = timestamp_ns(), ...)` block (admit, cancel, completion,
   selection ~L499-530), and the `timestamp_ns` import. About 60 lines. Do not add
   `trace_name`/`trace_selection` to `Planner`/`Morsel`.
   Alternative: cherry-pick the ~40 lines of `vortex-io` from `3c37da1a2a`
   (`request/trace.rs` +24, `request/mod.rs` `trace_id()`); not recommended for this slice.
3. Add the `Cargo.toml` deps listed above.
4. Tests: `driver/tests.rs` and `scripted.rs` exercise the driver with scripted planners and a
   fake `IoSource`; drop any test that asserts trace output.

### PR B: protocol adapter in `vortex-layout` (~700 + ~300 tests)

Depends on A. New `vortex-layout/src/scan/planning/` and `vortex-layout/src/scan/v2/` modules.

- `scan/planning/graph.rs` (257): `ScanGraph` wraps `pipeline::Scan`; `ProtocolGraph` maps
  `Turn::Read` to a `Fetch` `IoRequest` (L207-216) and delivers `IoResult`s back. Keep
  `.with_value_cache(values)` (L73) and the `ValueCache` import (L27): they compile on develop
  now that #10462 landed.
- `scan/planning/projection.rs` (568): keep `ProjectionPlanner` with `new` and `cut` (L104) and
  `ProjectionMorsel` (L251-~362). Trim `compute` (L202) to: take `selected`, `cut`, pop a split,
  emit `PlannerOutput::Morsel`. Drop `prefetch` (L70), `forget_pruned` (L131, needs
  `IoIntent::Forget` from #10463), the `VORTEX_SCAN_IO_FORGET_PRUNED_PROJECTION` switch, the
  `plan_segments` import from `v2::prefetch`, and the tests at L382+ that cover them.
- `scan/planning/mod.rs` (171): keep `SegmentLocation` (L44) and `ScanPlans` (L66, **including**
  `values: ValueCache`). Move `SelectedRows` here from `filter.rs` L62. Drop `announce`,
  `filter`, `plan_split`/`plan_sparse_projection` (they wrap `AnnouncePlanner`); replace with a
  `plan_projection(plans, scope, mask) -> Box<dyn Planner>` returning `ProjectionPlanner`.
- `scan/v2/io.rs` (321): keep `segment_ranges`, `SegmentScanIo`, `SegmentIoSource` (L36-212),
  adapting `SegmentSource` to `IoService`. Delete the `IoIntent::Forget` branch (L111). Keep the
  two tests (L213+), rewriting any that build `Forget` requests.
- `scan/v2/split.rs` (90): keep `SplitPlan::run`, the `admit` / `advance` /
  `poll_completion` loop (~L76-85). Drop `EARLY_ANNOUNCE` (L44-60).
- `scan/v2/splits.rs` (343): keep `projection_splits` (L252), `chunk_starts` (L135) /
  `collect_starts`, `layout_chunk_starts` (L54) / `collect_layout_starts`, `cut_at` (~110 lines).
  Drop `max_split_rows`, `MAX_SPARSE_ROWS`, the filter/sparse boundary functions.
- `scan/v2/lower.rs` (207): **drop entirely**. Develop's `plan::physical::lower` now handles
  Zoned/LegacyStats, lowering them to their data when unoptimized.
- Add `pub(crate) mod planning; pub mod v2;` to `scan/mod.rs`.
- Tests: an in-memory `SegmentSource` plus layouts built like
  `vortex-layout/src/plan/physical/pipeline/tests.rs` (`lower(&layout)`), driven by
  `Driver::run` from PR A; compare with `assert_arrays_eq!` against the default scan.

### PR C: opt-in executor behind a single fork

The source commit forked at every caller (`if v2::enabled() && v2::supports_scan(..) {
v2::into_stream(..) } else { b.into_stream() }` in the DataFusion opener, `vortex-file`, DuckDB).
This slice forks in exactly one place instead, and nothing V2-specific is added to the V1 path:

- `ScanBuilder`'s lazy prepare step calls `prepare_stream`, which is the only fork:
  `match v2::select(self)? { Executor::V1(b) => b.prepare()?.execute_stream(None),
  Executor::V2(scan) => scan.execute_stream() }`. `into_stream`, `into_iter` and the Arrow
  record-batch stream all go through it, so no caller changes.
- `v2::select` owns the whole gate: `VORTEX_SCAN_V2=1`, the builder carries a `ScanFile`, no
  filter, no limit, and every layout is Flat/Chunked/Struct/Dict/Zoned/LegacyStats (no List
  layout; lists written as flat leaves, the default, are supported). It logs the decision at
  `debug` on target `vortex_layout::scan::v2`. Each later PR widens this gate in one place.
- `VortexFile::scan()` attaches `ScanFile` via `ScanBuilder::with_scan_file`, built from
  `Footer::segment_specs_with_metadata`. Callers that build a `ScanBuilder` from a bare layout
  reader (DataFusion opener, DuckDB) stay on V1 until they attach one with the same setter: wiring,
  not a fork.
- `ScanBuilder::prepare()` / `build()` keep returning the V1 `RepeatedScan`.

### PR D (optional): more callers

- `vortex-datafusion/src/persistent/opener.rs`: add `.with_scan_file(vxf.scan_file())` to the
  builder chain. No `if`.

### Later

1. `FilterPlanner` (`scan/planning/filter.rs` 511 + `filter/tests.rs` 190) and
   `scan/v2/conjuncts.rs` (226); lift the no-filter gate.
2. Zone pruning in the planner (`scan/v2/pruning.rs` 275; `ZonedPlan` is already on develop).
3. #10463 coalescing service, `FileScanIo` (`vortex-file/src/segments/{io_service,scan_io}.rs`),
   `IoIntent::Forget`, tracing.
4. `announce.rs` (409), `prefetch.rs` (145), `share.rs` (147), split heuristics (sparse,
   `max_split_rows`), `file.rs` plan caching (102), `repeated_scan.rs` (606).
5. `vortex-file/src/planning/{footer_open,footer_prune,tests}` (~1.8k incl. fixtures).
6. DuckDB (`file_prefetch.rs`, `ffi.rs`, C++ reader), DataFusion diagnostics.

## Size

Slice: ~3.3k lines including tests (~1.85k non-test). Full stack: ~16.3k (#10462 +1622,
#10463 +3301, #10464 +11367); #10462 is already merged.

## Verification

Run only when requested, scoped per crate (`vortex-scan`, `vortex-layout`, `vortex-file`,
`vortex-datafusion`):

```bash
cargo clippy -p <crate> --all-targets --all-features -- -D warnings
cargo +nightly-2026-09-10 fmt -p <crate>   # NIGHTLY_TOOLCHAIN in .github/workflows/ci.yml
cargo nextest run -p <crate>
```

Then a parity check with `VORTEX_SCAN_V2=1` on an unfiltered DataFusion query.

## Open risks

- **CI regression, cause unknown.** On CI's 96-core ARM runners the full V2 stack regressed
  (Vortex queries up to 12x, FineWeb vortex-compact 1.25-1.8x, TPC-H SF10 q06 +28%) but not on
  4-core local runs. Per-split IO locks and the file read driver were ruled out. This slice is a
  good base to bisect it on CI.
- **Local q01 +10%:** extra decimal to Arrow conversion and bit-packed unpack versus V1's
  filter-on-compressed path. Expect the slice to be slower than V1 until filters land.
- `SegmentScanIo` issues one `SegmentSource` request per segment with no coalescing.
- Develop's `plan/physical` changed since the source commit's merge base (`1a9ed685d1`, ~2.2k
  lines via #10462). `Scan::try_new(session, plan, splits)` is unchanged, but recheck imports
  such as `pipeline::Split` and `ValueCache` when porting `graph.rs`.
- List columns cannot compile before their offsets are read; such files stay on V1.
