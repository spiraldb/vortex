<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# IO changes integrated with the updated PR branch

The local IO work was backed up, then the checkout was fast-forwarded from `afeb2d1a4` to
PR #10339's `ji/planning-plan-planexec` head,
`e645cd9a97ccf55f6b9fc1ef2013b53ff8780389`, bringing in all 27 intervening commits. The IO
changes were reapplied and six overlapping files were reconciled before validation.

## Preserved behavior and conflict resolutions

- Upstream reads retain one registered consumer per announcement. Fetches consume those
  registrations, share pending reads, and release consumed bytes instead of retaining them
  for the whole split. Both inline and boxed fetch futures hold their own read reference until
  completion; a pruning withdrawal cannot cancel another consumer's fetch.
- Upstream announcements retain one registration per potential projection consumer and skip
  chunks excluded by the input selection. The projection-announcement control remains available.
- Upstream filtering retains its initial mask, dense predicate planning, and optional bounded
  projection prefetch. The new selection diagnostics coexist with that implementation.
- Both the upstream compute-timing events and detailed driver tracing remain available.
- A single projection morsel still publishes its own reads without a separate prefetch batch.
  Pruning withdrawal protects its selected ranges and aliases even when that batch is empty.
  Two regression cases cover distinct and aliased ranges.
- Seven existing Arc-clone lint errors in updated tests were corrected without changing behavior.

The local file-handle reuse, native-read admission and demanded-extent coalescing controls remain
opt-in. Previously rejected cost-based speculative coalescing and compute experiments are not
included in this IO patch. The updated branch's encoding kernels and split scheduling are retained.

## Local validation after integration

All commands below run against the integrated checkout. Build artifacts and temporary files use
the instance SSD at `/mnt/vortex-ssd/votex-4`; focused native test runs also use an SSD working
directory. The broad Rust library run enables V2 and all crate features.

| Check | Result |
|---|---|
| `vortex-file` library tests | 221 passed |
| `vortex-io` library tests | 197 passed |
| `vortex-layout` library tests | 582 passed |
| `vortex-scan` library tests | 57 passed |
| File scan IO tests with boxed fetches, ready fetches, and demanded/native32 settings | 15 passed per setting; 45 additional executions |
| Projection tests with withdrawal disabled and enabled | 51 passed per setting; 102 additional executions |
| Benchmark diagnostic extrema aggregation regression | 1 passed |
| Doctests for the four library crates | 1 passed; 3 existing examples ignored |
| Python benchmark/analyzer tests | 48 passed |
| All-target/all-feature Clippy for the four libraries and `datafusion-bench`, warnings denied | Passed |
| Pinned `nightly-2026-09-10` formatting for those five crates | Passed |
| Scoped Ruff lint, Ruff format and ty checks for the four tool/test files | Passed |
| Patch whitespace | Passed |

The broad Rust command is:

```bash
CARGO_TARGET_DIR=/mnt/vortex-ssd/votex-4/target \
TMPDIR=/mnt/vortex-ssd/votex-4/tmp VORTEX_SCAN_V2=1 \
cargo test --locked -j 8 \
  -p vortex-io -p vortex-file -p vortex-layout -p vortex-scan \
  --all-features --lib -- --test-threads=4
```

Clippy uses the same target and temporary directories:

```bash
cargo clippy --locked \
  -p vortex-io -p vortex-file -p vortex-layout -p vortex-scan -p datafusion-bench \
  --all-targets --all-features -- -D warnings
```

Logs, the original patch/file archive, and focused test commands are under
`/mnt/vortex-ssd/votex-4/results/integrate-pr10339-20261008T203936Z/`.
No workspace-wide tests, cross-platform checks or SQL benchmark rerun were performed during
this integration.

## Performance evidence remains tied to its measured source

[The IO-only report](REPORT_IO_ONLY.md) retains the cold SSD results and their sequential
attribution: handle reuse/native32 were measured together, while coalescing was isolated on
top of that bundle. The 22.95% suite reduction and four eight-round confirmations refer to
the frozen pre-integration executable, not this newer merged source or an untouched PR baseline.
No new latency reduction is claimed for this integration. Changes in upstream read lifetimes
and planning make a fresh benchmark necessary before transferring those figures to the new tip.


## Refresh on 2026-10-16

The original feature branch was rebuilt at
`1c932c195009c5138a71b6815de7cc05b265626b` on the newer physical-plan pipelines.
Its IO commit already contains descriptor reuse, process-wide native admission,
demanded-extent coalescing, and the earlier IO lifetime work. This refresh starts
from that branch head and preserves its pipeline implementation and fallback rules.
The old executors are not restored.

The remaining local changes add resumable cold comparisons, broader contention
recognition, phase distributions, and request-count/byte queue occupancy to the
Python tools. Presets for the rejected backend prototypes have been removed;
historical trace fields remain readable. The discarded Rust experiments are not
included.

The local PR SQL benchmark step explicitly enables descriptor reuse, native32,
and demanded-extent coalescing. The remote benchmark step and develop-mode defaults
are unchanged. Standard SQL CI runs use their existing iteration policy; they are
not evidence of fresh-process cold performance. A separate local cold rerun uses
SSD data/executables/temp/output and verified page eviction.

Validation on the refreshed branch: 224 file and 197 IO library tests pass, with
V2 enabled and all crate features, using native test executables and temporary
files on SSD. All 60 Python tool tests pass. Strict all-target/all-feature Clippy
passes for the two IO crates; scoped Ruff, ty, workflow yamllint, and patch
whitespace checks pass. No new Rust source is introduced by this refresh.

The older latency reductions remain historical results for their recorded
executables. No new speedup is claimed for this pipeline revision before its
measurements finish.


### Merge with current develop

PR workflows could not start because the original feature branch conflicted with
`develop`. The refresh therefore also merges
`c59037cdec66e367749ff625374a795fe4dbd883` locally before publication.

Conflict resolution keeps the upstream legacy-zone conversion and pruning-proof
invalidation, pointer-keyed value cache, and relocated shared-array encoding. V2
lowering is adapted to the new legacy-statistics plan. The feature branch retains
its dynamic-value cache safeguards and V2 builder. DuckDB's V2 preparation and
optional file-prefetch path retain upstream's deferred scan-state destruction
outside its global lock; the opened Vortex file belongs to that shared state.

The merged source passes 1,112 Rust library tests: 224 file, 197 IO, 631 layout,
and 60 scan. The file and IO suites also pass with the measured reuse/native32/
demanded policy enabled. Strict all-target/all-feature Clippy passes for the four
libraries, DuckDB, and the DataFusion benchmark. The layout golden fixture was
copied unchanged into the SSD test working directory before its successful run.
Whitespace checks cover the resolved files; trailing blank lines in incoming JOB
SQL files are unchanged from develop.


DuckDB validation also passes: 244 library/end-to-end tests, with two existing
ignored cases. The total is 1,356 passing library tests across five crates, plus
421 further IO/file test executions with descriptor reuse, native32, and demanded
coalescing enabled. Four legacy coalescer cases now set their intended optional
expansion policy explicitly; their expected results are unchanged, and the
parameterized cases continue to cover both policies. The 60 Python tests and
strict affected-crate Clippy checks pass after reconciliation.
