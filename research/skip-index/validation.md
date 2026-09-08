# Validation

[Research](../../README.md) | [Alternative patches](experiments/README.md)

The combined prototype is commit `3e481dfd16`, based on PR head
`97fa19c640ffcb80e06608b0d64d358b8049c004`. The checks ran on September 8, 2026, with Rust 1.98.0.
Later documentation commits do not change the tested source.

| Check | Result | Log |
| --- | --- | --- |
| All layout/file tests and the array aggregate/statistics tests | 929 passed | [Tests](evidence/logs/final-tests.log) |
| Other array tests | 3001 excluded by the filter | [Tests](evidence/logs/final-tests.log) |
| Doctests in the three affected crates | 76 passed, 24 ignored | [Doctests](evidence/logs/final-docs.log) |
| Clippy in the three affected crates, all targets and features | Passed | [Affected crates](evidence/logs/final-clippy.log) |
| Workspace Clippy, all targets and features | Passed | [Workspace](evidence/logs/workspace-clippy-final.log) |
| Nightly formatting and `git diff --check` | Passed for the research changes | Recorded during the investigation |
| Combined patch applied to the reviewed PR head | Produces the prototype commit's source tree | Checked before publication |

The logs replace the local checkout path with `$REPO` and omit trailing blank lines.

## Commands

The recorded runs used:

```bash
export CARGO_BUILD_JOBS=3
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
```

From this branch, the file integration tests provide a focused check:

```bash
cargo test -p vortex-file --test bloom_skip_index
```

The full recorded selection uses `cargo-nextest`:

```bash
cargo nextest run -p vortex-array -p vortex-layout -p vortex-file \
  -E 'package(vortex-layout) | package(vortex-file) | test(aggregate_fn) | test(stats::)'
cargo test --doc -p vortex-array -p vortex-layout -p vortex-file
cargo clippy -p vortex-array -p vortex-layout -p vortex-file --all-targets --all-features
cargo clippy --all-targets --all-features
```

Workspace Clippy includes bindings and query-engine integrations beyond the affected crates. `cargo
+nightly fmt --all` also exposed unrelated baseline formatting in DuckDB/FFI. Those changes were
restored before the prototype commit.

## Coverage limits

The [registration matrix](evidence/registration-comparison.md) records failures in the competing
registration variants. The [reader experiments](evidence/reader-experiments.md) describe the
controls used to isolate Bloom configuration and unknown-plugin behavior.

The tests cover behavior, not performance. The research does not include a file-format migration or
a persisted Mean round trip. The Mean test exercises structured partial access.
