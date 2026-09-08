# Validation and reproduction

[Research landing page](../../README.md) | [Alternative patches](experiments/README.md)

## Recorded results

Validation ran on September 8, 2026, in isolated local checkouts of the reviewed PR snapshot. The
combined source is the first research commit on this branch. The documentation commit does not
change the tested Rust code. The repository pins Rust 1.98.0.

| Check | Result | Evidence |
| --- | --- | --- |
| Layout/file tests and aggregate/statistics tests | 929 passed | [Test log](evidence/logs/final-tests.log) |
| Other array tests | 3001 excluded by the test filter | [Test log](evidence/logs/final-tests.log) |
| Doctests in the three affected crates | 76 passed, 24 ignored | [Doctest log](evidence/logs/final-docs.log) |
| Clippy in the three affected crates, all targets and features | Passed | [Clippy log](evidence/logs/final-clippy.log) |
| Workspace Clippy, all targets and features | Passed | [Workspace log](evidence/logs/workspace-clippy-final.log) |
| Nightly formatting and `git diff --check` | Passed for the research changes | Recorded during the investigation |
| Combined patch application against the reviewed PR head | Passed | Rechecked before publication |

The logs replace the local checkout path with `$REPO`. Trailing blank lines are removed. Test output and diagnostics retain their original contents otherwise. The tests establish behavior, not performance.

The aggregate API cleanup also passed 22 zone-map tests, 14 bounded aggregate tests, and two
identity tests in its independent prototype. The combined run includes the relevant tests.

## Run the combined experiment

Use a fresh clone or a clean checkout of this branch. No patch application is necessary. Cargo
downloads the pinned toolchain and dependencies when they are absent.

```bash
git clone --branch ct/skip-index-research https://github.com/vortex-data/vortex.git
cd vortex
```

The recorded commands used these build settings:

```bash
export CARGO_BUILD_JOBS=3
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
```

For a quick check, run the file integration tests:

```bash
cargo test -p vortex-file --test bloom_skip_index
```

For the full recorded test selection, install `cargo-nextest` if it is absent:

```bash
cargo install --locked cargo-nextest
```

Then run the same selection:

```bash
cargo nextest run -p vortex-array -p vortex-layout -p vortex-file \
  -E 'package(vortex-layout) | package(vortex-file) | test(aggregate_fn) | test(stats::)'
cargo test --doc -p vortex-array -p vortex-layout -p vortex-file
cargo clippy -p vortex-array -p vortex-layout -p vortex-file --all-targets --all-features
cargo clippy --all-targets --all-features
```

Workspace Clippy includes bindings and query-engine integrations. Its dependencies are broader than
those of the quick integration test.

The recorded formatting command was `cargo +nightly fmt --all`. It also changed unrelated baseline
formatting in DuckDB/FFI. Those unrelated changes were restored before publication. No such
restoration is necessary to run the tests.

## Interpreting the experiments

The registration comparison includes intentionally failing alternatives. The [six-case
matrix](evidence/registration-comparison.md) explains their results. The [reader experiment
notes](evidence/reader-experiments.md) explain the pruning oracles and why file-level statistics
must be excluded in the unknown-plugin experiment.

The Bloom comparison selects an actual false positive at runtime. An arbitrary absent value is not
guaranteed to distinguish two filters. The experiment also checks that present values survive.

No performance benchmark, on-disk schema migration, or persisted Mean round trip is claimed. The
format options in the reader findings are design alternatives only.
