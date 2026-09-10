# AGENTS.md

Guidance for Claude, Codex and other coding agents working in the Vortex repository.

## Task Routing

- When asked to investigate a CI failure, especially via `/ci-failure-analysis`, use the
  `.agents/skills/ci-failure-analysis` skill. It is the source of truth for fetching logs,
  classifying failures, and attributing causation.

## Overview

Vortex is a Rust monorepo for columnar array processing, compression encodings, and file IO.
The workspace also contains Java bindings in `java/`, Python bindings in `vortex-python/`,
documentation in `docs/`, and benchmark tooling in `vortex-bench/` and `benchmarks/`.

## Repository Layout

- `vortex-buffer` defines zero-copy aligned `Buffer<T>` and `BufferMut<T>`, guaranteed to
  be aligned to `T` or to a requested runtime alignment.
- `vortex-array/src/dtype` contains the `DType` logical type system used throughout Vortex.
- `vortex-array` contains the core `Array` trait and the base encodings, including most
  Apache Arrow-style encodings.
- `encodings/*` contains more specialized compressed encodings.
- `vortex-file` implements file IO. It uses `LayoutReader` from `vortex-layout`.
- `vortex-io` holds the core async and blocking IO traits, plus the generic `object_store`
  adapters that implement them.
- `vortex-cloud` holds the cloud object store integration: the URL-to-`ObjectStore` registry
  and the OpenDAL-backed services (`cos://`, `oss://`). Every binding resolves URLs through it.
- `vortex-scan`, `vortex-session`, `vortex-datafusion`, and `vortex-duckdb` contain scan
  and execution integrations.
- `vortex-python` contains Python bindings. RST-flavored project docs live in `docs/`.

## Scoped Guidance

Before changing files in a subtree, read the closest nested `AGENTS.md`. In particular:

- `.github/AGENTS.md` covers workflows and other GitHub configuration.
- `docs/AGENTS.md` covers Sphinx documentation.

## Verification

Run the narrowest check that covers the files you changed. CI already runs the workspace-wide
version of everything below, so reproducing that breadth locally costs far more than the round
trip it saves.

Test execution is optional and left to the user. The commands below document how tests are run
in this repository; they are not mandatory completion checks. Run tests when the user requests
them, and report which tests ran or were not run. Continue to run applicable lint, formatting,
and static checks.

Do not run Rust checks at all for changes that only touch Markdown, RST, Sphinx configuration,
agent configuration, comments outside Rust code, symlinks, or other metadata with no Rust/API
behavior impact. Validate those by inspection or with a targeted doc/config command, and verify
symlink or path changes with `ls`, `find`, and `git status`.

### Rust

For Rust code, public API, feature flag, or generated-file changes, run these before stopping:

```bash
cargo clippy -p <crate-name> --all-targets --all-features -- -D warnings
cargo +nightly-<pinned> fmt -p <crate-name>
```

Two details make these match CI rather than merely resemble it:

- `-D warnings` is required. Without it clippy exits successfully on exactly the warnings CI
  rejects, so a local pass predicts nothing.
- Use the nightly pinned as `NIGHTLY_TOOLCHAIN` in `.github/workflows/ci.yml`. A floating
  `+nightly` can format differently from the toolchain CI checks against, so the reformatted tree
  still fails `fmt --check`.

There is no separate build step: `cargo clippy --all-targets` compiles the crate.

When the user wants Rust tests, scope them to the affected crate. Doctests cover Rust doc comments
and crate documentation:

```bash
cargo nextest run -p <crate-name>
cargo test --doc -p <crate-name>
```

If needed for a requested test run, install cargo-nextest with `cargo install --locked cargo-nextest`.

### Python

The following applies to Python bindings and their PyO3 implementation under `vortex-python/`,
and to CUDA bindings under `vortex-python-cuda/`. Run commands from the repository root.

Follow the [Python binding development workflow](CONTRIBUTING.md#python-bindings) for environment
setup, Maturin rebuilds, targeted testing, Cargo features, and the full Python check. Keep the
contributor guide as the source of truth for shared commands; its test workflows are available
when the user chooses to run them.

Run the lint, formatting, and type checks that match the files changed:

```bash
uvx ruff format --check <changed-python-files>
uvx ruff check <changed-python-files>
uvx ty check vortex-python vortex-python-cuda vortex-ffi/cmake/tests scripts/tests
```

Use `uvx` for both Ruff and ty, matching CI. The command above covers both binding packages and the
CMake and script tests. For a narrower check, pass the affected directory, such as
`uvx ty check vortex-python` or `uvx ty check vortex-ffi/cmake/tests`. Type-check the whole binding
package when changing stubs or annotations so their callers are checked too.

ty reads Python sources and stubs and needs third-party dependencies for type information. Prepare
them with `uv sync --all-packages --no-install-workspace` to avoid building the Rust extensions for
type checking. Runtime tests still need the installed extensions.

Use targeted `# ty: ignore[rule-name]` comments for intentional violations, such as invalid-input
tests or third-party stub limitations, and explain non-obvious suppressions. Pyright suppression
comments do not suppress ty diagnostics. Keep shared ty configuration in the root `pyproject.toml`.

Functions that only return `None`, including tests, may omit the `-> None` annotation. ty does not
require return annotations, and Ruff's `suppress-none-returning` setting permits omitting them for
these functions. Bare `return` and falling through are also allowed when the
return type permits `None`; Ruff's `RET502` and `RET503` rules are explicitly disabled.

When the user wants Python tests, run the targeted suite with:

```bash
uv run --all-packages pytest <changed-python-tests>
```

Do not add a `python -m py_compile` pass: syntax errors are already reported by `ruff check`,
`ty check`, and pytest collection.

For Python docstrings, `docs/api/python/`, or Sphinx configuration changes, follow
`docs/AGENTS.md`; the contributor guide documents clean Sphinx builds and doctests. Test execution
remains the user's choice. If PyO3 Rust files change, run the Rust lint and formatting checks above,
scoped to the affected binding crate (`-p vortex-python` or `-p vortex-python-cuda`).

Always finish Python binding work with `git diff --check`.

### C++ and CUDA

Format changed `.cpp`, `.hpp`, `.cu`, `.cuh`, and `.h` files under `lang/cpp`, `vortex-cuda`,
`vortex-duckdb`, and `vortex-ffi` with the repository's `.clang-format` configuration:

```bash
clang-format --style=file -i <changed-files>
```

Pass only the files you changed; CI excludes vendored or generated CUDA and Arrow headers from its
repository-wide check. clang-format is idempotent, so a `--dry-run --Werror` pass over the files
you just formatted cannot fail and is not worth running.

When the user wants CMake integration tests, CI runs the Python unittest suite with:

```bash
python3 -m unittest discover -s vortex-ffi/cmake/tests -v
```

These tests need CMake, Ninja, the C/C++ and Rust toolchains, and the lockfile-selected Cargo
dependencies cached by `cargo fetch --locked`.

### New and generated files

These CI checks are the ones most often missed when adding files rather than editing them:

- Every source file needs SPDX headers, in the comment syntax of its language:

  ```text
  SPDX-License-Identifier: Apache-2.0
  SPDX-FileCopyrightText: Copyright the Vortex contributors
  ```

  `REUSE.toml` records the exceptions, including the CC-BY-4.0 licensing of `docs/**`.

- Spelling is checked by `typos` against `_typos.toml`.
- CI asserts `git status --porcelain` is empty after a build. Regenerate generated files with the
  repository's tooling rather than editing them by hand, and commit the result.

### Notes

- For `.github/` changes, follow `.github/AGENTS.md`, which covers both the yamllint invocation and
  the nightly toolchain pin.
- If cargo fails with exactly `sccache: error: Operation not permitted`, rerun that command
  with `RUSTC_WRAPPER=` so rustc runs directly. Only do this for that exact error.

## Rust Code Style

- Follow `STYLE.md` for Rust formatting, documentation, API, error-handling, import, safety, and
  performance conventions. Its hidden-cost accessor table is the reference for changes to
  per-element loops; back such changes with the benchmarks it names.
- Only write comments that explain non-obvious logic or important context. Do not comment
  self-explanatory code.

## Tests

- Strongly consider `rstest` cases when parameterizing repetitive test logic.
- Prefer test functions that return `VortexResult<()>` and use `?` instead of `unwrap`.
- Prefer test module names `tests`, not `test`.
- Use `assert_arrays_eq!` for array comparisons instead of element-by-element assertions.
- Keep tests concise and focused on behavior, edge cases, and regressions.
- If a bug fix is requested, add or identify a regression test when practical. Leave execution
  to the user; when tests are run, a test that passes before and after the fix does not prove it.
- If clippy lints in tests prohibit patterns that are acceptable only in test code, consider
  allowing the lint at the test module level.
- If an existing `foo.rs` module needs many tests, promote it to a directory module:
  `foo/mod.rs` plus `foo/tests.rs`, included from `foo/mod.rs` behind the appropriate test
  configuration.

## Common Mistakes

Check new and modified lines against this list before finishing:

- Adding imports inside functions when module-level imports would work.
- Updating expected test output to match buggy behavior without independently verifying the
  intended semantics.
- Silently reducing the scope of an approved plan when implementation is harder than expected.

## Summaries

When summarizing work, write valid Markdown that can be copied into GitHub. Include the checks
you ran and call out any checks you could not run.

## Branches

When creating a branch on the user's behalf, prefix its name with the user's established branch
prefix, not the agent's name (for example, do not use `codex/` or `claude/`). Infer the user's
prefix from their existing branches when possible. Otherwise, use available identity context such
as `whoami`, Git configuration, or other information the user has provided. If the evidence is
ambiguous, ask the user instead of inventing a prefix.

## Commits

All commits must be signed off by the committers in this form:

```text
Signed-off-by: "COMMITTER" <COMMITTER_EMAIL>
```
