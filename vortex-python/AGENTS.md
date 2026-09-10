# Python Binding Guidance

Applies to the Python bindings and their PyO3 implementation under `vortex-python/`.

Run the commands below from the repository root.

## Build and Development

Follow the [Python binding development workflow](../CONTRIBUTING.md#python-bindings) in the
contributor guide for environment setup, Maturin rebuilds, targeted testing, Cargo features, and
the full Python check. Keep the contributor guide as the source of truth for shared commands.

## Testing

Run the narrow checks that match the files changed before broader Python suites:

```bash
uv run --all-packages pytest <changed-python-tests>
```

If Python docstrings, `docs/api/python/`, or Sphinx configuration change, also follow
`docs/AGENTS.md` and run the relevant clean Sphinx checks.

## Linting and Formatting

```bash
uvx ruff format --check <changed-python-files>
uvx ruff check <changed-python-files>
uv run basedpyright vortex-python
```

Use `uvx` for ruff and `uv run` for basedpyright, matching how CI invokes each. Do not add a
`python -m py_compile` pass: every syntax error it can report is already reported by `ruff check`,
`basedpyright`, and pytest collection.

If PyO3 Rust files under `vortex-python/src/` change, also run the Rust checks in the root
`AGENTS.md`, scoped with `-p vortex-python`.

Always finish Python binding work with `git diff --check`.
