<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# Workspace cleanup

Cleanup restored input validation and returned the workspace to `ct/row-fn-engine-research`.
The temporary `ct/rofl-assume-valid-utf8` branch had no unique commits and was deleted.
The [experiment patch](../2026-09-29-assume-valid-utf8/experiment.patch) retains the unsafe variant.
Implementation changes, reports, and measurement evidence remain in the workspace.

The [scratch archive](scratch-logs.tar.gz) retains temporary benchmark and compiler logs.
Each archived file was compared with its original before removal.
The [manifest](manifest.json) records the removed logs and task-specific build artifacts.
Shared build dependencies and unrelated worktrees, branches, and stashes were left unchanged.
No temporary worktrees were attached to this task or registered for its rofl experiment.

Cleanup used source inspection, file comparisons, and Git state checks.
No builds, tests, formatting, linting, or benchmarks ran during cleanup.
