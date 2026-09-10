# Import provenance

The executor was imported from the `vortex-morsel` tree at
`ae8b9800409a60d1ceebb2b8181a144581a0cc45` on `codex/morsel-push-optimized`.
The exact import is commit `e592bf4269add47dfb3994d105e55652cae30503`.

This branch develops the physical push pipelines directly. The former recursive execution mode
and separate pull crate were removed after checkpoint `bd1f3cbbe8`. That checkpoint retains the
comparison implementations and their evaluation records.

The shared array, buffer, and mask prerequisites were imported separately, including the newer
sparse `intersect_by_rank` optimization already present in this repository.
