<!--
SPDX-License-Identifier: CC-BY-4.0
SPDX-FileCopyrightText: Copyright the Vortex contributors
-->

# Handoff: LPC research, `claude/lpc-encoding-research-21n7s7`

The findings are in `README.md`, and the literature review is in `LITERATURE.md`. This page covers
how to pick the work up on another machine.

## State

| Piece | Status | Notes |
|---|---|---|
| Dataset download (`fetch.py`) | Done | 85 series, 37M values, 13 sources, with no API keys. Fetching takes ~20 min (PLAID is a 700 MB zip). |
| Size model (`lpc_eval.py`) | Done | Every number is in `results.jsonl`. A full run takes ~12 min on 4 vCPUs. |
| Real Vortex sizes (`export_vortex.py`, `vortex_compare.py`) | Done | Uses `affine_eval` from `ji/affine-encoding` (`ef6e698d6`). Raw output is `vortex_results.tsv`. |
| LPC vs Delta, both cascaded (`cascade_compare.py`) | Done | Raw output is `vortex_fixed_results.tsv`. |
| Encoder ablations (`ablate.py`) | Done | Sampled fit, coefficient precision, accumulator range. |
| Decode kernels (`lpc_kernels.rs`) | Done; re-measure on a better CPU | Serial, autovectorized lanes, AVX-512 lanes, fused unpack + LPC, fused + 16×16 transpose to original order. |
| pco timings on real data (`pco_speed.py`) | Done for 4 series | |
| A real Vortex `fastlanes.lpc` encoding | Not started | See "Next steps" below. |

## Setup on the new machine

```bash
git fetch origin claude/lpc-encoding-research-21n7s7 && git checkout claude/lpc-encoding-research-21n7s7
python3 -m venv venv && ./venv/bin/pip install numpy scipy pcodec zstandard wfdb soundfile pyarrow obspy pyedflib h5py
./venv/bin/python -I research/lpc/fetch.py data      # writes data/raw, data/series/*.npy, data/manifest.jsonl
./venv/bin/python -I research/lpc/lpc_eval.py data   # resumable; appends to data/results.jsonl
./venv/bin/python -I research/lpc/summarize.py data/results.jsonl
```

- `data/` is not committed. Put it outside the repo, or keep it out of commits.
- `fetch.py <dir> <source...>` re-fetches only some sources: `iris tides geomag physionet audio
  music jena household binance cwru more grid daphnet`.
- The Wikimedia API rate-limits shared proxies. Sleep-EDF downloads were also too slow to finish
  here, so neither source is used.
- For the real Vortex numbers, see the end of the "Reproducing" section in `README.md`. In brief:
  - add a worktree of `ji/affine-encoding`;
  - build `affine_eval` with `--features pco` (~7 min on 4 vCPUs);
  - run `export_vortex.py`, then `affine_eval`, then `vortex_compare.py` and `cascade_compare.py`.

## What to re-measure on the stronger CPU

1. **Kernels.** Build with
   `rustc --edition 2024 -O -C target-cpu=native research/lpc/lpc_kernels.rs`, then run it with
   the core pinned (`taskset -c 2`) and frequency scaling off. Record the CPU model.
   - Every kernel asserts against the scalar reference before it is timed.
   - The serial baseline needs a second build with `-C llvm-args=-vectorize-slp=false`.
     Otherwise LLVM vectorizes the tap loop and serial gets 2–3x slower.
   - The AVX-512 kernels need `avx512f`. On AVX2-only CPUs, port `lanes_avx512` and
     `unpack_lpc_ordered_avx512` to 8-lane `__m256i`. That port does not exist yet.
2. **End-to-end against Vortex Delta.** Compare `unpack_lpc_ordered_avx512` at orders 4/8/16 with
   `ji/affine-encoding`'s `chunk_delta_decode` example and FastLanes Delta on the same machine.
   The README's comparison (0.64 vs 0.6–0.76 ns/value at order 4) mixes two machines.
3. **Tune the transpose path.** The ring buffer is an array of `__m512i` indexed by `t % 64`, so
   it may stay in memory, and the 16×16 transpose costs about 0.25 ns/value. Try:
   - unrolling by 16 rows, keeping the last `p` rows in registers;
   - decoding 32 rows per tile.
4. **i16 multiply path.** For 16-bit data with 12-bit coefficients, `vpmaddwd` handles two taps per
   instruction, which should roughly halve orders 16–32.
5. **pco Conv1 versus lane LPC** on all 85 series: run `pco_speed.py data <names...>`.
6. **Encoder in Rust.** Write a covariance-method fit on a 1/16 sample, then time encode in
   ns/value. Python numbers are not representative.

## Next steps toward a Vortex encoding

These are listed in order.

1. Prototype `fastlanes.lpc` in `encodings/fastlanes`, next to Affine and ChunkDelta on
   `ji/affine-encoding`, and rebase onto that branch.
   - **Layout:** 16 lanes × 1024 values per block, residuals packed vertically.
   - **Metadata per lane:** i16 coefficients, `shift`, and warm-ups.
   - **Child:** the residual child cascades.
2. Write an `LpcScheme` that estimates by compressing whole sampled chunks through the cascade, as
   `ChunkDeltaScheme` does.
   - It steps aside unless it beats the best non-LPC tree by a margin.
   - It steps aside when the column already compresses well with dict, run-end or sequence
     encodings: timestamps, IMU, PPG and EMG all lost 10–76% in the real Vortex run.
3. Add the winning datasets to the `affine_eval` manifest and compare real trees with the scheme
   enabled. Use seismic LHZ/BHZ/LDO, CWRU vibration, GTZAN, ESC-50, the PLAID currents, ECG
   MIT-BIH/PTB, the tides and Fantasia respiration.

## Gotchas

- **pco Conv1 rejects 64-bit types,** so `lpc_eval.py` casts series to i32 when they fit.
- **Everything is divided by its GCD first.** Binance prices carry 8 decimals and a large GCD.
- **The `imu_ltmm` series are saturated at ±32767,** and three of them are shorter than 4096
  values and are skipped.
- **Two PLAID columns (`1816_current`, `1005_voltage`) have no exact integer lattice** after
  decimal scaling. pco's float modes beat every predictor on them, so treat them as artifacts.
- **Per-chunk models matter for non-stationary data** (barometer 1.39x vs 1.11x with one model per
  64K block). Keep per-lane coefficients in the format; the measured decode cost is ≤10% at
  orders ≤16.
- **The real-Vortex gains are partly a BtrBlocks weakness.** On several originals the compressor
  does not find a bit-packed delta (e.g. `chunk_delta(primitive, …)` on tides). The model's ratios
  are the conservative ones.
