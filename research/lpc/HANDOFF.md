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
| Dataset download (`fetch.py`) | Done | 70 series, 35M values. Fetching takes ~15 min, with no API keys. |
| Size model (`lpc_eval.py`) | Done | Every number is in `results.jsonl`. A full run takes ~10 min on 4 vCPUs. |
| Summary tables (`summarize.py`) | Done | Generates the tables in `README.md`. |
| Decode kernels (`lpc_kernels.rs`) | Done, but needs a better CPU | It was measured on a noisy 4-vCPU Xeon VM @ 2.1 GHz. |
| pco timings on real data (`pco_speed.py`) | Done for 4 series | |
| A real Vortex encoding | Not started | See "Next steps" below. |

## Setup on the new machine

```bash
git fetch origin claude/lpc-encoding-research-21n7s7 && git checkout claude/lpc-encoding-research-21n7s7
python3 -m venv venv && ./venv/bin/pip install numpy scipy pcodec zstandard wfdb soundfile pyarrow obspy pyedflib
./venv/bin/python -I research/lpc/fetch.py data      # writes data/raw, data/series/*.npy, data/manifest.jsonl
./venv/bin/python -I research/lpc/lpc_eval.py data   # resumable; appends to data/results.jsonl
./venv/bin/python -I research/lpc/summarize.py data/results.jsonl
```

- `data/` is not committed. Put it outside the repo, or keep it out of commits.
- `fetch.py <dir> <source...>` re-fetches only some sources: `iris tides geomag physionet audio
  music jena household binance cwru more`.
- The Wikimedia API rate-limits shared proxies. Sleep-EDF downloads were also too slow to finish
  here, so neither source is used.

## What to re-measure on the stronger CPU

1. **Kernels.** Build with
   `rustc --edition 2024 -O -C target-cpu=native research/lpc/lpc_kernels.rs`, then run it with
   the core pinned (`taskset -c 2`) and frequency scaling off.
   - Also build with `-C llvm-args=-vectorize-slp=false`. That build gives the honest serial
     baseline, because otherwise LLVM SLP-vectorizes the tap loop and serial gets 2x slower.
   - Record the CPU model and its AVX-512 / AVX2 support.
2. **The 8/16/32-lane anomaly.** At order ≥4 those lane counts ran far slower than 64 lanes
   (2–8 ns vs 0.3–1.7 ns/value). Check the assembly (`cargo asm`, or `objdump -d` on `lanes::<8, 32>`).
   I expect 32 lanes to match 64 once the accumulator stays in registers. If it does not,
   hand-write the kernel with `std::arch` intrinsics.
3. **Fused decode.** Fuse a FastLanes bit-unpack with the 64-lane predictor, and time the pair
   end to end against Vortex Delta. The baseline is the `chunk_delta_decode` example on
   `ji/affine-encoding`.
4. **pco Conv1 versus lane LPC** on all 70 series: run `pco_speed.py data <names...>`.
5. **Encode cost.** The least-squares fit is O(n·p²). Measure fitting on a 1/16 sample against a
   full fit, at orders 16 and 32.

## Next steps toward a Vortex encoding

These are listed in order.

1. Prototype `fastlanes.lpc` in `encodings/fastlanes`, next to Affine and ChunkDelta on
   `ji/affine-encoding`, and rebase onto that branch.
   - **Layout:** 64 lanes × 1024 values, residuals transposed.
   - **Metadata per lane:** i16 coefficients, `shift`, and warm-ups.
   - **Child:** the residual child cascades.
2. Write an `LpcScheme` that estimates by compressing whole sampled chunks through the cascade, as
   `ChunkDeltaScheme` does.
   - It steps aside unless it beats Delta, delta-of-delta and FoR by at least 0.5 bits/value.
   - It also skips when ANS-coded deltas are far smaller than bit-packed deltas, which marks a
     peaked or few-valued alphabet (BIDMC, EMG, MIT-BIH).
3. Add the winning datasets to `affine_eval`, since it already reads raw `i64` columns from a
   manifest. Use seismic LHZ/BHZ, CWRU vibration, GTZAN, ESC-50, the IU.ANMO barometer and the
   Fantasia respiration record. Then compare the real Vortex trees.

## Gotchas

- **pco Conv1 rejects 64-bit types,** so `lpc_eval.py` casts series to i32 when they fit.
- **Everything is divided by its GCD first.** Binance prices carry 8 decimals and a large GCD.
- **The `imu_ltmm` series are saturated at ±32767,** and three of them are shorter than 4096
  values and are skipped.
- **Per-chunk models matter for non-stationary data** (barometer 1.39x vs 1.11x with one model per
  64K block). Keep per-lane coefficients in the format; the measured decode cost is ≤10% at
  orders ≤16.
