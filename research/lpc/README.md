# LPC (linear predictive coding) for Vortex integer columns: where it wins, and what it costs to decode

LPC predicts each value from the previous `p` values, `x[i] ≈ (Σ_k q_k · x[i-k]) >> shift`, and
stores the residuals. This study asks two questions:

1. Which real datasets need LPC, because Delta, delta-of-delta and the per-chunk Affine/LeCo model
   from the `ji/affine-encoding` branch are not enough?
2. How fast can LPC decode, given that the predictor is a loop-carried dependency? Does an
   interleaving trick like interleaved rANS help?

The answers come from 70 real series across 15 categories (35M values, all downloaded from public
sources by `fetch.py`), a size model of each scheme (`lpc_eval.py`), decode kernels in Rust
(`lpc_kernels.rs`), pco's own LPC mode as a reference, and a literature review (`LITERATURE.md`).

## Summary

- **LPC wins on high-rate, band-limited physical waveforms.** Examples are vibration, seismic,
  audio, respiration, EEG and some ECG. It loses or ties on business and low-rate data:
  timestamps, prices, 1-minute power, 10-minute weather, IMU at 100 Hz, and EMG.
  - Bit-packed LPC beats the best of FoR, Delta, delta-of-delta and Affine by:
    - **≥5% on 37 of 70 series** and **≥10% on 15**;
    - **1.15x** on vibration (geo-mean), **1.11x** on seismic, **1.08x** on audio;
    - **1.39x at best**, on an IU.ANMO barometer.
  - Affine never wins on any series, and neither does LeCo's linear-in-index model. These signals
    oscillate; they do not trend.
- **On those signals, lightweight LPC beats Vortex's heaviest integer codec.** Bit-packed LPC (no
  entropy coder) is smaller than pco with default settings (`DeltaSpec::Auto`) on 17 series: 9 of
  14 audio, 5 of 14 seismic and 3 of 5 vibration. pco Auto never picks its LPC mode (Conv1).
- **The gains are real but moderate, so they need a cheap decoder.**
  - Typical wins are 5–25%. For comparison, entropy coding the residuals of the same predictor is
    often worth more: about 2 bits/value on ECG, PPG and EMG, where the residuals are peaked or
    take few distinct values.
  - On those peaked series LPC can hurt once residuals are entropy coded. The "LPC+ANS" column
    shows 0.5–0.97x on PPG, EMG and ECG. A scheme must therefore compare on the cascaded size,
    not on the bit width alone.
- **The hypothesis about decode speed is right.**
  - **pco** (on 4 of these real series): Conv1 decodes at **16.6–18.4 ns/value**, against
    **2.2–2.6 ns/value** for its delta modes.
  - **Serial integer LPC** in Rust takes **~6–11 ns/value** at orders 4–32, even with the history
    kept in registers. Orders 1–2 take ~2 ns.
  - **The literature agrees.**
    - Sprintz: "the limiting factor is serial dependence".
    - The pco docs leave Conv1 out of Auto because of its "decompression performance costs".
    - libFLAC removed its decoder SIMD.
- **The interleaving trick from interleaved rANS fixes it, and is bit-exact.**
  - **Layout:** store each block as 64 independent lanes (each lane a contiguous run of 1024
    values), transposed so one SIMD row holds one value from every lane.
  - **Speed:** the kernel decodes at **0.26 / 0.51 / 0.84 / 1.62 ns/value at orders 4 / 8 / 16 / 32**.
    That is 20–50x faster than serial, and orders ≤8 are as fast as or faster than Vortex Delta
    today (0.6–0.76 ns/value, from the `ji/affine-encoding` commit notes).
  - **Size:** per-lane warm-ups cost ≤1% of size. Each lane can also carry its own coefficients
    (one model per 1024-value chunk) at the same speed: 0.30 / 0.53 / 0.95 ns/value at orders
    4 / 8 / 16.
- **Recommendation.** Build LPC as a FastLanes-style lane-interleaved encoding:
  - orders 4–16 (32 only when it pays);
  - i32 accumulation when the bit budget allows;
  - residuals cascading into BitPacked, or into Pco/ANS for ratio;
  - a scheme that picks it only when the cascaded sample beats Delta, delta-of-delta and FoR.

  It belongs in sensor, audio and scientific workloads. It will rarely fire on TPC-H or ClickBench
  style columns.

## What was compared

Every lightweight scheme is modelled the same way. Values are cut into 1024-value chunks. Each
chunk's residuals are stored with their own frame of reference and bit width, with patches
allowed for outliers (as in the `VarBitPacked` and `BitPacked` + patches combination). Only the
predictor differs:

| Name | Predictor | Header cost per chunk |
|---|---|---|
| `for` | none (FoR) | min + width |
| `delta` | `x[i-1]`, as in ChunkDelta / Parquet DELTA_BINARY_PACKED | + first value |
| `dod` | `2x[i-1] - x[i-2]` (delta-of-delta) | + 2 warm-ups |
| `affine` | per-chunk least-squares line in the index (Affine / LeCo-fix) | + slope |
| `lpcP/chunk` | order-P LPC, least squares fit per chunk, 15-bit coefficients | + P coefficients + P warm-ups |
| `lpcP/lanes` | order-P LPC, one fit per 64K block, decoded as 64 lanes × 1024 values | + P warm-ups per lane |

The best non-LPC scheme is the minimum over `for`, `delta`, `dod` and `affine` for each series.
`lpc` is the minimum over orders 2–32 of both LPC layouts. Both layouts decode with the
lane-parallel kernel: `chunk` gives every lane its own weights.

Each predictor is also measured with its residuals entropy coded instead of bit-packed (pco with
`DeltaSpec::NoOp`, which is tANS-based): the "+ANS" columns. Further references:

- pco level 8 with `Auto`;
- the best of pco's consecutive orders 1–3;
- the best of pco's `TryConv1` orders 2–32;
- zstd levels 3 and 19.

Before any scheme runs, every series is divided by its GCD.

## Results by category

Each category is ordered by geo-mean gain of bit-packed LPC over the best non-LPC lightweight
scheme. The "+ANS" column is the same ratio when both sides entropy code their residuals.

| category | series | geo-mean LPC gain (bit-packed) | min..max | geo-mean LPC gain (ANS residuals) | LPC beats pco auto in |
|---|---|---|---|---|---|
| vibration | 5 | 1.15x | 1.07..1.23 | 1.16x | 3/5 |
| seismic | 14 | 1.11x | 0.99..1.39 | 1.11x | 5/14 |
| audio | 14 | 1.08x | 1.03..1.14 | 1.09x | 9/14 |
| tides | 4 | 1.06x | 1.04..1.11 | 1.02x | 0/4 |
| geomagnetic | 4 | 1.06x | 1.01..1.09 | 1.04x | 0/4 |
| ppg_resp | 4 | 1.05x | 1.01..1.10 | 0.62x | 0/4 |
| pcg | 1 | 1.05x | 1.05..1.05 | 1.07x | 0/1 |
| eeg | 4 | 1.04x | 1.01..1.10 | 1.04x | 0/4 |
| ecg | 6 | 1.04x | 0.99..1.10 | 0.99x | 0/6 |
| imu | 3 | 1.02x | 1.00..1.03 | 0.91x | 0/3 |
| weather | 4 | 1.01x | 1.01..1.01 | 0.98x | 0/4 |
| emg | 2 | 1.01x | 0.99..1.02 | 0.63x | 0/2 |
| power | 3 | 1.01x | 1.00..1.01 | 0.88x | 0/3 |
| financial | 1 | 0.99x | 0.99..0.99 | 0.73x | 0/1 |
| timestamp | 1 | 0.90x | 0.90..0.90 | 0.65x | 0/1 |


### Why LPC wins where it does

- **Oscillating signals.** Seismic, vibration and audio data are sums of resonances, plus noise.
  - Delta and delta-of-delta are fixed polynomial predictors, which fit only slow, smooth motion.
  - On vibration at 12 kHz, deltas are *wider* than the raw values, so FoR is the best non-LPC
    scheme there.
  - LPC learns the resonances, and orders 16–32 are chosen most often.
- **Long-period seismic channels (LHZ / LH1 at 1 Hz).** These are dominated by the microseism
  (a 5–10 s oscillation), and LPC saves 2.3 bits/value there (1.24x).
- **Non-stationary signals** (barometric pressure, geomagnetic field, speech). These prefer one
  model per 1024-value chunk (`lpcP/chunk`) over one per 64K block. The barometer gains 1.39x
  with per-chunk models but only 1.11x with one model per block.

### Where LPC does not help, or hurts

- **Slow, low-rate or step-like data.** Examples are weather at 10 min, household power at
  1 min, tides at 6 min, IU.ANMO temperature and wind, financial prices, and timestamps.
  - Delta already captures almost all of the predictable structure here, and LPC's coefficients
    and warm-ups are pure overhead.
  - These are the kinds of columns in the pco, ALP and Chimp benchmarks, and in TPC-H and
    ClickBench.
- **Peaked or quantized distributions.** BIDMC PPG/ECG, EMG, MIT-BIH ECG and the IMU gait data
  (`imu_ltmm`, ±32767) all fall here.
  - Entropy coding the plain deltas gives 2–9 bits/value less than bit-packing them.
  - LPC's rounding spreads a few-valued delta alphabet into many residual values, so "LPC+ANS"
    is worse than "delta+ANS" (0.5–0.97x).
  - Bit width alone would choose LPC here and be wrong.

## Decode throughput

### Measured

The kernels are in `lpc_kernels.rs`: residuals are in L1/L2, and the times exclude bit-unpacking.
The machine was a 4-vCPU Xeon @ 2.1 GHz VM with AVX-512, built with `-C target-cpu=native`; times
are the best of 9. The serial rows are from a build with `-C llvm-args=-vectorize-slp=false`. Left
to itself, LLVM vectorizes the dot product across the `p` taps in the serial loop, which makes it
slower (13–23 ns/value).

| order | serial, i32 acc | serial, i64 acc | 64 lanes, shared weights | 64 lanes, per-lane weights |
|---|---|---|---|---|
| 1 (delta) | 1.94 (0.42 as a plain prefix sum) | 2.26 | 0.25 | 0.22 |
| 2 | 2.01 | 2.29 | 0.24 | 0.23 |
| 4 | 5.84 | 6.21 | 0.26 | 0.30 |
| 8 | 6.26 | 6.88 | 0.51 | 0.53 |
| 16 | 7.72 | 7.69 | 0.84 | 0.95 |
| 32 | 10.77 | 22.72 | 1.62 | 2.22 |

All values are ns/value.

- **The 64-lane kernel is bound by multiply throughput, not latency.** Each row of 64 values
  needs `p` `vpmulld` instructions per 16 lanes.
  - **Lane count matters.** Orders 1–2 already run at 0.29–0.33 ns/value with 32 lanes. From
    order 4 up, the 8/16/32-lane builds run at 0.9–7.8 ns/value and do not scale with lanes. This
    looks like a codegen problem rather than a latency limit, and the lane loop is a candidate
    for hand-written intrinsics.
  - **Accumulator width matters.** An i64 accumulator halves the lanes per register and costs
    about 2x. Use i32 when `residual bits + coefficient bits + log2(p) ≤ 31`, which holds for
    16-bit audio and most sensor ADCs.
- **pco on real data** (`pco_speed.py`, the same VM; 1M values, i32, level 8):

  | series | consecutive | Conv1(8) | Conv1(32) |
  |---|---|---|---|
  | seis_IU_ANMO_LHZ_1hz | 11.25 bits, 2.60 ns/value | 9.69 bits, 17.1 ns/value | 8.96 bits, 18.4 ns/value |
  | vib_cwru_normal_12khz | 9.08 bits, 2.60 ns/value | 7.93 bits, 16.7 ns/value | 7.36 bits, 17.9 ns/value |
  | audio_gtzan_blues2_30 | 11.44 bits, 2.50 ns/value | 10.73 bits, 16.7 ns/value | 10.17 bits, 18.4 ns/value |
  | ecg_mitdb100_MLII | 3.86 bits, 2.21 ns/value | 3.98 bits, 16.6 ns/value | 3.97 bits, 18.1 ns/value |

  Conv1 costs 6–7x pco's normal decode time. Its encoder also takes 30–55 ns/value.

### From the literature

The details, with sources and hardware, are in `LITERATURE.md`.

- **Sprintz** (Blalock et al. 2018) uses FIRE, a learned AR(2). It decodes at >3 GB/s, but only
  by vectorizing across *columns*; a single column gets no SIMD. Its paper names the serial
  dependency as the limit.
- **pco Conv1** is the only LPC + tANS codec in a columnar context. On the literature agent's
  synthetic data it decodes at 0.35–0.4 GB/s, against ~1 GB/s for its consecutive mode. pco 1.0.2
  added a specialised order-6 decoder that is "only slightly faster".
- **FLAC, MPEG-4 ALS and Monkey's Audio** gain 2–4% of ratio over each other, at 1.6–1.8x the
  decode time. libFLAC 1.4 removed its decoder SIMD.
- **LeCo** (SIGMOD 2024) is linear in the index, not autoregressive. It decodes 14–34% slower
  than FOR. **TimescaleDB and InfluxDB** stop at delta-of-delta + simple8b plus Gorilla XOR; no
  TSDB ships LPC.
- **Parallel scans** of linear recurrences (Blelloch's companion-matrix scan, Martin & Cundy 2018)
  are exact only for integer-coefficient predictors with no rounding shift. For those predictors
  (delta, delta-of-delta, FLAC fixed orders) the scan is just repeated prefix sums. Quantized LPC
  is not associative, so lane interleaving is the bit-exact option.

### About the ANS idea

"ANS-style" can mean two different things, and both matter.

1. **Interleaving independent streams to hide latency.** Interleaved rANS (Giesen 2014) does this
   and gets 1.6–2.8x. FastLanes Delta does the same with its transposed lanes. For LPC this is the
   fix measured above, and it gives 20–50x because the serial chain is long (mul → add → shift →
   add per value).
2. **Entropy coding the residuals with ANS.** This is worth ~0.6–1.0 bits/value over bit-packing
   on Laplacian-like residuals, and much more on peaked ones (the "+ANS" columns).
   - Once LPC is interleaved, the tANS decoder is the bottleneck. In pco, tANS plus unpack alone is
     ~1 GB/s.
   - So a fast LPC + ANS codec needs both the predictor and the entropy coder interleaved.
   - I found no published codec that does both: pco interleaves only its 4-way tANS.

## Design sketch for a Vortex `LPC` encoding

- **Layout.**
  - Use blocks of 64 lanes × 1024 values. A smaller final block uses fewer lanes.
  - Store the residuals transposed, so that row `t` holds value `t` of every lane. The residual
    child cascades: BitPacked, VarBitPacked, or Pco with `NoOp` for ratio.
  - Store per lane:
    - the coefficients `q[p]`, or one shared set per block;
    - `shift`;
    - `p` warm-up values: the first as FoR over the lane starts, the rest as in-lane deltas.
- **Decode.**
  - Unpack one row of 64 residuals.
  - Add `(Σ_j q_j · row[t-p+j]) >> shift`, with i32 lanes when the width bound allows.
  - Fuse with the bit-unpack, as Affine and Delta already do on `ji/affine-encoding`.
- **Random access.**
  - `scalar_at` must replay the lane from its start, up to 1024 steps; this is the same story as
    Delta.
  - Slices are lane-aligned for free.
- **Compressor.**
  - Fit with least squares (the covariance method) on a sample. Choose the order from {4, 8, 16, 32}.
  - Accept LPC only when the cascaded size of whole sampled chunks beats Delta, delta-of-delta and
    FoR by a margin. `ChunkDeltaScheme` and `AffineScheme` on `ji/affine-encoding` already work
    this way.
  - Skip LPC when ANS-coded deltas are much smaller than bit-packed deltas, which indicates a
    peaked or few-valued alphabet.
- **Floats.** Cascade LPC after ALP: ALP's integer child is what LPC sees. CWRU vibration was
  recovered to exact ADC counts here, which is what ALP would produce.

## Caveats

- **Sizes are a model, not Vortex output.**
  - The patch and header costs approximate `BitPacked` with patches and `VarBitPacked`. The
    "+ANS" sizes use pco `NoOp` on the flattened residual stream.
  - The comparison is like for like across predictors. Absolute sizes in a real Vortex file will
    differ by the layout and metadata overhead.
- **Delta is modelled as lag-1 per chunk** (ChunkDelta / Parquet style). FastLanes Delta's
  transposed layout is about the same size or slightly larger.
- **The kernels time only the predictor inverse,** not a fused unpack-and-predict. The
  `ji/affine-encoding` notes measure FastLanes unpack at ~0.2–0.4 ns/value.
- **The VM is shared and noisy.** Treat the throughput numbers as ratios.
- **The datasets lean toward signals,** because finding LPC's wins was the point. Business and
  low-rate data are included as controls. LPC does not help there, so a production compressor
  would rarely pick it outside sensor, audio and scientific columns.
- **A few sources failed to download,** and were left out rather than substituted: one IRIS
  strong-motion channel, two IRIS stations, and a Sleep-EDF record. Three LTMM records were too
  short (<4096 values) and were skipped.

## Reproducing

```bash
python3 -m venv venv && ./venv/bin/pip install numpy scipy pcodec zstandard wfdb soundfile pyarrow obspy pyedflib
./venv/bin/python -I research/lpc/fetch.py data          # downloads into data/raw, writes data/series/*.npy
./venv/bin/python -I research/lpc/lpc_eval.py data       # appends to data/results.jsonl (resumable)
./venv/bin/python -I research/lpc/summarize.py data/results.jsonl
./venv/bin/python -I research/lpc/pco_speed.py data seis_IU_ANMO_LHZ_1hz vib_cwru_normal_12khz
rustc --edition 2024 -O -C target-cpu=native research/lpc/lpc_kernels.rs -o lpc_kernels && ./lpc_kernels
```

`results.jsonl` holds every number used here: each series' manifest row, plus the bits/value of
every predictor × coder pair.

## Full results

All sizes are bits/value.

- **Coder:** "best non-LPC" and "LPC" bit-pack per chunk with patches. "+ANS" entropy codes the
  same residuals with pco `NoOp`.
- **pco Conv1** is `nan` for 64-bit series (Conv1 supports ≤32-bit types).

| series | category | rate Hz | best non-LPC (bits) | LPC, lane-parallel (bits) | LPC gain | non-LPC+ANS | LPC+ANS | ANS gain | pco auto | pco Conv1 | Conv1 gain | zstd-19 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| seis_IU_ANMO_LDO_pressure | seismic | 1 | delta 6.09 | lpc8/chunk 4.37 | **1.39x** | 5.00 | 3.57 | 1.40x | 5.00 | 3.38 | 1.48x | 8.21 |
| seis_IU_ANMO_LHZ_1hz | seismic | 1 | delta 12.08 | lpc32/lanes 9.73 | **1.24x** | 11.25 | 8.92 | 1.26x | 11.25 | 8.96 | 1.26x | 14.26 |
| seis_IU_ANMO_LH1_1hz | seismic | 1 | dod 11.99 | lpc32/lanes 9.73 | **1.23x** | 11.10 | 9.00 | 1.23x | 11.12 | 8.97 | 1.24x | 14.34 |
| vib_cwru_normal_12khz | vibration | 12000 | dod 10.07 | lpc32/lanes 8.18 | **1.23x** | 9.07 | 7.46 | 1.22x | 9.08 | 7.36 | 1.23x | 11.56 |
| seis_IU_ANMO_BHZ_40hz | seismic | 40 | dod 6.66 | lpc32/lanes 5.46 | **1.22x** | 5.86 | 4.68 | 1.25x | 5.91 | 4.57 | 1.28x | 9.56 |
| vib_cwru_ball007_12khz | vibration | 12000 | for 12.95 | lpc32/lanes 11.06 | **1.17x** | 11.83 | 9.92 | 1.19x | 11.83 | 9.82 | 1.26x | 13.48 |
| seis_IU_ANMO_HHZ_100hz | seismic | 100 | delta 7.83 | lpc32/lanes 6.78 | **1.15x** | 7.05 | 6.03 | 1.17x | 7.05 | 5.95 | 1.19x | 10.14 |
| vib_cwru_outerrace007_12khz | vibration | 12000 | for 14.08 | lpc16/lanes 12.21 | **1.15x** | 12.33 | 10.50 | 1.18x | 12.33 | 10.35 | 1.25x | 13.76 |
| audio_esc50_env8clips | audio | 44100 | dod 10.06 | lpc16/chunk 8.79 | **1.14x** | 9.98 | 8.73 | 1.14x | 9.98 | 9.00 | 1.11x | 11.36 |
| vib_cwru_outerrace021_12khz | vibration | 12000 | for 14.33 | lpc32/lanes 12.74 | **1.12x** | 11.80 | 10.48 | 1.13x | 11.80 | 10.32 | 1.18x | 13.24 |
| audio_gtzan_classical2_120 | audio | 22050 | dod 9.84 | lpc32/lanes 8.84 | **1.11x** | 9.33 | 8.26 | 1.13x | 9.26 | 8.16 | 1.13x | 12.95 |
| seis_IU_ANMO_VHZ_0.1hz | seismic | 1/10 | delta 6.54 | lpc16/chunk 5.89 | **1.11x** | 4.87 | 4.74 | 1.03x | 4.87 | 5.87 | 0.83x | 8.10 |
| tide_seattle_6min | tides | 1/360 | dod 6.18 | lpc16/lanes 5.57 | **1.11x** | 4.08 | 4.47 | 0.91x | 4.05 | 3.55 | 1.14x | 10.31 |
| audio_gtzan_blues2_30 | audio | 22050 | dod 12.31 | lpc16/chunk 11.14 | **1.10x** | 11.45 | 10.22 | 1.12x | 11.44 | 10.16 | 1.13x | 15.21 |
| resp_fantasia_RESP | ppg_resp | 250 | delta 5.45 | lpc32/lanes 4.95 | **1.10x** | 4.85 | 3.85 | 1.26x | 4.86 | 4.06 | 1.20x | 6.51 |
| ecg_mitdb100_MLII | ecg | 360 | dod 6.28 | lpc4/lanes 5.71 | **1.10x** | 3.86 | 4.00 | 0.96x | 3.86 | 3.96 | 0.97x | 5.05 |
| eeg_chbmit_256hz_T7-P7 | eeg | 256 | dod 6.24 | lpc16/lanes 5.68 | **1.10x** | 5.07 | 4.79 | 1.06x | 5.07 | 4.73 | 1.07x | 7.69 |
| geomag_FRD_H_1hz | geomagnetic | 1 | dod 3.61 | lpc4/chunk 3.30 | **1.09x** | 2.60 | 2.48 | 1.05x | 2.60 | 2.42 | 1.07x | 6.79 |
| seis_IU_COLA_BH1_40hz | seismic | 40 | dod 6.70 | lpc16/lanes 6.13 | **1.09x** | 5.85 | 5.28 | 1.11x | 5.85 | 5.17 | 1.13x | 9.97 |
| audio_gtzan_p1_55 | audio | 22050 | dod 13.47 | lpc32/lanes 12.36 | **1.09x** | 12.61 | 11.46 | 1.10x | 12.62 | 11.30 | 1.12x | 14.98 |
| audio_gtzan_row60_60 | audio | 22050 | dod 13.80 | lpc32/lanes 12.69 | **1.09x** | 12.93 | 11.82 | 1.09x | 12.95 | 11.61 | 1.11x | 15.46 |
| audio_gtzan_row90_90 | audio | 22050 | dod 12.46 | lpc16/chunk 11.49 | **1.08x** | 11.58 | 10.58 | 1.09x | 11.62 | 10.66 | 1.09x | 14.95 |
| audio_gtzan_p1_165 | audio | 22050 | delta 13.29 | lpc32/lanes 12.26 | **1.08x** | 12.29 | 11.32 | 1.09x | 12.30 | 11.12 | 1.11x | 15.04 |
| audio_gtzan_p1_220 | audio | 22050 | dod 13.03 | lpc8/chunk 12.08 | **1.08x** | 12.01 | 11.28 | 1.06x | 12.02 | 11.35 | 1.06x | 14.97 |
| vib_cwru_innerrace007_12khz | vibration | 12000 | for 14.08 | lpc16/lanes 13.11 | **1.07x** | 12.81 | 11.56 | 1.11x | 12.81 | 11.44 | 1.16x | 14.20 |
| audio_gtzan_blues_0 | audio | 22050 | dod 12.86 | lpc32/lanes 12.00 | **1.07x** | 12.04 | 11.16 | 1.08x | 12.05 | 10.99 | 1.10x | 15.08 |
| geomag_BOU_H_1hz | geomagnetic | 1 | dod 2.94 | lpc8/chunk 2.75 | **1.07x** | 1.97 | 1.89 | 1.04x | 2.24 | 1.84 | 1.07x | 5.57 |
| audio_librispeech_speech | audio | 16000 | delta 11.70 | lpc16/chunk 10.93 | **1.07x** | 10.96 | 10.22 | 1.07x | 10.97 | 11.06 | 0.99x | 13.03 |
| audio_gtzan_p1_275 | audio | 22050 | delta 14.80 | lpc32/lanes 13.84 | **1.07x** | 13.74 | 12.80 | 1.07x | 13.75 | 12.70 | 1.08x | 15.95 |
| eeg_chbmit_256hz_FP1-F7 | eeg | 256 | dod 6.45 | lpc32/lanes 6.09 | **1.06x** | 5.67 | 5.17 | 1.10x | 5.68 | 5.15 | 1.10x | 8.26 |
| audio_gtzan_classical_150 | audio | 22050 | dod 7.30 | lpc16/lanes 6.90 | **1.06x** | 6.65 | 6.17 | 1.08x | 6.65 | 6.15 | 1.08x | 9.90 |
| bidmc01_II | ppg_resp | 125 | delta 16.01 | lpc32/lanes 15.14 | **1.06x** | 6.64 | 12.42 | 0.53x | 6.67 | 12.37 | 0.54x | 5.53 |
| seis_IU_ANMO_BDF_infrasound | seismic | 40 | delta 14.26 | lpc32/lanes 13.50 | **1.06x** | 13.34 | 12.70 | 1.05x | 13.35 | 12.45 | 1.07x | 18.78 |
| tide_thebattery_6min | tides | 1/360 | dod 6.23 | lpc4/lanes 5.90 | **1.06x** | 4.78 | 4.58 | 1.04x | 4.78 | 4.55 | 1.05x | 10.68 |
| tide_sanfrancisco_1min | tides | 1/60 | delta 7.43 | lpc16/lanes 7.06 | **1.05x** | 6.58 | 6.18 | 1.06x | 6.59 | 6.14 | 1.07x | 10.81 |
| geomag_FRD_Z_1hz | geomagnetic | 1 | delta 3.71 | lpc4/chunk 3.53 | **1.05x** | 2.49 | 2.35 | 1.06x | 2.50 | 2.28 | 1.09x | 5.17 |
| seis_IU_MAJO_BHZ_quake | seismic | 40 | dod 13.74 | lpc16/chunk 13.09 | **1.05x** | 12.93 | 12.47 | 1.04x | 12.95 | 12.20 | 1.06x | 19.47 |
| pcg_heartsound_2khz | pcg | 2000 | dod 9.99 | lpc8/chunk 9.55 | **1.05x** | 8.36 | 7.82 | 1.07x | 8.48 | 8.02 | 1.04x | 11.92 |
| tide_sanfrancisco_6min | tides | 1/360 | dod 6.58 | lpc16/lanes 6.32 | **1.04x** | 5.57 | 5.29 | 1.05x | 5.57 | 5.22 | 1.07x | 10.80 |
| ecg_mitdb100_V5 | ecg | 360 | dod 6.22 | lpc16/lanes 5.99 | **1.04x** | 3.75 | 3.93 | 0.96x | 3.75 | 3.90 | 0.96x | 5.01 |
| audio_gtzan_p1_0 | audio | 22050 | delta 13.99 | lpc8/chunk 13.47 | **1.04x** | 13.13 | 12.72 | 1.03x | 13.14 | 12.83 | 1.02x | 15.30 |
| ecg_mitdb203_MLII | ecg | 360 | dod 5.89 | lpc16/lanes 5.67 | **1.04x** | 4.64 | 4.46 | 1.04x | 4.64 | 4.43 | 1.05x | 7.14 |
| ecg_ptb1khz_v2 | ecg | 1000 | dod 8.03 | lpc32/lanes 7.75 | **1.04x** | 6.16 | 5.94 | 1.04x | 6.17 | 5.87 | 1.05x | 10.30 |
| audio_gtzan_p1_110 | audio | 22050 | dod 12.45 | lpc8/chunk 12.03 | **1.03x** | 11.50 | 11.15 | 1.03x | 11.53 | 11.52 | 1.00x | 14.75 |
| bidmc01_PLETH | ppg_resp | 125 | dod 11.14 | lpc4/lanes 10.77 | **1.03x** | 4.37 | 9.58 | 0.46x | 4.37 | 9.53 | 0.46x | 5.73 |
| imu_ltmm_fl001_base_ml-acceleration | imu | 100 | dod 15.11 | lpc8/chunk 14.68 | **1.03x** | 12.28 | 13.09 | 0.94x | 12.07 | 13.11 | 0.91x | 14.73 |
| seis_II_PFO_BHZ_40hz | seismic | 40 | dod 5.30 | lpc8/lanes 5.15 | **1.03x** | 4.40 | 4.13 | 1.07x | 4.40 | 4.00 | 1.10x | 10.15 |
| ecg_ptb1khz_i | ecg | 1000 | delta 8.27 | lpc32/lanes 8.09 | **1.02x** | 7.12 | 6.87 | 1.04x | 7.13 | 6.80 | 1.05x | 10.50 |
| emg_healthy_4khz | emg | 4000 | delta 12.85 | lpc8/chunk 12.58 | **1.02x** | 5.63 | 9.26 | 0.61x | 5.64 | 9.19 | 0.61x | 6.71 |
| imu_ltmm_fl001_base_v-acceleration | imu | 100 | dod 15.37 | lpc4/lanes 15.11 | **1.02x** | 12.27 | 13.57 | 0.90x | 12.43 | 13.56 | 0.85x | 14.96 |
| eeg_mmidb_160hz_Fc5 | eeg | 160 | delta 7.27 | lpc8/lanes 7.16 | **1.01x** | 6.37 | 6.36 | 1.00x | 6.38 | 6.34 | 1.01x | 9.34 |
| geomag_BOU_Z_1hz | geomagnetic | 1 | delta 2.75 | lpc2/chunk 2.71 | **1.01x** | 1.88 | 1.85 | 1.02x | 1.88 | 1.84 | 1.02x | 4.27 |
| weather_jena_temp_10min | weather | 1/600 | dod 8.16 | lpc4/lanes 8.05 | **1.01x** | 6.34 | 6.16 | 1.03x | 6.34 | 6.13 | 1.03x | 10.15 |
| weather_jena_humidity_10min | weather | 1/600 | delta 10.47 | lpc4/lanes 10.34 | **1.01x** | 7.33 | 8.48 | 0.86x | 7.16 | 6.92 | 1.03x | 9.68 |
| weather_jena_pressure_10min | weather | 1/600 | delta 6.45 | lpc8/lanes 6.37 | **1.01x** | 5.14 | 5.00 | 1.03x | 5.15 | 5.02 | 1.02x | 8.42 |
| power_household_global_intensity_1min | power | 1/60 | delta 6.39 | lpc16/lanes 6.31 | **1.01x** | 2.50 | 3.06 | 0.82x | 2.50 | 2.86 | 0.88x | 2.96 |
| seis_IU_ANMO_LWS_windspeed | seismic | 1 | delta 4.79 | lpc8/lanes 4.75 | **1.01x** | 3.75 | 3.73 | 1.01x | 3.75 | 3.72 | 1.01x | 4.51 |
| eeg_mmidb_160hz_Cz | eeg | 160 | delta 7.17 | lpc4/lanes 7.11 | **1.01x** | 6.26 | 6.24 | 1.00x | 6.27 | 6.22 | 1.01x | 9.31 |
| bidmc01_RESP | ppg_resp | 125 | dod 10.75 | lpc4/lanes 10.67 | **1.01x** | 3.60 | 7.68 | 0.47x | 3.60 | 7.21 | 0.50x | 7.28 |
| weather_jena_wind_10min | weather | 1/600 | delta 9.14 | lpc8/lanes 9.08 | **1.01x** | 7.84 | 7.81 | 1.00x | 7.85 | 7.81 | 1.00x | 10.02 |
| power_household_global_active_power_1min | power | 1/60 | delta 10.97 | lpc16/lanes 10.91 | **1.01x** | 6.14 | 7.26 | 0.85x | 6.15 | 7.00 | 0.88x | 8.00 |
| seis_IU_SNZO_HH1_100hz | seismic | 100 | dod 2.03 | lpc32/lanes 2.02 | **1.00x** | 1.60 | 1.61 | 0.99x | 1.59 | 1.64 | 0.97x | 1.96 |
| imu_ltmm_fl001_base_ap-acceleration | imu | 100 | dod 15.13 | lpc2/chunk 15.10 | **1.00x** | 12.04 | 13.38 | 0.90x | 12.34 | 13.38 | 0.87x | 14.75 |
| power_household_voltage_1min | power | 1/60 | delta 9.33 | lpc8/lanes 9.32 | **1.00x** | 7.88 | 7.88 | 1.00x | 7.89 | 7.87 | 1.00x | 10.23 |
| seis_IU_ANMO_LFZ_magnetic | seismic | 1 | delta 4.08 | lpc16/lanes 4.08 | **1.00x** | 3.11 | 2.90 | 1.07x | 3.12 | 2.87 | 1.09x | 5.13 |
| ecg_apnea100hz_ECG | ecg | 100 | for 8.81 | lpc32/lanes 8.87 | **0.99x** | 5.60 | 6.19 | 0.90x | 5.60 | 6.17 | 0.91x | 6.68 |
| emg_neuropathy_4khz | emg | 4000 | for 15.50 | lpc8/chunk 15.60 | **0.99x** | 7.64 | 11.57 | 0.66x | 6.29 | 7.51 | 0.84x | 8.35 |
| fin_btcusdt_trade_price | financial |  | delta 7.86 | lpc8/chunk 7.92 | **0.99x** | 2.90 | 3.97 | 0.73x | 2.91 | 3.17 | 0.92x | 3.99 |
| seis_IU_ANMO_LKO_temp | seismic | 1 | delta 1.21 | lpc4/lanes 1.22 | **0.99x** | 0.15 | 0.17 | 0.92x | 0.15 | 0.16 | 0.94x | 0.23 |
| fin_btcusdt_trade_time | timestamp |  | delta 9.47 | lpc8/chunk 10.48 | **0.90x** | 4.97 | 7.68 | 0.65x | 4.98 | nan | nanx | 7.65 |
