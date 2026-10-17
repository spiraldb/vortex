# LPC (linear predictive coding) for Vortex integer columns: where it wins, and what it costs to decode

LPC predicts each value from the previous `p` values, `x[i] ≈ (Σ_k q_k · x[i-k]) >> shift`, and
stores the residuals. This study asks two questions:

1. Which real datasets need LPC, because Delta, delta-of-delta and the per-chunk Affine/LeCo model
   from `ji/affine-encoding` are not enough?
2. How fast can LPC decode, given that the predictor is a loop-carried dependency? Does an
   interleaving trick like interleaved rANS help?

The evidence:

- **Data:** 85 real series in 16 categories, 37M values, all downloaded from public sources by
  `fetch.py`.
- **Size model:** `lpc_eval.py`.
- **Real Vortex sizes:** `affine_eval` from `ji/affine-encoding`, run on each column and on its LPC
  residuals (`export_vortex.py`, `vortex_compare.py`).
- **Encoder ablations:** `ablate.py`.
- **Decode kernels in Rust:** `lpc_kernels.rs`, including fused AVX-512 unpack + predict.
- **pco's own LPC mode** as a reference.
- **A literature review:** `LITERATURE.md`.

## Summary

### Sizes

**LPC wins on high-rate, band-limited physical waveforms:** vibration, seismic, audio, tides,
geomagnetic, ECG/EEG/respiration and power-grid current/voltage.

| Category (series) | LPC vs Delta | LPC vs delta-of-delta |
|---|---|---|
| Vibration (5) | 1.19x | 1.22x |
| Seismic (14) | 1.15x (max 1.39x) | 1.18x |
| Audio (14) | 1.10x | 1.09x |
| Grid waveforms (9) | 1.09x | 1.12x |

- **Against the best non-LPC predictor** (any of FoR, Delta, delta-of-delta, Affine), LPC is
  ≥5% smaller on 40 of 85 series and ≥10% smaller on 18. These are geo-mean size ratios from the
  model, with every scheme bit-packed the same way.
- **Affine never wins on any series.**
- **LPC does nothing for business and low-rate data.** That covers timestamps, prices,
  1-minute power, 10-minute weather and 64–100 Hz body-worn accelerometers.

**Real Vortex output agrees, and is often more favourable.** The comparison is:

- *LPC side:* Vortex's own compressor on the LPC residuals, plus the LPC headers.
- *Baseline:* the best of Vortex `default` and `affine+varbp+cdelta` on the original column.

LPC is ≥5% smaller on **47 of 85** series and ≥10% smaller on **29**, with every ECG, vibration,
tide and geomagnetic series smaller. Part of that margin comes from the BtrBlocks compressor
choosing worse trees than Delta on several originals (see "Real Vortex sizes").

**Bit-packed LPC needs no entropy coder to beat pco on oscillating data.** It is smaller than pco
with default settings (`DeltaSpec::Auto`) on 9 of 14 audio, 5 of 14 seismic and 3 of 5 vibration
series. pco Auto never picks its LPC mode (Conv1).

**With the same recursive compression on both sides, LPC beats cascaded Delta by** 1.19x on
vibration, 1.16x on geomagnetic, 1.08x on ECG and tides, and 1.05x on seismic and audio (geo-means;
up to 1.36x). It is ≥5% smaller than the better of cascaded Delta and delta-of-delta on 34 of 85
series. See "LPC vs Delta, both with the full Vortex cascade".

**LPC must not be picked blindly.**

- On data with few distinct values or with dictionary/run-length structure, LPC residuals destroy
  the structure. Examples are PPG, EMG, IMU and timestamps.
- In real Vortex those columns get 0.24–0.9x, i.e. LPC is larger.
- A scheme must compare full cascaded sizes, as `ChunkDeltaScheme` and `AffineScheme` already do.

**The show-stopper dataset is high-pitched real acoustic notes.**

- On NSynth notes at ≥72 MIDI pitch (≤31 samples per cycle), LPC + cascade is up to 1.91x smaller
  than cascaded Delta on a 1M-value column of evenly sampled notes.
- On the best single notes it is 2.5–3.4x smaller.
- Delta and pco there are no better than storing the raw values.

See "The show stopper".

### Decode speed

**Your hypothesis is right.** The serial dependency makes LPC decoding slow:

- **pco Conv1** on real data takes **16.6–18.4 ns/value**, against **2.2–2.6** for its delta
  modes.
- **Serial integer LPC** takes **~6–11 ns/value** at orders 4–32, even with the history kept in
  registers.
- **The literature agrees.**
  - Sprintz: "the limiting factor is serial dependence".
  - The pco docs leave Conv1 out of Auto because of its "decompression performance costs".
  - libFLAC removed its decoder SIMD.

**The interleaving trick from interleaved rANS removes the bottleneck, and is bit-exact.** Store
each block as independent lanes, each a contiguous run of 1024 values.

| Kernel (AVX-512, 2.1 GHz Xeon VM) | Order 4 | Order 8 | Order 16 | Order 32 |
|---|---|---|---|---|
| Predictor only, 16–128 lanes | 0.26–0.41 | 0.39–0.49 | 0.81–1.08 | 1.63–1.78 |
| Fused bit-unpack + predict, lane-interleaved output | 0.29 | 0.47 | 0.69 | 1.41 |
| Fused bit-unpack + predict + in-register 16×16 transpose to original order | 0.64 | 0.88 | 1.13 | 1.96 |

All values are ns/value. The last row is the realistic end-to-end decode. At order 4 it matches
FastLanes Delta in Vortex today (0.6–0.76 ns/value, per the `ji/affine-encoding` notes), and it is
9–26x faster than pco Conv1.

### Encoder

The encoder is cheap and simple:

- **Sampling:** fitting coefficients on a 1/16 sample of lanes costs ≤0.08 bits/value.
- **Coefficient precision:** 12-bit (i16) coefficients cost ≤0.13 bits/value against 15-bit on
  all but one series (0.4). 8-bit coefficients cost up to 2 bits/value.
- **Accumulator:** with i32 wrapping arithmetic in both encoder and decoder the result is always
  lossless. The accumulator stayed under 2^31 on every winning series but one (IU.ANMO LH1, 2^31.5
  with 15-bit coefficients), and 12-bit coefficients leave about 3 bits more headroom.

### ANS

"ANS-style" helps in two different ways.

1. **The interleaving trick** is the decode-speed fix above.
2. **Entropy coding the residuals** is worth another ~0.6–1 bit/value over bit-packing.
   - The cost is that tANS then becomes the bottleneck: pco's unpack and tANS alone run at about
     1 GB/s.
   - No published codec interleaves both the predictor and the ANS coder.

### Recommendation

Build `fastlanes.lpc` as a lane-interleaved encoding:

- 16-lane AVX-512 rows, each lane 1024 values;
- i16 coefficients, per lane or per block;
- orders 4–16, with 32 only when it pays;
- a fused unpack + predict + transpose decode;
- residuals cascading into BitPacked (or Pco/ANS for ratio).

Gate it on the cascaded sample size against Delta, delta-of-delta and FoR. It belongs in sensor,
audio, scientific and grid-telemetry workloads, and will rarely fire on TPC-H or ClickBench style
columns.

## The show stopper: high-pitched acoustic notes (NSynth)

**Physics.** LPC beats Delta by the largest margin on a high-SNR tonal signal with few samples per
cycle.

- Delta-of-delta leaves `(2 − 2cos ω)·A` of a sinusoid. At 2–8 samples per cycle that is 1–4×
  the amplitude itself, so Delta does *worse* than storing the raw values.
- LPC of order 2 already predicts a pure sinusoid exactly.

**Data.** The NSynth test set (Magenta, CC BY 4.0, 4,096 four-second notes, 16 kHz 16-bit PCM,
`fetch.py nsynth` / `nsynth_high`) matches that. I used only its 1,689 *acoustic* notes: samples
of real instruments, not synthesizers.

**Gain by pitch, over every acoustic note** (`nsynth_notes.py`). Each note is modelled as in
`lpc_eval.py`. The gain is against the better of Delta and delta-of-delta.

|---|---|---|---|---|---|---|
| 12-23 | 23 Hz | 692 | 20 | 1.15x | 1.17x | 0 |
| 24-35 | 46 Hz | 346 | 200 | 1.11x | 1.13x | 0 |
| 36-47 | 92 Hz | 173 | 262 | 1.11x | 1.14x | 0 |
| 48-59 | 185 Hz | 86 | 279 | 1.15x | 1.16x | 0 |
| 60-71 | 370 Hz | 43 | 314 | 1.19x | 1.22x | 6 |
| 72-83 | 740 Hz | 22 | 250 | 1.38x | 1.42x | 40 |
| 84-95 | 1480 Hz | 11 | 193 | 1.38x | 1.39x | 36 |
| 96-107 | 2960 Hz | 5 | 156 | 1.21x | 1.37x | 26 |
| 108-119 | 5920 Hz | 3 | 15 | 2.27x | 1.85x | 8 |

**Selection.** To avoid cherry-picking, `nsynth_high` takes every 10th acoustic note at pitch ≥72
and every 3rd at pitch ≥96 (by file name). It concatenates them into 16-note, 1,024,000-sample
columns. The real Vortex cascade (`cascade_compare.py`, `vortex_nsynth_results.tsv`) gives these
sizes in bits/value:

| column | Vortex today (best tree on raw values) | Delta + cascade | DoD + cascade | LPC + cascade | LPC vs cascaded Delta | vs Vortex today |
|---|---|---|---|---|---|---|
| `audio_nsynth_pitch72plus_1` | 4.47 | 4.20 | 4.01 | **2.20** | **1.91x** | **2.03x** |
| `audio_nsynth_pitch96plus_1` | 2.30 | 2.36 | 2.61 | **1.52** | **1.55x** | 1.51x |
| `audio_nsynth_pitch72plus_2` | 5.64 | 4.48 | 4.68 | **3.05** | **1.47x** | 1.85x |
| `audio_nsynth_pitch96plus_0` | 14.09 | 14.61 | 14.80 | 11.87 | 1.23x | 1.19x |
| `audio_nsynth_pitch72plus_0` | 13.42 | 13.61 | 13.82 | 11.27 | 1.21x | 1.19x |
| `audio_nsynth_pitch96plus_2` | 3.34 | 2.75 | 2.89 | 2.39 | 1.15x | 1.40x |
| acoustic keyboard, first 24 notes (all pitches) | 9.49 | 6.86 | 5.79 | 5.18 | 1.33x | 1.83x |
| acoustic flute, first 24 notes | 12.58 | 12.08 | 12.15 | 10.08 | 1.20x | 1.25x |
| brass / guitar / mallet / vocal / reed / string | 3.9–11.7 | 4.3–11.5 | 4.7–11.6 | 4.0–11.1 | 1.02–1.15x | 1.0–1.5x |

**Best single notes** (picked as the best of 1,689, so read these as the ceiling):

| note | Vortex today | Delta + cascade | LPC + cascade | LPC vs cascaded Delta |
|---|---|---|---|---|
| `mallet_acoustic_056-104-127` | 10.39 | 10.79 | **3.18** | **3.39x** |
| `mallet_acoustic_056-110-050` | 12.68 | 13.39 | **4.56** | **2.94x** |
| `guitar_acoustic_010-097-025` | 9.49 | 9.39 | **3.58** | **2.62x** |
| `guitar_acoustic_010-102-100` | 8.91 | 9.15 | **3.64** | **2.51x** |

**Takeaways.**

- **Delta makes these columns bigger than the raw values,** and Vortex today cannot do better
  than ~raw. LPC of order 8–16 cuts them by 2–3.4x per note, and by up to 2x on a 1M-value column
  of evenly sampled real high-register notes.
- **pco is no rescue.** pco Auto is *worse* than bit-packed LPC on every one of these columns,
  e.g. 11.58 vs 2.88 bits/value on the best mallet note.
- **The same physics applies outside music:** any high-SNR narrowband signal sampled at a few to
  a few tens of points per cycle. Examples are gear-mesh and bearing tones in vibration (CWRU,
  1.2x), undersampled tones, and IF/IQ captures.
- **Caveat on provenance.** NSynth's acoustic notes come from commercial sample libraries that
  Magenta resampled to 16 kHz and stored as int16. They are real recordings, not synthesized, but
  they are curated single notes. A polyphonic mix of the same instruments would gain less (cf.
  GTZAN, 1.03–1.14x).

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

- **Best non-LPC** is the minimum over `for`, `delta`, `dod` and `affine` for each series.
- **`lpc`** is the minimum over orders 2–32 of both LPC layouts. Both layouts decode with the
  lane-parallel kernel: `chunk` gives every lane its own weights.
- **"+ANS" columns** measure each predictor with its residuals entropy coded instead of
  bit-packed (pco with `DeltaSpec::NoOp`, which is tANS-based).
- **Other references:**
  - pco level 8 with `Auto`;
  - the best of pco's consecutive orders 1–3;
  - the best of pco's `TryConv1` orders 2–32;
  - zstd levels 3 and 19.
- **GCD:** every series is divided by its GCD first.
- **Floats** (PLAID, CWRU, ADMIT) are converted to exact integers. PLAID uses its decimal digits,
  as ALP would. CWRU and ADMIT use their ADC quantum, recovered with max error ≤1e-8.

## Results by category (model)

Each category is ordered by geo-mean gain of bit-packed LPC over the best non-LPC lightweight
scheme. The "+ANS" column is the same ratio when both sides entropy code their residuals.

| category | series | geo-mean LPC gain (bit-packed) | min..max | geo-mean LPC gain (ANS residuals) | LPC beats pco auto in |
|---|---|---|---|---|---|
| vibration | 5 | 1.15x | 1.07..1.23 | 1.16x | 3/5 |
| seismic | 14 | 1.11x | 0.99..1.39 | 1.11x | 5/14 |
| audio | 14 | 1.08x | 1.03..1.14 | 1.09x | 9/14 |
| grid | 9 | 1.07x | 1.00..1.21 | 0.83x | 1/9 |
| tides | 4 | 1.06x | 1.04..1.11 | 1.02x | 0/4 |
| geomagnetic | 4 | 1.06x | 1.01..1.09 | 1.04x | 0/4 |
| ppg_resp | 4 | 1.05x | 1.01..1.10 | 0.62x | 0/4 |
| pcg | 1 | 1.05x | 1.05..1.05 | 1.07x | 0/1 |
| eeg | 4 | 1.04x | 1.01..1.10 | 1.04x | 0/4 |
| ecg | 6 | 1.04x | 0.99..1.10 | 0.99x | 0/6 |
| imu | 8 | 1.01x | 1.00..1.03 | 0.77x | 0/8 |
| weather | 4 | 1.01x | 1.01..1.01 | 0.98x | 0/4 |
| emg | 2 | 1.01x | 0.99..1.02 | 0.63x | 0/2 |
| power | 3 | 1.01x | 1.00..1.01 | 0.88x | 0/3 |
| financial | 1 | 0.99x | 0.99..0.99 | 0.73x | 0/1 |
| timestamp | 2 | 0.93x | 0.90..0.96 | 0.79x | 1/2 |


LPC compared directly with Delta and with delta-of-delta (model, geo-mean, bit-packed):

| category | series | LPC vs Delta | max | LPC vs delta-of-delta |
|---|---|---|---|---|
| tides | 4 | 1.20x | 1.45x | 1.09x |
| vibration | 5 | 1.19x | 1.23x | 1.22x |
| ecg | 6 | 1.16x | 1.30x | 1.07x |
| seismic | 14 | 1.15x | 1.39x | 1.18x |
| ppg_resp | 4 | 1.12x | 1.21x | 1.08x |
| pcg | 1 | 1.11x | 1.11x | 1.05x |
| audio | 14 | 1.10x | 1.21x | 1.09x |
| geomagnetic | 4 | 1.09x | 1.19x | 1.10x |
| grid | 9 | 1.09x | 1.21x | 1.12x |
| eeg | 4 | 1.07x | 1.14x | 1.09x |
| imu | 8 | 1.04x | 1.05x | 1.07x |
| emg | 2 | 1.02x | 1.02x | 1.05x |
| weather | 4 | 1.01x | 1.02x | 1.04x |
| power | 3 | 1.01x | 1.01x | 1.06x |
| financial | 1 | 0.99x | 0.99x | 1.05x |
| timestamp | 2 | 0.92x | 0.93x | 1.40x |

### Why LPC wins where it does

- **Oscillating signals.** Seismic, vibration, audio and grid data are sums of resonances, plus
  noise.
  - Delta and delta-of-delta are fixed polynomial predictors, which fit only slow, smooth motion.
  - On vibration at 12 kHz, deltas are *wider* than the raw values, so FoR is the best non-LPC
    scheme there.
  - LPC learns the resonances, and orders 16–32 are chosen most often.
- **Long-period seismic channels (LHZ / LH1 at 1 Hz).** These are dominated by the microseism
  (a 5–10 s oscillation), and LPC saves 2.3 bits/value there (1.24x).
- **Non-stationary signals** (barometric pressure, geomagnetic field, speech). These prefer one
  model per 1024-value chunk over one per 64K block. The barometer gains 1.39x with per-chunk
  models but only 1.11x with one model per block.
- **Grid waveforms gain less than expected.** A 50/60 Hz sinusoid sampled at 30–500 kHz is so
  smooth that delta-of-delta already predicts it well. What is left is mostly ADC noise and
  appliance switching transients. LPC still wins on current waveforms (up to 1.21x), where
  harmonics matter.

### Where LPC does not help, or hurts

- **Slow, low-rate or step-like data.** Examples are weather at 10 min, household power at 1 min,
  tides at 6 min, IU.ANMO temperature and wind, financial prices, and timestamps.
  - Delta already captures almost all of the predictable structure here, and LPC's coefficients
    and warm-ups are pure overhead.
  - These are the kinds of columns in the pco, ALP and Chimp benchmarks, and in TPC-H and
    ClickBench.
- **Peaked or quantized distributions.** BIDMC PPG/ECG, EMG, MIT-BIH ECG, Daphnet and LTMM
  accelerometers fall here.
  - Entropy coding the plain deltas gives 2–9 bits/value less than bit-packing them.
  - LPC's rounding spreads a few-valued delta alphabet into many residual values, so "LPC+ANS"
    is worse than "delta+ANS" (0.5–0.97x).
  - Bit width alone would choose LPC here and be wrong.

## Real Vortex sizes

**Method.**

- `export_vortex.py` writes each GCD-reduced series, and the residual stream of its best
  lane-parallel LPC variant, as raw i64 columns.
- `affine_eval` from `ji/affine-encoding` (commit `ef6e698d6`) compresses both with two real
  BtrBlocks configurations: `default`, and `affine+varbp+cdelta` (Affine + VarBitPacked +
  ChunkDelta, the strongest non-LPC set on that branch).
- `vortex_compare.py` adds the LPC header bits (coefficients and warm-ups, from
  `vortex_lpc_headers.tsv`) to the best residual tree. It then compares that total against the
  best tree for the original column.
- The raw output is in `vortex_results.tsv`.

**Results.**

- LPC is ≥5% smaller on 47 of 85 series and ≥10% smaller on 29.
- It loses where the original has structure that BtrBlocks exploits and that LPC residuals
  destroy: `dict`, `runend` and `sequence` trees on timestamps, IMU, PPG and EMG.
- A scheme would simply not pick LPC there.
- **The real gains are larger than the model's on ECG, tides and geomagnetic data,** because the
  BtrBlocks compressor misses deltas on those originals.
  - Example: `tide_seattle_6min` gets `chunk_delta(primitive, …)` at 8.12 bits/value, where the
    model's bit-packed delta needs 6.18.
  - A better non-LPC cascade would close part of that gap, so treat the model's ratios as the
    conservative ones.

## LPC vs Delta, both with the full Vortex cascade

This is the like-for-like comparison: every predictor's residual stream goes through the same
recursive BtrBlocks compression.

- **Inputs:** `export_vortex.py` also writes each series' per-chunk Delta residuals and
  delta-of-delta residuals.
- **Compression:** `affine_eval` compresses every residual column with `default` and
  `affine+varbp+cdelta`, and keeps the better result. The cascade may pick FoR, bit-packing,
  dict, run-end, RLE, ChunkDelta, Affine and so on, recursively.
- **Headers:**
  - Delta pays for the first value of each chunk, FoR-coded across chunks.
  - Delta-of-delta also pays for its warm-ups.
  - LPC pays for its coefficients and warm-ups.
- `cascade_compare.py` builds the tables. The raw output is in `vortex_results.tsv` and
  `vortex_fixed_results.tsv`.

| category | series | LPC vs Delta | min..max | LPC vs best of Delta, DoD | LPC smaller than both in |
|---|---|---|---|---|---|
| vibration | 5 | 1.19x | 1.13..1.23 | 1.18x | 5/5 |
| geomagnetic | 4 | 1.16x | 1.01..1.27 | 1.06x | 3/4 |
| ecg | 6 | 1.08x | 1.02..1.22 | 1.03x | 6/6 |
| tides | 4 | 1.08x | 1.04..1.15 | 1.05x | 4/4 |
| pcg | 1 | 1.06x | 1.06..1.06 | 1.05x | 1/1 |
| seismic | 14 | 1.05x | 0.59..1.36 | 1.05x | 11/14 |
| audio | 14 | 1.05x | 0.99..1.09 | 1.05x | 12/14 |
| weather | 4 | 1.02x | 1.00..1.04 | 1.02x | 4/4 |
| power | 3 | 1.01x | 1.00..1.03 | 1.01x | 3/3 |
| eeg | 4 | 1.01x | 1.00..1.02 | 1.01x | 2/4 |
| imu | 8 | 0.98x | 0.92..1.17 | 0.98x | 2/8 |
| timestamp | 2 | 0.96x | 0.94..0.98 | 0.96x | 0/2 |
| financial | 1 | 0.93x | 0.93..0.93 | 0.93x | 0/1 |
| grid | 9 | 0.87x | 0.30..1.17 | 0.86x | 5/9 |
| emg | 2 | 0.80x | 0.79..0.81 | 0.80x | 0/2 |
| ppg_resp | 4 | 0.80x | 0.66..1.19 | 0.80x | 1/4 |

series 85: LPC >=5% smaller than cascaded Delta in 40, >=10% in 21; than the better of Delta/DoD: >=5% in 34, >=10% in 15.

- **The gap is smaller than in the "best Vortex on the original" comparison.**
  - That comparison credited LPC for trees where BtrBlocks missed the delta. Example: `tide_seattle`
    was 8.12 bits/value on the original column, but 6.53 with an explicit Delta + cascade.
  - Against cascaded Delta, LPC keeps a clear win on vibration (1.19x), geomagnetic (1.16x), ECG
    and tides (1.08x), and seismic and audio (1.05x geo-mean, up to 1.36x).
- **LPC loses wherever cascaded deltas find a dictionary.**
  - Deltas of a quantized or upsampled signal take few distinct values, and `dict(bitpacked, …)`
    exploits that (PPG, EMG, PLAID, some IMU).
  - LPC's rounded prediction spreads those values out, so the dictionary no longer applies.
  - The same happens with run-end structure: `seis_IU_SNZO_HH1_100hz` is a mostly flat channel
    where `rle` + run-end wins.
- **Two of the worst grid rows are artifacts:** `grid_plaid_1816_current` (0.30x) and
  `1005_voltage` (0.59x) are the decimal-scaling artifacts noted in Caveats.
- **The selection rule is the same as before:** LPC only pays when its own cascaded sample beats
  cascaded Delta/delta-of-delta. Such a rule would pick LPC for 34 of these 85 series, at a ≥5%
  gain.

<details><summary>Per-series table</summary>

| series | category | original, best Vortex | Delta + cascade (tree) | DoD + cascade | LPC + cascade (tree) | vs Delta | vs best fixed |
|---|---|---|---|---|---|---|---|
| seis_IU_ANMO_LDO_pressure | seismic | 6.15 | 6.15 `zigzag(bitpacked(primitive,primitive,primitive))` | 6.11 | 4.53 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.36x** | 1.35x |
| geomag_BOU_H_1hz | geomagnetic | 3.48 | 4.04 `zigzag(bitpacked(primitive,primitive,primitive))` | 3.07 | 3.17 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.27x** | 0.97x |
| vib_cwru_normal_12khz | vibration | 10.09 | 10.11 `zigzag(bitpacked(primitive,primitive,primitive))` | 10.05 | 8.21 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.23x** | 1.22x |
| ecg_mitdb100_V5 | ecg | 7.03 | 7.06 `zigzag(bitpacked(primitive,primitive,primitive))` | 6.16 | 5.79 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.22x** | 1.06x |
| geomag_FRD_H_1hz | geomagnetic | 4.42 | 4.13 `zigzag(bitpacked(primitive,primitive,primitive))` | 3.87 | 3.39 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.22x** | 1.14x |
| vib_cwru_outerrace007_12khz | vibration | 14.03 | 15.08 `v2(bitpacked,primitive)` | 15.25 | 12.39 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.22x** | 1.22x |
| seis_IU_ANMO_LHZ_1hz | seismic | 12.30 | 12.38 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 12.58 | 10.40 lpc32/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.19x** | 1.19x |
| vib_cwru_outerrace021_12khz | vibration | 14.45 | 15.10 `zigzag(bitpacked(primitive,primitive,primitive))` | 15.59 | 12.71 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.19x** | 1.19x |
| resp_fantasia_RESP | ppg_resp | 6.02 | 6.02 `v2(bitpacked,dict(bitpacked,primitive))` | 6.46 | 5.07 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.19x** | 1.19x |
| seis_IU_ANMO_LH1_1hz | seismic | 12.34 | 12.27 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 12.45 | 10.40 lpc32/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.18x** | 1.18x |
| imu_daphnet_S03R01_trunk_vert_64hz | imu | 9.03 | 10.26 `zigzag(bitpacked(primitive,primitive,primitive))` | 10.48 | 8.74 lpc2/chunk `zigzag(varbitpacked)` | **1.17x** | 1.17x |
| grid_plaid_1464_current_30khz | grid | 5.27 | 5.04 `zigzag(bitpacked(primitive,primitive,primitive))` | 5.19 | 4.31 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.17x** | 1.17x |
| vib_cwru_ball007_12khz | vibration | 13.06 | 13.09 `zigzag(bitpacked(primitive,primitive,primitive))` | 14.08 | 11.20 lpc32/lanes `v2(bitpacked,primitive)` | **1.17x** | 1.17x |
| seis_IU_ANMO_BHZ_40hz | seismic | 7.00 | 7.03 `zigzag(bitpacked(primitive,primitive,primitive))` | 7.07 | 6.08 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.16x** | 1.16x |
| seis_IU_ANMO_HHZ_100hz | seismic | 8.18 | 8.29 `zigzag(bitpacked(primitive,primitive,primitive))` | 8.92 | 7.17 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.16x** | 1.16x |
| tide_seattle_6min | tides | 8.12 | 6.53 `chunk_delta(varbitpacked,primitive,primitive)` | 6.18 | 5.67 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.15x** | 1.09x |
| geomag_FRD_Z_1hz | geomagnetic | 4.09 | 3.99 `zigzag(bitpacked(primitive,primitive,primitive))` | 4.27 | 3.53 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.13x** | 1.13x |
| vib_cwru_innerrace007_12khz | vibration | 14.09 | 14.84 `zigzag(bitpacked(primitive,primitive,primitive))` | 15.12 | 13.17 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.13x** | 1.13x |
| seis_IU_COLA_BH1_40hz | seismic | 7.37 | 7.63 `zigzag(bitpacked(primitive,primitive,primitive))` | 7.03 | 6.79 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.12x** | 1.03x |
| ecg_mitdb100_MLII | ecg | 7.59 | 6.64 `chunk_delta(bitpacked(primitive,primitive,primitive),v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 6.09 | 6.02 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.10x** | 1.01x |
| grid_plaid_1312_current_30khz | grid | 7.64 | 7.79 `zigzag(chunk_delta(bitpacked(primitive,primitive,primitive),bitpacked(primitive,primitive,primitive),v2(bitpacked,primitive)))` | 6.97 | 7.07 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.10x** | 0.98x |
| seis_IU_ANMO_VHZ_0.1hz | seismic | 6.98 | 7.58 `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 8.33 | 6.91 lpc16/chunk `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | **1.10x** | 1.10x |
| audio_gtzan_row60_60 | audio | 14.16 | 14.12 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 14.14 | 12.99 lpc32/lanes `zigzag(varbitpacked)` | **1.09x** | 1.09x |
| audio_gtzan_p1_165 | audio | 13.61 | 13.69 `zigzag(varbitpacked)` | 13.92 | 12.60 lpc32/lanes `zigzag(varbitpacked)` | **1.09x** | 1.09x |
| grid_plaid_1005_current_30khz | grid | 6.48 | 6.01 `chunk_delta(varbitpacked,primitive,primitive)` | 5.94 | 5.53 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.09x** | 1.07x |
| audio_gtzan_classical2_120 | audio | 10.88 | 10.08 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 9.98 | 9.35 lpc32/lanes `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | **1.08x** | 1.07x |
| tide_thebattery_6min | tides | 7.38 | 6.60 `chunk_delta(varbitpacked,primitive,primitive)` | 6.23 | 6.13 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.08x** | 1.02x |
| audio_esc50_env8clips | audio | 10.29 | 10.16 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 10.31 | 9.44 lpc16/chunk `chunk_delta(varbitpacked,bitpacked(primitive,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | **1.08x** | 1.08x |
| grid_plaid_788_current_30khz | grid | 6.98 | 6.13 `zigzag(bitpacked(primitive,primitive,primitive))` | 6.08 | 5.70 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.08x** | 1.07x |
| ecg_ptb1khz_v2 | ecg | 9.60 | 8.16 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,primitive)` | 7.80 | 7.59 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.08x** | 1.03x |
| audio_gtzan_p1_55 | audio | 13.75 | 13.80 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 14.12 | 12.91 lpc32/lanes `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | **1.07x** | 1.07x |
| audio_gtzan_p1_275 | audio | 15.01 | 15.05 `zigzag(bitpacked(primitive,primitive,primitive))` | 15.27 | 14.08 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.07x** | 1.07x |
| grid_admit_voltage_500khz | grid | 7.70 | 8.04 `v2(bitpacked,dict(primitive,primitive))` | 8.79 | 7.53 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.07x** | 1.07x |
| grid_plaid_759_current_30khz | grid | 3.64 | 3.06 `zigzag(bitpacked(primitive,primitive,primitive))` | 3.07 | 2.87 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.07x** | 1.07x |
| pcg_heartsound_2khz | pcg | 11.09 | 10.49 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 10.39 | 9.85 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.06x** | 1.05x |
| seis_II_PFO_BHZ_40hz | seismic | 7.09 | 5.59 `chunk_delta(varbitpacked,zigzag(bitpacked(primitive,primitive,primitive)),dict(bitpacked,primitive))` | 5.69 | 5.27 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.06x** | 1.06x |
| audio_gtzan_blues2_30 | audio | 12.59 | 12.63 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 13.27 | 11.89 lpc16/chunk `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.06x** | 1.06x |
| tide_sanfrancisco_6min | tides | 7.43 | 6.99 `chunk_delta(varbitpacked,primitive,primitive)` | 6.98 | 6.62 lpc16/lanes `zigzag(varbitpacked)` | **1.06x** | 1.05x |
| audio_librispeech_speech | audio | 12.04 | 12.58 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 13.36 | 11.92 lpc16/chunk `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | **1.06x** | 1.06x |
| ecg_apnea100hz_ECG | ecg | 9.22 | 9.58 `zigzag(bitpacked(primitive,primitive,primitive))` | 9.89 | 9.07 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.06x** | 1.06x |
| audio_gtzan_blues_0 | audio | 13.24 | 13.20 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 13.59 | 12.58 lpc32/lanes `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.05x** | 1.05x |
| weather_jena_pressure_10min | weather | 7.00 | 6.78 `zigzag(bitpacked(primitive,primitive,primitive))` | 6.77 | 6.50 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.04x** | 1.04x |
| audio_gtzan_row90_90 | audio | 12.79 | 12.85 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 13.36 | 12.32 lpc16/chunk `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.04x** | 1.04x |
| seis_IU_ANMO_BDF_infrasound | seismic | 14.42 | 14.73 `zigzag(bitpacked(primitive,primitive,primitive))` | 15.35 | 14.18 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.04x** | 1.04x |
| tide_sanfrancisco_1min | tides | 7.77 | 7.79 `zigzag(varbitpacked)` | 9.03 | 7.51 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.04x** | 1.04x |
| audio_gtzan_p1_220 | audio | 13.53 | 13.36 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 13.58 | 12.92 lpc8/chunk `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.03x** | 1.03x |
| ecg_mitdb203_MLII | ecg | 7.22 | 6.32 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 6.35 | 6.14 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.03x** | 1.03x |
| power_household_global_intensity_1min | power | 7.36 | 7.00 `zigzag(bitpacked(primitive,primitive,primitive))` | 7.17 | 6.83 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.03x** | 1.03x |
| eeg_chbmit_256hz_FP1-F7 | eeg | 7.12 | 6.80 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 7.37 | 6.66 lpc32/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.02x** | 1.02x |
| ecg_ptb1khz_i | ecg | 8.42 | 8.28 `zigzag(bitpacked(primitive,primitive,primitive))` | 8.72 | 8.12 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.02x** | 1.02x |
| seis_IU_MAJO_BHZ_quake | seismic | 14.53 | 14.19 `chunk_delta(varbitpacked,primitive,primitive)` | 14.52 | 13.93 lpc16/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **1.02x** | 1.02x |
| weather_jena_humidity_10min | weather | 11.08 | 10.61 `zigzag(bitpacked(primitive,primitive,primitive))` | 10.70 | 10.45 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.02x** | 1.02x |
| audio_gtzan_p1_0 | audio | 14.29 | 14.41 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 14.86 | 14.20 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.01x** | 1.01x |
| geomag_BOU_Z_1hz | geomagnetic | 3.42 | 3.15 `zigzag(bitpacked(primitive,primitive,primitive))` | 3.21 | 3.10 lpc2/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.01x** | 1.01x |
| imu_ltmm_fl001_base_ap-acceleration | imu | 13.38 | 15.24 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,primitive)` | 15.24 | 15.11 lpc2/chunk `v2(bitpacked,primitive)` | **1.01x** | 1.01x |
| weather_jena_temp_10min | weather | 8.42 | 8.22 `zigzag(bitpacked(primitive,primitive,primitive))` | 8.28 | 8.15 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.01x** | 1.01x |
| seis_IU_ANMO_LFZ_magnetic | seismic | 4.04 | 4.05 `zigzag(bitpacked(primitive,primitive,primitive))` | 4.80 | 4.03 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.01x** | 1.01x |
| eeg_mmidb_160hz_Fc5 | eeg | 7.44 | 7.28 `zigzag(bitpacked(primitive,primitive,primitive))` | 8.05 | 7.23 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.01x** | 1.01x |
| power_household_global_active_power_1min | power | 11.53 | 11.31 `zigzag(bitpacked(primitive,primitive,primitive))` | 11.86 | 11.25 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.01x** | 1.01x |
| power_household_voltage_1min | power | 9.83 | 9.37 `zigzag(bitpacked(primitive,primitive,primitive))` | 10.03 | 9.33 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.00x** | 1.00x |
| weather_jena_wind_10min | weather | 9.73 | 9.21 `zigzag(bitpacked(primitive,primitive,primitive))` | 10.06 | 9.20 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.00x** | 1.00x |
| eeg_mmidb_160hz_Cz | eeg | 7.23 | 7.10 `zigzag(bitpacked(primitive,primitive,primitive))` | 7.40 | 7.11 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.00x** | 1.00x |
| eeg_chbmit_256hz_T7-P7 | eeg | 6.57 | 6.43 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 6.89 | 6.45 lpc16/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.00x** | 1.00x |
| seis_IU_ANMO_LWS_windspeed | seismic | 5.16 | 5.62 `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | 6.26 | 5.64 lpc8/lanes `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.00x** | 1.00x |
| imu_ltmm_fl001_base_ml-acceleration | imu | 13.93 | 15.22 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,primitive)` | 15.22 | 15.29 lpc8/chunk `v2(bitpacked,primitive)` | **1.00x** | 1.00x |
| audio_gtzan_p1_110 | audio | 12.83 | 12.86 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 13.35 | 12.91 lpc8/chunk `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | **1.00x** | 1.00x |
| audio_gtzan_classical_150 | audio | 8.21 | 7.54 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 7.67 | 7.65 lpc16/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **0.99x** | 0.99x |
| seis_IU_ANMO_LKO_temp | seismic | 0.51 | 0.39 `runend(delta(chunk_delta(primitive,primitive,primitive),bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 0.52 | 0.39 lpc4/lanes `runend(delta(chunk_delta(primitive,primitive,primitive),bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | **0.98x** | 0.98x |
| ts_daphnet_S01R01_ms | timestamp | 0.25 | 1.02 `v2(bitpacked)` | 2.03 | 1.05 lpc2/chunk `v2(bitpacked)` | **0.98x** | 0.98x |
| imu_daphnet_S01R01_trunk_lat_64hz | imu | 9.10 | 10.11 `dict(bitpacked,v2(bitpacked))` | 10.88 | 10.45 lpc2/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.97x** | 0.97x |
| imu_daphnet_S01R01_thigh_vert_64hz | imu | 9.09 | 10.10 `dict(bitpacked,v2(bitpacked(primitive,primitive,primitive)))` | 11.21 | 10.52 lpc4/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **0.96x** | 0.96x |
| grid_plaid_1312_voltage_30khz | grid | 14.91 | 11.14 `dict(bitpacked,zigzag(bitpacked(primitive,primitive,primitive)))` | 11.56 | 11.61 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.96x** | 0.96x |
| fin_btcusdt_trade_time | timestamp | 11.20 | 10.99 `bitpacked(primitive,primitive,primitive)` | 11.95 | 11.69 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.94x** | 0.94x |
| imu_daphnet_S02R01_ankle_lat_64hz | imu | 9.22 | 10.17 `affine[gcd](varbitpacked,primitive,dict(runend(primitive,primitive),primitive))` | 10.97 | 10.86 lpc4/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **0.94x** | 0.94x |
| imu_daphnet_S01R01_ankle_fwd_64hz | imu | 10.10 | 11.22 `dict(bitpacked,v2(bitpacked,primitive))` | 12.43 | 12.03 lpc2/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **0.93x** | 0.93x |
| fin_btcusdt_trade_price | financial | 7.34 | 7.18 `zigzag(bitpacked(primitive,primitive,primitive))` | 7.69 | 7.70 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.93x** | 0.93x |
| imu_ltmm_fl001_base_v-acceleration | imu | 13.42 | 13.95 `chunk_delta(dict(bitpacked,primitive),primitive,primitive)` | 15.40 | 15.16 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.92x** | 0.92x |
| emg_healthy_4khz | emg | 10.29 | 10.30 `dict(bitpacked,v2(bitpacked))` | 11.60 | 12.65 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.81x** | 0.81x |
| emg_neuropathy_4khz | emg | 12.34 | 12.49 `dict(bitpacked,v2(bitpacked,primitive))` | 13.90 | 15.80 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.79x** | 0.79x |
| bidmc01_RESP | ppg_resp | 8.18 | 7.14 `dict(bitpacked,primitive)` | 7.12 | 9.59 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.75x** | 0.74x |
| bidmc01_II | ppg_resp | 9.28 | 11.06 `dict(bitpacked(primitive,primitive,primitive),v2(bitpacked,primitive))` | 11.58 | 15.79 lpc32/lanes `v2(bitpacked(primitive,primitive,primitive),primitive)` | **0.70x** | 0.70x |
| bidmc01_PLETH | ppg_resp | 8.13 | 7.13 `dict(bitpacked,primitive)` | 7.12 | 10.84 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.66x** | 0.66x |
| seis_IU_SNZO_HH1_100hz | seismic | 1.43 | 1.48 `rle(chunk_delta(varbitpacked,primitive,primitive),chunk_delta(runend(bitpacked,bitpacked)),primitive)` | 2.38 | 2.48 lpc32/lanes `rle(zigzag(bitpacked(primitive,primitive,primitive)),delta(bitpacked,runend(bitpacked,bitpacked)),primitive)` | **0.59x** | 0.59x |
| grid_plaid_1005_voltage_30khz | grid | 12.24 | 12.39 `dict(bitpacked(primitive,primitive,primitive),v2(bitpacked,primitive))` | 13.56 | 21.17 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.59x** | 0.59x |
| grid_plaid_1816_current_30khz | grid | 13.72 | 6.19 `dict(bitpacked,primitive)` | 7.27 | 20.64 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.30x** | 0.30x |

</details>

## Searching for a show-stopper dataset

**Where LPC can win big.** Delta-of-delta on a sinusoid of amplitude `A`, at normalized frequency
`ω = 2πf/fs`, leaves a residual of about `(2 − 2cos ω)·A`. LPC removes that down to the noise
floor. A large LPC win therefore needs a narrowband signal far above its noise floor, sampled
only moderately often: roughly 5–100 samples per cycle.

- **Heavily oversampled signals don't qualify.** The grid waveforms at 30–500 kHz are an example:
  Delta already reaches the noise floor there.
- **Coarsely quantized signals don't either.** Wave buoys at 1 cm resolution are an example: the
  quantization noise *is* the signal's fine structure.

**Long-period seismic, 15 stations worldwide** (`scan_iris.py`; 6 days of LHZ at 1 Hz, first 512K
samples, the same model as `lpc_eval.py`):

| station | Delta | delta-of-delta | LPC | gain vs better fixed predictor |
|---|---|---|---|---|
| IU.ANMO | 12.41 | 12.24 | 9.46 | 1.29x |
| IU.RAR | 15.74 | 16.13 | 12.54 | 1.26x |
| IU.PAB | 11.90 | 12.00 | 9.62 | 1.24x |
| II.ALE | 13.28 | 13.47 | 10.80 | 1.23x |
| IU.KIP | 16.33 | 16.87 | 13.48 | 1.21x |
| II.BFO, IU.TATO, IU.PTCN, G.ECH | 13.5–17.1 | 13.7–17.6 | 11.2–14.2 | 1.20x |
| IU.COLA, IU.SSPA, IU.GUMO, IU.HRV, II.KDAK | 12.2–14.6 | 12.5–15.1 | 10.2–12.4 | 1.17–1.19x |

All sizes are bits/value.

- **The win is general.** Every station gains 17–29%, because the microseism, a 5–8 s ocean-driven
  oscillation sampled at 1 Hz, dominates every long-period channel on Earth.
- **The win is capped at ~2.5–3 bits/value.** That is the gap between the microseism amplitude
  and the broadband noise floor at these stations.
- **Higher-rate channels gain less.** BHZ at 40 Hz and HHZ at 100 Hz gain 1.00–1.16x, and the
  BDF infrasound sensor 1.05x: oversampled noise, where delta-of-delta is near optimal.
- **Barometers and VH channels** gain 1.03–1.20x.

**CDIP Datawell wave buoys** (`fetch.py waves`, 1.28 Hz, exact 1 cm grid, four buoys, Z and X
axes): 1.06–1.13x against the best fixed predictor.

## Encoder ablations

`ablate.py` runs on the 34 series where lane-parallel LPC gains ≥5%. All sizes are bits/value.

- **Fitting on a 1/16 sample** (4 of 64 lanes per block) costs at most 0.08 bits/value, usually
  0–0.05. Some series even improve slightly, because fitting the whole block overfits the noise.
  It is also 7–18x cheaper: 35–55 ns/value at order 32 in numpy, against 400–630 for the full
  fit. A Rust covariance fit would be far cheaper than either.
- **Coefficient precision:**
  - 12-bit coefficients cost 0–0.13 bits/value on 33 of the 34 series. `tide_seattle_6min` is the
    exception at 0.4.
  - 8-bit coefficients cost up to 2.2 bits/value (`audio_gtzan_classical2_120`).
  - So i16 coefficients with ≥12 significant bits are the right choice.
- **Accumulator range:**
  - The largest |accumulator| was ≤2^28.5 on 32 of the 34 series. The exceptions were IU.ANMO
    BDF infrasound at 2^30.5 and LH1 at 2^31.5.
  - Encoder and decoder both compute the prediction in wrapping i32, so the round trip is
    lossless regardless. A rare overflow only costs one large residual, which becomes a patch.
  - i32 lanes are therefore safe. Use i64 only when `data bits + coefficient bits + log2(order)`
    is well above 31.

## Decode throughput

### Measured

The kernels are in `lpc_kernels.rs`, run on a 4-vCPU Xeon @ 2.1 GHz VM with AVX-512, built with
`-C target-cpu=native`; times are the best of 9. Every SIMD kernel is checked against the scalar
reference.

**Predictor only.** Residuals are in L1/L2, and the times exclude bit-unpacking. All values are
ns/value.

| order | serial, i32 acc¹ | autovectorized, 64 lanes | AVX-512 intrinsics, 16 lanes | 32 lanes | 64 lanes | 128 lanes |
|---|---|---|---|---|---|---|
| 1 (delta) | 1.94 (0.42 as a plain prefix sum) | 0.22 | 0.17 | 0.17 | 0.17 | 0.17 |
| 2 | 2.01 | 0.25 | 0.35 | 0.25 | 0.21 | 0.20 |
| 4 | 5.84 | 0.35 | 0.41 | 0.30 | 0.32 | 0.29 |
| 8 | 6.26 | 0.47 | 0.45 | 0.49 | 0.46 | 0.47 |
| 16 | 7.72 | 0.83 | 0.91 | 0.86 | 0.89 | 0.89 |
| 32 | 10.77 | 1.64 | 1.71 | 1.70 | 1.64 | 1.63 |

¹ Built with `-C llvm-args=-vectorize-slp=false`. By default LLVM vectorizes the serial loop
across the `p` taps, which makes it 13–31 ns/value.

- **Autovectorization is unreliable.** The autovectorized lane kernel is fast only at 64 lanes. At
  8, 16 and 32 lanes it ran at 0.9–7.8 ns/value. The assembly shows why: LLVM vectorized across the
  taps using `vpgatherqd`, instead of across the lanes.
- **The AVX-512 intrinsic kernel removes the problem.** It keeps `L/16` accumulators per row and
  loops over taps in the outer loop.
  - It needs only 16 lanes to reach the multiply-throughput bound: one `vpmulld` per tap per 16
    lanes.
  - Its time grows linearly with order. Order 32 is 2 cycles per value.
- **i16 data could go further.** For 16-bit audio, with 12-bit coefficients, `vpmaddwd` would
  process two taps per instruction. That is not measured.

**Fused decode.** These kernels take residuals packed at `w` bits in a FastLanes-style vertical
layout (each lane's k-th 32-bit word sits next to the other lanes'), and write 4 MiB of output. All
values are ns/value.

| order | unpack + FoR | unpack + LPC, lane-interleaved output (32 lanes) | unpack + LPC + 16×16 transpose to original order (16 lanes × 1024) |
|---|---|---|---|
| 0 / 1 | 0.19–0.23 | 0.20–0.25 | 0.45 |
| 4 | — | 0.29 | 0.64 |
| 8 | — | 0.47 | 0.88 |
| 16 | — | 0.69 | 1.13 |
| 32 | — | 1.41 | 1.96 |

- **Residual width does not matter** (6, 10 and 14 bits tested).
- **The transpose costs about 0.25 ns/value.** It is 64 `vpermt2d` instructions per 256 values,
  plus the 16 strided stores.
- **Lanes of 65536 values are slower** than lanes of 1024 (0.70 vs 0.45 ns/value at order 0),
  because 16 store streams 256 KiB apart alias in the caches.
- **Lane length is a compression trade-off.** A lane of 1024 values costs `p` warm-ups, i.e.
  `p`/1024 of the values, which the size model already includes.

**pco on real data** (`pco_speed.py`; 1M values, i32, level 8):

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
- **pco Conv1** is the only LPC + tANS codec in a columnar context. pco 1.0.2 added a specialised
  order-6 decoder that is "only slightly faster".
- **FLAC, MPEG-4 ALS and Monkey's Audio** gain 2–4% of ratio over each other, at 1.6–1.8x the
  decode time. libFLAC 1.4 removed its decoder SIMD.
- **LeCo** (SIGMOD 2024) is linear in the index, not autoregressive. It decodes 14–34% slower than
  FOR. **TimescaleDB and InfluxDB** stop at delta-of-delta + simple8b plus Gorilla XOR; no TSDB
  ships LPC.
- **Parallel scans** of linear recurrences (Blelloch's companion-matrix scan, Martin & Cundy 2018)
  are exact only for integer-coefficient predictors with no rounding shift. For those predictors
  (delta, delta-of-delta, FLAC fixed orders) the scan is just repeated prefix sums. Quantized LPC
  is not associative, so lane interleaving is the bit-exact option.

### About the ANS idea

"ANS-style" can mean two different things, and both matter.

1. **Interleaving independent streams to hide latency.** Interleaved rANS (Giesen 2014) does this
   and gets 1.6–2.8x. FastLanes Delta does the same with its transposed lanes. For LPC this is the
   fix measured above, and it gives 10–25x end to end because the serial chain is long (mul → add
   → shift → add per value).
2. **Entropy coding the residuals with ANS.** This is worth ~0.6–1.0 bits/value over bit-packing
   on Laplacian-like residuals, and much more on peaked ones (the "+ANS" columns).
   - Once LPC is interleaved, the tANS decoder is the bottleneck: pco's unpack and tANS alone run
     at about 1 GB/s.
   - So a fast LPC + ANS codec needs both the predictor and the entropy coder interleaved. I found
     no published codec that does both.

## Design sketch for a Vortex `LPC` encoding

- **Layout.**
  - Use blocks of 16 lanes × 1024 values, so one AVX-512 register holds one row. A smaller final
    block uses fewer lanes.
  - Pack the residuals vertically (FastLanes-style), so row `t` of every lane is at the same bit
    offset.
  - The residual child cascades: BitPacked / VarBitPacked, or Pco with `NoOp` for ratio.
  - Store per lane:
    - i16 coefficients `q[p]` (or one set per block) and `shift`;
    - `p` warm-ups: the first as FoR over the lane starts, the rest as in-lane deltas.
- **Decode.**
  - Unpack a row of 16 residuals and add the reference.
  - Add `(Σ_j q_j · row[t-p+j]) >> shift` in wrapping i32.
  - Every 16 rows, transpose the 16×16 tile in registers and store 16 contiguous values per lane.
  - The output is in original order, with no untranspose pass.
- **Random access.**
  - `scalar_at` must replay the lane from its start, up to 1024 steps; this is the same story as
    Delta.
  - Slices are lane-aligned for free.
- **Compressor.**
  - Fit with least squares (the covariance method) on a 1/16 sample, and choose the order from
    {4, 8, 16, 32}.
  - Accept LPC only when the cascaded size of whole sampled chunks beats Delta, delta-of-delta and
    FoR by a margin, as `ChunkDeltaScheme` and `AffineScheme` on `ji/affine-encoding` already do.
  - Skip LPC when the column already has a cheap `dict`, `runend` or `sequence` tree, or when
    ANS-coded deltas are much smaller than bit-packed deltas (a peaked or few-valued alphabet).
- **Floats.** Cascade LPC after ALP: ALP's integer child is what LPC sees. CWRU vibration and the
  ADMIT voltages were recovered to exact ADC counts here, which is what ALP would produce. PLAID was
  decimal-scaled, as ALP does.

## Caveats

- **Model sizes approximate real encodings.**
  - The model's patch and header costs approximate `BitPacked` with patches and `VarBitPacked`.
    The "+ANS" sizes use pco `NoOp` on the flattened residual stream.
  - The real-Vortex comparison adds model header bits to real residual trees. Coefficients and
    warm-ups would really be small child arrays, and would compress somewhat better.
- **Delta is modelled as lag-1 per chunk** (ChunkDelta / Parquet style). FastLanes Delta's
  transposed layout is about the same size or slightly larger.
- **The VM is shared and noisy.** Treat the throughput numbers as ratios. Re-measure on dedicated
  hardware; `HANDOFF.md` lists what to run.
- **The datasets lean toward signals,** because finding LPC's wins was the point. Business and
  low-rate data are included as controls.
- **Some sources were left out rather than substituted.**
  - Failed downloads: one IRIS strong-motion channel, two IRIS stations, a Sleep-EDF record, and
    Wikimedia Commons audio (rate-limited).
  - Too short (<4096 values): three LTMM records, and EEG mmidb is barely above that limit.
  - The EV charging PLC CSV, because its 7-significant-digit floats have no exact integer lattice.
- **Two PLAID columns are decimal-scaling artifacts:** `1816_current` and `1005_voltage`. They
  have no exact lattice either, and pco's float-quantization modes beat every predictor on them.

## Reproducing

```bash
python3 -m venv venv && ./venv/bin/pip install numpy scipy pcodec zstandard wfdb soundfile pyarrow obspy pyedflib h5py
./venv/bin/python -I research/lpc/fetch.py data          # downloads into data/raw, writes data/series/*.npy
./venv/bin/python -I research/lpc/lpc_eval.py data       # appends to data/results.jsonl (resumable)
./venv/bin/python -I research/lpc/summarize.py data/results.jsonl
./venv/bin/python -I research/lpc/ablate.py data 1.05
./venv/bin/python -I research/lpc/pco_speed.py data seis_IU_ANMO_LHZ_1hz vib_cwru_normal_12khz
rustc --edition 2024 -O -C target-cpu=native research/lpc/lpc_kernels.rs -o lpc_kernels && ./lpc_kernels

# Real Vortex sizes (needs a checkout of ji/affine-encoding)
./venv/bin/python -I research/lpc/export_vortex.py data vx_in
(cd ../vx-affine && cargo build --release -p vortex-btrblocks --features pco --example affine_eval)
AFFINE_EVAL_CONFIGS=default,affine+varbp+cdelta ../vx-affine/target/release/examples/affine_eval vx_in > vx_out.tsv
./venv/bin/python -I research/lpc/vortex_compare.py vx_in vx_out.tsv data
```

`results.jsonl` holds every model number: each series' manifest row, plus the bits/value of every
predictor × coder pair. `vortex_results.tsv` holds the real Vortex trees and sizes.

## Real Vortex results by category

| category | series | geo-mean gain over best Vortex | min..max | LPC smaller in |
|---|---|---|---|---|
| tides | 4 | 1.19x | 1.04..1.43 | 4/4 |
| geomagnetic | 4 | 1.16x | 1.10..1.31 | 4/4 |
| ecg | 6 | 1.16x | 1.02..1.26 | 6/6 |
| vibration | 5 | 1.15x | 1.07..1.23 | 5/5 |
| pcg | 1 | 1.13x | 1.13..1.13 | 1/1 |
| seismic | 14 | 1.07x | 0.57..1.36 | 12/14 |
| audio | 14 | 1.06x | 0.99..1.16 | 13/14 |
| weather | 4 | 1.06x | 1.03..1.08 | 4/4 |
| power | 3 | 1.05x | 1.03..1.08 | 3/3 |
| eeg | 4 | 1.03x | 1.02..1.07 | 4/4 |
| grid | 9 | 1.02x | 0.58..1.28 | 7/9 |
| financial | 1 | 0.95x | 0.95..0.95 | 0/1 |
| imu | 8 | 0.89x | 0.84..1.03 | 1/8 |
| ppg_resp | 4 | 0.82x | 0.59..1.19 | 1/4 |
| emg | 2 | 0.80x | 0.78..0.81 | 0/2 |
| timestamp | 2 | 0.48x | 0.24..0.96 | 0/2 |


## Full results (model)

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
| grid_plaid_1464_current_30khz | grid | 30000 | delta 5.08 | lpc16/lanes 4.21 | **1.21x** | 4.09 | 3.42 | 1.20x | 4.10 | 3.37 | 1.22x | 5.57 |
| vib_cwru_ball007_12khz | vibration | 12000 | for 12.95 | lpc32/lanes 11.06 | **1.17x** | 11.83 | 9.92 | 1.19x | 11.83 | 9.82 | 1.26x | 13.48 |
| seis_IU_ANMO_HHZ_100hz | seismic | 100 | delta 7.83 | lpc32/lanes 6.78 | **1.15x** | 7.05 | 6.03 | 1.17x | 7.05 | 5.95 | 1.19x | 10.14 |
| vib_cwru_outerrace007_12khz | vibration | 12000 | for 14.08 | lpc16/lanes 12.21 | **1.15x** | 12.33 | 10.50 | 1.18x | 12.33 | 10.35 | 1.25x | 13.76 |
| audio_esc50_env8clips | audio | 44100 | dod 10.06 | lpc16/chunk 8.79 | **1.14x** | 9.98 | 8.73 | 1.14x | 9.98 | 9.00 | 1.11x | 11.36 |
| grid_plaid_1005_current_30khz | grid | 30000 | dod 5.85 | lpc8/chunk 5.12 | **1.14x** | 5.28 | 4.16 | 1.27x | 5.29 | 4.65 | 1.14x | 10.15 |
| vib_cwru_outerrace021_12khz | vibration | 12000 | for 14.33 | lpc32/lanes 12.74 | **1.12x** | 11.80 | 10.48 | 1.13x | 11.80 | 10.32 | 1.18x | 13.24 |
| audio_gtzan_classical2_120 | audio | 22050 | dod 9.84 | lpc32/lanes 8.84 | **1.11x** | 9.33 | 8.26 | 1.13x | 9.26 | 8.16 | 1.13x | 12.95 |
| seis_IU_ANMO_VHZ_0.1hz | seismic | 1/10 | delta 6.54 | lpc16/chunk 5.89 | **1.11x** | 4.87 | 4.74 | 1.03x | 4.87 | 5.87 | 0.83x | 8.10 |
| tide_seattle_6min | tides | 1/360 | dod 6.18 | lpc16/lanes 5.57 | **1.11x** | 4.08 | 4.47 | 0.91x | 4.05 | 3.55 | 1.14x | 10.31 |
| grid_admit_voltage_500khz | grid | 500000 | delta 7.41 | lpc32/lanes 6.70 | **1.11x** | 5.63 | 4.89 | 1.15x | 5.63 | 4.85 | 1.16x | 12.19 |
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
| grid_plaid_759_current_30khz | grid | 30000 | delta 2.46 | lpc8/chunk 2.36 | **1.04x** | 1.62 | 1.58 | 1.03x | 1.62 | 1.63 | 1.00x | 2.21 |
| grid_plaid_1312_voltage_30khz | grid | 30000 | dod 12.93 | lpc8/lanes 12.39 | **1.04x** | 6.85 | 9.19 | 0.75x | 7.79 | 7.90 | 0.99x | 9.03 |
| grid_plaid_1005_voltage_30khz | grid | 30000 | dod 21.81 | lpc4/chunk 20.94 | **1.04x** | 11.19 | 19.53 | 0.57x | 11.22 | 10.10 | 1.11x | 18.25 |
| tide_sanfrancisco_6min | tides | 1/360 | dod 6.58 | lpc16/lanes 6.32 | **1.04x** | 5.57 | 5.29 | 1.05x | 5.57 | 5.22 | 1.07x | 10.80 |
| grid_plaid_1816_current_30khz | grid | 30000 | dod 21.20 | lpc16/lanes 20.39 | **1.04x** | 4.86 | 19.66 | 0.25x | 4.87 | 18.59 | 0.26x | 8.68 |
| ecg_mitdb100_V5 | ecg | 360 | dod 6.22 | lpc16/lanes 5.99 | **1.04x** | 3.75 | 3.93 | 0.96x | 3.75 | 3.90 | 0.96x | 5.01 |
| audio_gtzan_p1_0 | audio | 22050 | delta 13.99 | lpc8/chunk 13.47 | **1.04x** | 13.13 | 12.72 | 1.03x | 13.14 | 12.83 | 1.02x | 15.30 |
| ecg_mitdb203_MLII | ecg | 360 | dod 5.89 | lpc16/lanes 5.67 | **1.04x** | 4.64 | 4.46 | 1.04x | 4.64 | 4.43 | 1.05x | 7.14 |
| ecg_ptb1khz_v2 | ecg | 1000 | dod 8.03 | lpc32/lanes 7.75 | **1.04x** | 6.16 | 5.94 | 1.04x | 6.17 | 5.87 | 1.05x | 10.30 |
| grid_plaid_1312_current_30khz | grid | 30000 | dod 7.03 | lpc4/chunk 6.78 | **1.04x** | 4.58 | 4.36 | 1.05x | 4.58 | 4.73 | 0.97x | 5.19 |
| audio_gtzan_p1_110 | audio | 22050 | dod 12.45 | lpc8/chunk 12.03 | **1.03x** | 11.50 | 11.15 | 1.03x | 11.53 | 11.52 | 1.00x | 14.75 |
| bidmc01_PLETH | ppg_resp | 125 | dod 11.14 | lpc4/lanes 10.77 | **1.03x** | 4.37 | 9.58 | 0.46x | 4.37 | 9.53 | 0.46x | 5.73 |
| imu_ltmm_fl001_base_ml-acceleration | imu | 100 | dod 15.11 | lpc8/chunk 14.68 | **1.03x** | 12.28 | 13.09 | 0.94x | 12.07 | 13.11 | 0.91x | 14.73 |
| seis_II_PFO_BHZ_40hz | seismic | 40 | dod 5.30 | lpc8/lanes 5.15 | **1.03x** | 4.40 | 4.13 | 1.07x | 4.40 | 4.00 | 1.10x | 10.15 |
| ecg_ptb1khz_i | ecg | 1000 | delta 8.27 | lpc32/lanes 8.09 | **1.02x** | 7.12 | 6.87 | 1.04x | 7.13 | 6.80 | 1.05x | 10.50 |
| emg_healthy_4khz | emg | 4000 | delta 12.85 | lpc8/chunk 12.58 | **1.02x** | 5.63 | 9.26 | 0.61x | 5.64 | 9.19 | 0.61x | 6.71 |
| imu_daphnet_S02R01_ankle_lat_64hz | imu | 64 | for 9.84 | lpc4/chunk 9.67 | **1.02x** | 5.46 | 8.02 | 0.68x | 5.47 | 8.23 | 0.66x | 5.67 |
| imu_ltmm_fl001_base_v-acceleration | imu | 100 | dod 15.37 | lpc4/lanes 15.11 | **1.02x** | 12.27 | 13.57 | 0.90x | 12.43 | 13.56 | 0.85x | 14.96 |
| eeg_mmidb_160hz_Fc5 | eeg | 160 | delta 7.27 | lpc8/lanes 7.16 | **1.01x** | 6.37 | 6.36 | 1.00x | 6.38 | 6.34 | 1.01x | 9.34 |
| imu_daphnet_S03R01_trunk_vert_64hz | imu | 64 | for 8.17 | lpc2/chunk 8.06 | **1.01x** | 4.56 | 6.83 | 0.67x | 4.57 | 6.88 | 0.66x | 4.34 |
| geomag_BOU_Z_1hz | geomagnetic | 1 | delta 2.75 | lpc2/chunk 2.71 | **1.01x** | 1.88 | 1.85 | 1.02x | 1.88 | 1.84 | 1.02x | 4.27 |
| weather_jena_temp_10min | weather | 1/600 | dod 8.16 | lpc4/lanes 8.05 | **1.01x** | 6.34 | 6.16 | 1.03x | 6.34 | 6.13 | 1.03x | 10.15 |
| weather_jena_humidity_10min | weather | 1/600 | delta 10.47 | lpc4/lanes 10.34 | **1.01x** | 7.33 | 8.48 | 0.86x | 7.16 | 6.92 | 1.03x | 9.68 |
| weather_jena_pressure_10min | weather | 1/600 | delta 6.45 | lpc8/lanes 6.37 | **1.01x** | 5.14 | 5.00 | 1.03x | 5.15 | 5.02 | 1.02x | 8.42 |
| power_household_global_intensity_1min | power | 1/60 | delta 6.39 | lpc16/lanes 6.31 | **1.01x** | 2.50 | 3.06 | 0.82x | 2.50 | 2.86 | 0.88x | 2.96 |
| imu_daphnet_S01R01_thigh_vert_64hz | imu | 64 | for 9.38 | lpc4/chunk 9.29 | **1.01x** | 5.65 | 7.79 | 0.73x | 5.65 | 7.87 | 0.72x | 5.61 |
| seis_IU_ANMO_LWS_windspeed | seismic | 1 | delta 4.79 | lpc8/lanes 4.75 | **1.01x** | 3.75 | 3.73 | 1.01x | 3.75 | 3.72 | 1.01x | 4.51 |
| eeg_mmidb_160hz_Cz | eeg | 160 | delta 7.17 | lpc4/lanes 7.11 | **1.01x** | 6.26 | 6.24 | 1.00x | 6.27 | 6.22 | 1.01x | 9.31 |
| bidmc01_RESP | ppg_resp | 125 | dod 10.75 | lpc4/lanes 10.67 | **1.01x** | 3.60 | 7.68 | 0.47x | 3.60 | 7.21 | 0.50x | 7.28 |
| imu_daphnet_S01R01_trunk_lat_64hz | imu | 64 | for 9.35 | lpc2/chunk 9.28 | **1.01x** | 5.47 | 7.74 | 0.71x | 5.48 | 7.76 | 0.71x | 5.57 |
| imu_daphnet_S01R01_ankle_fwd_64hz | imu | 64 | for 10.75 | lpc2/chunk 10.68 | **1.01x** | 6.22 | 8.73 | 0.71x | 6.23 | 8.97 | 0.69x | 6.53 |
| weather_jena_wind_10min | weather | 1/600 | delta 9.14 | lpc8/lanes 9.08 | **1.01x** | 7.84 | 7.81 | 1.00x | 7.85 | 7.81 | 1.00x | 10.02 |
| power_household_global_active_power_1min | power | 1/60 | delta 10.97 | lpc16/lanes 10.91 | **1.01x** | 6.14 | 7.26 | 0.85x | 6.15 | 7.00 | 0.88x | 8.00 |
| seis_IU_SNZO_HH1_100hz | seismic | 100 | dod 2.03 | lpc32/lanes 2.02 | **1.00x** | 1.60 | 1.61 | 0.99x | 1.59 | 1.64 | 0.97x | 1.96 |
| imu_ltmm_fl001_base_ap-acceleration | imu | 100 | dod 15.13 | lpc2/chunk 15.10 | **1.00x** | 12.04 | 13.38 | 0.90x | 12.34 | 13.38 | 0.87x | 14.75 |
| power_household_voltage_1min | power | 1/60 | delta 9.33 | lpc8/lanes 9.32 | **1.00x** | 7.88 | 7.88 | 1.00x | 7.89 | 7.87 | 1.00x | 10.23 |
| grid_plaid_788_current_30khz | grid | 30000 | dod 6.00 | lpc4/chunk 5.99 | **1.00x** | 2.95 | 2.99 | 0.99x | 2.95 | 2.93 | 1.01x | 3.53 |
| seis_IU_ANMO_LFZ_magnetic | seismic | 1 | delta 4.08 | lpc16/lanes 4.08 | **1.00x** | 3.11 | 2.90 | 1.07x | 3.12 | 2.87 | 1.09x | 5.13 |
| ecg_apnea100hz_ECG | ecg | 100 | for 8.81 | lpc32/lanes 8.87 | **0.99x** | 5.60 | 6.19 | 0.90x | 5.60 | 6.17 | 0.91x | 6.68 |
| emg_neuropathy_4khz | emg | 4000 | for 15.50 | lpc8/chunk 15.60 | **0.99x** | 7.64 | 11.57 | 0.66x | 6.29 | 7.51 | 0.84x | 8.35 |
| fin_btcusdt_trade_price | financial |  | delta 7.86 | lpc8/chunk 7.92 | **0.99x** | 2.90 | 3.97 | 0.73x | 2.91 | 3.17 | 0.92x | 3.99 |
| seis_IU_ANMO_LKO_temp | seismic | 1 | delta 1.21 | lpc4/lanes 1.22 | **0.99x** | 0.15 | 0.17 | 0.92x | 0.15 | 0.16 | 0.94x | 0.23 |
| ts_daphnet_S01R01_ms | timestamp | 64 | delta 1.08 | lpc2/chunk 1.12 | **0.96x** | 1.00 | 1.05 | 0.96x | 3.53 | 3.01 | 1.17x | 9.63 |
| fin_btcusdt_trade_time | timestamp |  | delta 9.47 | lpc8/chunk 10.48 | **0.90x** | 4.97 | 7.68 | 0.65x | 4.98 | nan | nanx | 7.65 |

## Full results (real Vortex)

All sizes are bits/value. Trees are the BtrBlocks encodings chosen; `primitive` leaves are uncompressed.

| series | category | Vortex default | best Vortex (tree) | LPC + Vortex on residuals (residual tree) | gain |
|---|---|---|---|---|---|
| tide_seattle_6min | tides | 8.32 | 8.12 `chunk_delta(primitive,primitive,primitive)` | 5.67 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.43x** |
| seis_IU_ANMO_LDO_pressure | seismic | 6.38 | 6.15 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 4.53 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.36x** |
| seis_II_PFO_BHZ_40hz | seismic | 7.47 | 7.09 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 5.27 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.35x** |
| geomag_FRD_H_1hz | geomagnetic | 4.42 | 4.42 `delta(v2(bitpacked,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 3.39 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.31x** |
| seis_IU_ANMO_LKO_temp | seismic | 0.53 | 0.51 `dict(runend(varbitpacked,primitive),v2(bitpacked))` | 0.39 lpc4/lanes `runend(delta(chunk_delta(primitive,primitive,primitive),bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | **1.30x** |
| grid_plaid_1312_voltage_30khz | grid | 14.91 | 14.91 `dict(bitpacked,v2(bitpacked,primitive))` | 11.61 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.28x** |
| grid_plaid_759_current_30khz | grid | 7.46 | 3.64 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,dict(runend(primitive,primitive),primitive))` | 2.87 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.27x** |
| ecg_ptb1khz_v2 | ecg | 9.74 | 9.60 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,primitive)` | 7.59 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.26x** |
| ecg_mitdb100_MLII | ecg | 9.02 | 7.59 `chunk_delta(bitpacked(primitive,primitive,primitive),v2(bitpacked),v2(bitpacked))` | 6.02 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.26x** |
| vib_cwru_normal_12khz | vibration | 11.18 | 10.09 `chunk_delta(bitpacked(primitive,primitive,primitive),v2(bitpacked),v2(bitpacked))` | 8.21 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.23x** |
| grid_plaid_788_current_30khz | grid | 9.02 | 6.98 `chunk_delta(varbitpacked,primitive,primitive)` | 5.70 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.22x** |
| grid_plaid_1464_current_30khz | grid | 5.27 | 5.27 `delta(v2(bitpacked,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 4.31 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.22x** |
| ecg_mitdb100_V5 | ecg | 9.02 | 7.03 `chunk_delta(bitpacked(primitive,primitive,primitive),v2(bitpacked),v2(bitpacked))` | 5.79 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.21x** |
| tide_thebattery_6min | tides | 7.38 | 7.38 `delta(v2(bitpacked,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 6.13 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.20x** |
| resp_fantasia_RESP | ppg_resp | 6.07 | 6.02 `chunk_delta(bitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 5.07 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.19x** |
| seis_IU_ANMO_LH1_1hz | seismic | 17.74 | 12.34 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 10.40 lpc32/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.19x** |
| seis_IU_ANMO_LHZ_1hz | seismic | 13.58 | 12.30 `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | 10.40 lpc32/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.18x** |
| ecg_mitdb203_MLII | ecg | 7.32 | 7.22 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 6.14 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.18x** |
| grid_plaid_1005_current_30khz | grid | 7.45 | 6.48 `chunk_delta(varbitpacked,primitive,primitive)` | 5.53 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.17x** |
| vib_cwru_ball007_12khz | vibration | 13.06 | 13.06 `v2(bitpacked,primitive)` | 11.20 lpc32/lanes `v2(bitpacked,primitive)` | **1.17x** |
| audio_gtzan_classical2_120 | audio | 14.04 | 10.88 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 9.35 lpc32/lanes `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | **1.16x** |
| geomag_FRD_Z_1hz | geomagnetic | 4.23 | 4.09 `delta(chunk_delta(primitive,primitive,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 3.53 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.16x** |
| seis_IU_ANMO_BHZ_40hz | seismic | 7.22 | 7.00 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 6.08 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.15x** |
| seis_IU_ANMO_HHZ_100hz | seismic | 8.46 | 8.18 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 7.17 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.14x** |
| vib_cwru_outerrace021_12khz | vibration | 14.45 | 14.45 `zigzag(bitpacked(primitive,primitive,primitive))` | 12.71 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.14x** |
| vib_cwru_outerrace007_12khz | vibration | 14.03 | 14.03 `zigzag(bitpacked(primitive,primitive,primitive))` | 12.39 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.13x** |
| pcg_heartsound_2khz | pcg | 14.51 | 11.09 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 9.85 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.13x** |
| tide_sanfrancisco_6min | tides | 7.72 | 7.43 `chunk_delta(varbitpacked,primitive,primitive)` | 6.62 lpc16/lanes `zigzag(varbitpacked)` | **1.12x** |
| geomag_BOU_Z_1hz | geomagnetic | 3.42 | 3.42 `delta(v2(bitpacked,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 3.10 lpc2/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.10x** |
| geomag_BOU_H_1hz | geomagnetic | 4.33 | 3.48 `chunk_delta(varbitpacked,primitive,v2(bitpacked))` | 3.17 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.10x** |
| audio_esc50_env8clips | audio | 16.02 | 10.29 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 9.44 lpc16/chunk `chunk_delta(varbitpacked,bitpacked(primitive,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | **1.09x** |
| audio_gtzan_row60_60 | audio | 16.02 | 14.16 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 12.99 lpc32/lanes `zigzag(varbitpacked)` | **1.09x** |
| seis_IU_COLA_BH1_40hz | seismic | 7.81 | 7.37 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 6.79 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.08x** |
| grid_plaid_1312_current_30khz | grid | 11.12 | 7.64 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 7.07 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.08x** |
| audio_gtzan_p1_165 | audio | 15.15 | 13.61 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 12.60 lpc32/lanes `zigzag(varbitpacked)` | **1.08x** |
| power_household_global_intensity_1min | power | 7.36 | 7.36 `bitpacked(primitive,primitive,primitive)` | 6.83 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.08x** |
| weather_jena_pressure_10min | weather | 13.21 | 7.00 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive)),v2(bitpacked))` | 6.50 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.08x** |
| audio_gtzan_classical_150 | audio | 11.57 | 8.21 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 7.65 lpc16/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.07x** |
| vib_cwru_innerrace007_12khz | vibration | 14.09 | 14.09 `zigzag(bitpacked(primitive,primitive,primitive))` | 13.17 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.07x** |
| eeg_chbmit_256hz_FP1-F7 | eeg | 10.25 | 7.12 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 6.66 lpc32/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.07x** |
| audio_gtzan_p1_275 | audio | 16.02 | 15.01 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 14.08 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.07x** |
| audio_gtzan_p1_55 | audio | 15.74 | 13.75 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 12.91 lpc32/lanes `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | **1.07x** |
| weather_jena_humidity_10min | weather | 13.17 | 11.08 `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | 10.45 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.06x** |
| audio_gtzan_blues2_30 | audio | 16.02 | 12.59 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 11.89 lpc16/chunk `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.06x** |
| weather_jena_wind_10min | weather | 11.09 | 9.73 `chunk_delta(bitpacked(primitive,primitive,primitive),bitpacked(primitive,primitive),v2(bitpacked))` | 9.20 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.06x** |
| power_household_voltage_1min | power | 11.03 | 9.83 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 9.33 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.05x** |
| audio_gtzan_blues_0 | audio | 15.38 | 13.24 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 12.58 lpc32/lanes `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.05x** |
| audio_gtzan_p1_220 | audio | 15.20 | 13.53 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 12.92 lpc8/chunk `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.05x** |
| seis_IU_MAJO_BHZ_quake | seismic | 23.66 | 14.53 `chunk_delta(varbitpacked,primitive,primitive)` | 13.93 lpc16/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **1.04x** |
| audio_gtzan_row90_90 | audio | 15.33 | 12.79 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 12.32 lpc16/chunk `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **1.04x** |
| ecg_ptb1khz_i | ecg | 8.57 | 8.42 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,primitive)` | 8.12 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.04x** |
| tide_sanfrancisco_1min | tides | 8.34 | 7.77 `chunk_delta(varbitpacked,primitive,primitive)` | 7.51 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.04x** |
| weather_jena_temp_10min | weather | 8.42 | 8.42 `delta(v2(bitpacked,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 8.15 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.03x** |
| imu_daphnet_S03R01_trunk_vert_64hz | imu | 9.09 | 9.03 `chunk_delta(varbitpacked,primitive,primitive)` | 8.74 lpc2/chunk `zigzag(varbitpacked)` | **1.03x** |
| eeg_mmidb_160hz_Fc5 | eeg | 9.28 | 7.44 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,primitive)` | 7.23 lpc8/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.03x** |
| power_household_global_active_power_1min | power | 12.02 | 11.53 `chunk_delta(bitpacked(primitive,primitive,primitive),bitpacked(primitive,primitive,primitive),v2(bitpacked))` | 11.25 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.03x** |
| grid_admit_voltage_500khz | grid | 8.34 | 7.70 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,v2(bitpacked))` | 7.53 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.02x** |
| eeg_chbmit_256hz_T7-P7 | eeg | 10.11 | 6.57 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 6.45 lpc16/lanes `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),v2(bitpacked))` | **1.02x** |
| seis_IU_ANMO_BDF_infrasound | seismic | 18.29 | 14.42 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 14.18 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.02x** |
| eeg_mmidb_160hz_Cz | eeg | 9.00 | 7.23 `chunk_delta(bitpacked(primitive,primitive,primitive),primitive,primitive)` | 7.11 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.02x** |
| ecg_apnea100hz_ECG | ecg | 9.22 | 9.22 `v2(bitpacked(primitive,primitive,primitive),dict(primitive,primitive))` | 9.07 lpc32/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.02x** |
| audio_librispeech_speech | audio | 15.05 | 12.04 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 11.92 lpc16/chunk `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | **1.01x** |
| seis_IU_ANMO_VHZ_0.1hz | seismic | 12.61 | 6.98 `chunk_delta(varbitpacked,zigzag(bitpacked(primitive,primitive,primitive)),zigzag(affine[gcd](bitpacked)))` | 6.91 lpc16/chunk `chunk_delta(varbitpacked,bitpacked(primitive,primitive,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | **1.01x** |
| audio_gtzan_p1_0 | audio | 15.38 | 14.29 `chunk_delta(varbitpacked,v2(bitpacked(primitive,primitive,primitive)),v2(bitpacked))` | 14.20 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **1.01x** |
| seis_IU_ANMO_LFZ_magnetic | seismic | 4.21 | 4.04 `chunk_delta(bitpacked(primitive,primitive,primitive),v2(bitpacked),v2(bitpacked))` | 4.03 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **1.00x** |
| audio_gtzan_p1_110 | audio | 16.02 | 12.83 `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | 12.91 lpc8/chunk `chunk_delta(varbitpacked,v2(bitpacked),v2(bitpacked))` | **0.99x** |
| fin_btcusdt_trade_time | timestamp | 11.33 | 11.20 `delta(chunk_delta(bitpacked(primitive,primitive,primitive),primitive),bitpacked(primitive,primitive,primitive))` | 11.69 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.96x** |
| fin_btcusdt_trade_price | financial | 7.40 | 7.34 `delta(chunk_delta(bitpacked,primitive,primitive),zigzag(bitpacked(primitive,primitive,primitive)))` | 7.70 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.95x** |
| seis_IU_ANMO_LWS_windspeed | seismic | 7.01 | 5.16 `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | 5.64 lpc8/lanes `chunk_delta(varbitpacked,bitpacked,v2(bitpacked))` | **0.91x** |
| imu_ltmm_fl001_base_ml-acceleration | imu | 16.06 | 13.93 `chunk_delta(dict(bitpacked,primitive),primitive,primitive)` | 15.29 lpc8/chunk `v2(bitpacked,primitive)` | **0.91x** |
| imu_ltmm_fl001_base_ap-acceleration | imu | 16.06 | 13.38 `chunk_delta(dict(bitpacked,primitive),primitive,primitive)` | 15.11 lpc2/chunk `v2(bitpacked,primitive)` | **0.89x** |
| imu_ltmm_fl001_base_v-acceleration | imu | 16.06 | 13.42 `chunk_delta(dict(bitpacked,primitive),primitive,primitive)` | 15.16 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.89x** |
| imu_daphnet_S01R01_trunk_lat_64hz | imu | 9.10 | 9.10 `dict(bitpacked,v2(bitpacked))` | 10.45 lpc2/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.87x** |
| imu_daphnet_S01R01_thigh_vert_64hz | imu | 9.09 | 9.09 `dict(bitpacked,v2(bitpacked(primitive,primitive)))` | 10.52 lpc4/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **0.86x** |
| bidmc01_RESP | ppg_resp | 10.28 | 8.18 `chunk_delta(dict(primitive,primitive),primitive,primitive)` | 9.59 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.85x** |
| imu_daphnet_S02R01_ankle_lat_64hz | imu | 9.22 | 9.22 `dict(bitpacked,v2(bitpacked(primitive,primitive)))` | 10.86 lpc4/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **0.85x** |
| imu_daphnet_S01R01_ankle_fwd_64hz | imu | 10.10 | 10.10 `dict(bitpacked,v2(bitpacked))` | 12.03 lpc2/chunk `chunk_delta(varbitpacked,primitive,primitive)` | **0.84x** |
| emg_healthy_4khz | emg | 10.29 | 10.29 `dict(bitpacked,v2(bitpacked))` | 12.65 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.81x** |
| emg_neuropathy_4khz | emg | 12.34 | 12.34 `dict(bitpacked,v2(bitpacked,primitive))` | 15.80 lpc8/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.78x** |
| bidmc01_PLETH | ppg_resp | 9.28 | 8.13 `affine[+slope](chunk_delta(dict(primitive,primitive),primitive,dict(primitive,primitive)),primitive,sparse(primitive,primitive))` | 10.84 lpc4/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.75x** |
| grid_plaid_1816_current_30khz | grid | 13.72 | 13.72 `dict(bitpacked,v2(bitpacked(primitive,primitive,primitive),primitive))` | 20.64 lpc16/lanes `zigzag(bitpacked(primitive,primitive,primitive))` | **0.66x** |
| bidmc01_II | ppg_resp | 9.28 | 9.28 `dict(bitpacked,v2(bitpacked))` | 15.79 lpc32/lanes `v2(bitpacked(primitive,primitive,primitive),primitive)` | **0.59x** |
| grid_plaid_1005_voltage_30khz | grid | 12.24 | 12.24 `delta(v2(bitpacked),dict(bitpacked,v2(bitpacked,primitive)))` | 21.17 lpc4/chunk `zigzag(bitpacked(primitive,primitive,primitive))` | **0.58x** |
| seis_IU_SNZO_HH1_100hz | seismic | 2.73 | 1.43 `rle(chunk_delta(varbitpacked,primitive,primitive),chunk_delta(runend(bitpacked,bitpacked)),primitive)` | 2.48 lpc32/lanes `rle(zigzag(bitpacked(primitive,primitive,primitive)),delta(bitpacked,runend(bitpacked,bitpacked)),primitive)` | **0.57x** |
| ts_daphnet_S01R01_ms | timestamp | 0.25 | 0.25 `delta(sequence,dict(runend(primitive,bitpacked),primitive))` | 1.05 lpc2/chunk `v2(bitpacked)` | **0.24x** |

## Encoder ablations (full table)

Bits/value of lane-parallel LPC, bit-packed with patches, on series where it gains ≥5%. Fit times are from numpy.

| series | order | 15-bit, full fit | 15-bit, 1/16 fit | 12-bit | 8-bit | max abs accumulator (bits) | fit ns/value (full, 1/16) |
|---|---|---|---|---|---|---|---|
| seis_IU_ANMO_LHZ_1hz | 32 | 9.73 | 9.69 | 9.75 | 10.72 | 24.9 | 627, 35 |
| tide_thebattery_6min | 4 | 5.90 | 5.90 | 5.90 | 6.86 | 22.6 | 47, 7 |
| tide_seattle_6min | 16 | 5.57 | 5.56 | 5.97 | 7.60 | 23.7 | 176, 21 |
| tide_sanfrancisco_1min | 16 | 7.06 | 7.08 | 7.06 | 7.27 | 24.5 | 257, 34 |
| vib_cwru_normal_12khz | 32 | 8.18 | 8.18 | 8.18 | 8.69 | 21.5 | 423, 36 |
| vib_cwru_innerrace007_12khz | 16 | 13.11 | 13.11 | 13.11 | 13.11 | 25.3 | 181, 17 |
| seis_IU_ANMO_BHZ_40hz | 32 | 5.46 | 5.47 | 5.51 | 7.11 | 23.1 | 431, 37 |
| seis_IU_COLA_BH1_40hz | 16 | 6.13 | 6.15 | 6.15 | 7.60 | 23.7 | 169, 21 |
| seis_IU_ANMO_HHZ_100hz | 32 | 6.78 | 6.80 | 6.79 | 7.37 | 22.9 | 510, 44 |
| seis_IU_ANMO_LDO_pressure | 16 | 5.52 | 5.53 | 5.52 | 5.56 | 26.8 | 146, 14 |
| seis_IU_ANMO_BDF_infrasound | 32 | 13.50 | 13.51 | 13.50 | 14.02 | 30.5 | 489, 35 |
| audio_esc50_env8clips | 32 | 8.87 | 8.93 | 8.89 | 9.72 | 26.2 | 407, 35 |
| ecg_mitdb100_MLII | 4 | 5.71 | 5.71 | 5.73 | 5.51 | 19.9 | 36, 4 |
| bidmc01_II | 32 | 15.14 | 15.11 | 15.14 | 15.12 | 27.5 | 443, 58 |
| resp_fantasia_RESP | 32 | 4.95 | 5.00 | 4.95 | 5.02 | 22.3 | 447, 36 |
| audio_gtzan_blues_0 | 32 | 12.00 | 12.07 | 12.02 | 13.39 | 23.8 | 406, 37 |
| audio_gtzan_classical_150 | 16 | 6.90 | 6.91 | 6.91 | 7.34 | 21.9 | 172, 15 |
| audio_gtzan_blues2_30 | 32 | 11.23 | 11.31 | 11.30 | 12.98 | 25.0 | 416, 40 |
| audio_gtzan_classical2_120 | 32 | 8.84 | 8.90 | 8.97 | 11.09 | 23.4 | 449, 40 |
| audio_gtzan_row60_60 | 32 | 12.69 | 12.75 | 12.70 | 14.29 | 23.9 | 415, 38 |
| audio_gtzan_row90_90 | 32 | 11.71 | 11.77 | 11.76 | 13.43 | 24.6 | 416, 38 |
| eeg_chbmit_256hz_FP1-F7 | 32 | 6.09 | 6.17 | 6.09 | 6.27 | 22.3 | 406, 35 |
| eeg_chbmit_256hz_T7-P7 | 16 | 5.68 | 5.69 | 5.68 | 6.02 | 21.7 | 147, 13 |
| seis_IU_ANMO_LH1_1hz | 32 | 9.73 | 9.69 | 9.78 | 11.15 | 31.5 | 421, 42 |
| vib_cwru_ball007_12khz | 32 | 11.06 | 11.06 | 11.07 | 11.14 | 21.9 | 427, 38 |
| vib_cwru_outerrace007_12khz | 16 | 12.21 | 12.21 | 12.21 | 12.21 | 25.1 | 163, 14 |
| vib_cwru_outerrace021_12khz | 32 | 12.74 | 12.76 | 12.74 | 12.81 | 24.9 | 503, 55 |
| audio_gtzan_p1_55 | 32 | 12.36 | 12.42 | 12.38 | 13.48 | 23.9 | 415, 35 |
| audio_gtzan_p1_165 | 32 | 12.26 | 12.34 | 12.30 | 13.43 | 23.5 | 479, 40 |
| audio_gtzan_p1_220 | 16 | 12.25 | 12.28 | 12.26 | 12.47 | 25.9 | 173, 14 |
| audio_gtzan_p1_275 | 32 | 13.84 | 13.86 | 13.85 | 14.78 | 24.1 | 482, 48 |
| grid_plaid_1005_current_30khz | 16 | 5.44 | 5.48 | 5.55 | 7.11 | 24.3 | 231, 22 |
| grid_plaid_1464_current_30khz | 16 | 4.21 | 4.21 | 4.22 | 4.22 | 22.5 | 215, 26 |
| grid_admit_voltage_500khz | 32 | 6.70 | 6.70 | 6.70 | 6.99 | 28.5 | 452, 52 |
