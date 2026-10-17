<!--
SPDX-License-Identifier: CC-BY-4.0
SPDX-FileCopyrightText: Copyright the Vortex contributors
-->

# LPC / higher-order linear prediction for lightweight columnar encodings: literature and measurements

Prepared for the Vortex encodings discussion (October 2026). Every number below has its source and,
where known, its hardware. "Local measurement" means I ran it in this session on a 4-vCPU VM
reporting `Intel(R) Xeon(R) Processor @ 2.10GHz` (AVX-512F/DQ, AVX2, BMI2). It used rustc
`-C target-cpu=native`, pco at git HEAD (v1.0.4, commit `0f849aa`, 2026-10-09), and 4 Mi i32 values
(16 MiB) per run. These are single-thread, best-of-5/7 runs. Treat them as relative numbers, not
absolute ones: it is a noisy VM.

---

## TL;DR

* **The hypothesis is right, and the published record and my own measurements agree.** Serial
  integer LPC reconstruction is latency-bound by the loop-carried multiply-add-shift chain.
  * Sprintz says so explicitly: *"The limiting factor is serial dependence between decoding one
    sample and predicting the next one; this is accelerated by having wider samples that fill a
    vector register, but not by having longer blocks."*
  * pco documents Conv1 (its LPC mode) as giving *"noticeably reduced decompression speeds"*.
    It deliberately leaves Conv1 out of `DeltaSpec::Auto` for that reason.
  * libFLAC removed all decoder SIMD/asm in 1.4.0 because it *"provided only a small speed
    benefit"*.
* **Local numbers.** A plain serial order-6 LPC inverse runs at ~8.4 ns/value (~475 MB/s of i32).
  The same kernel over **32 independent lanes in a transposed (FastLanes-style) layout runs at
  ~0.87 ns/value (~4.6 GB/s), a ~10x speedup**. A 64-lane i32-accumulator version reaches
  ~0.45 ns/value (~8.9 GB/s), close to the plain delta prefix sum (0.62 ns/value serial,
  0.42 ns/value with 32 lanes).
  * In pco, Conv1(6) decodes at ~350–400 MB/s against ~0.95–1.05 GB/s for Consecutive.
    Conv1 at other orders drops to ~185 MB/s.
* **The ANS analogy holds exactly.** Giesen's interleaved rANS gets 1.6–1.9x from 2-way
  interleaving and 2.2–2.8x from 8-way SSE4.1. FastLanes gets its delta speed from the same idea
  of independent lanes. The cost for LPC is **p warm-up values per lane** (or a cheaper warm-up
  predictor), which is heavy at FastLanes' 1024-value granularity:
  * at 32 lanes × 32 values, order 6 means ~19% of values are warm-ups;
  * the fix is to use bigger blocks per lane (e.g. 32 lanes × ≥1024 values) or to code warm-ups
    with a lower-order predictor.
* **Parallel-scan (Blelloch companion-matrix) formulations** only apply exactly when the
  recurrence is linear over Z/2^w, i.e. integer coefficients with no rounding shift. Examples are
  delta, delta-of-delta, FLAC "fixed" predictors and pco Consecutive. Quantized LPC with
  `>> shift` is *not* associative, so scan tricks are not bit-exact for it. Lane interleaving is
  bit-exact and simpler.
* **Compression gain of LPC over delta/delta-of-delta.** On synthetic oscillatory or smooth
  signals, pco Conv1 beats the best Consecutive order by **+7.6% (resonant) to +20.5% (smooth)**.
  It gains 0% on a random walk. ECG studies find that optimal LPC coefficients round to
  **[2, −1]**, i.e. delta-of-delta. Expect gains mainly on audio, vibration, seismic and
  other high-rate, band-limited sensor waveforms.

---

## 1. Prior work: lightweight, database and time-series encodings using linear prediction

### 1.1 Sprintz (Blalock, Madden, Guttag; Proc. ACM IMWUT 2(3), Sept 2018; arXiv 1808.02515)
* **Predictor.** FIRE (Fast Integer REgression), an online-learned AR(2) restricted to
  `x_i = x_{i-1} + α(x_{i-1} − x_{i-2})`, i.e. `δ_i = α δ_{i-1}`.
  * α is one fixed-point coefficient per column, updated once per block by a sign-gradient
    (L1 loss) step, with learning rate 1/2.
  * It is backward-adaptive: no coefficients are stored, and the decoder repeats the learning.
  * Blocks are 8 samples. Payloads are bit-packed, with optional Huffman coding and RLE of zero
    blocks.
* **Vectorization is across columns (variables) only.** *"delta (de)coding, zigzag (de)coding,
  and Fire all operate on each column independently, and so naturally vectorize."* Speed rises
  with column count and peaks at multiples of 32 B rows (256-bit registers).
* **Speeds.** Hardware: 2013 MacBook Pro, Core i7-4960HQ 2.6 GHz, single thread, memcpy ≈ 7.5 GB/s.
  * Decompression: "over 3GB/s", and "multiple GB/s once rows exceed ∼16B" without Huffman.
    With Huffman it is "consistently ... over 500MB/s".
  * Compression: >200 MB/s (8-bit, highest ratio) to 600 MB/s. It is unvectorized and ~50% higher
    on 16-bit data.
  * FIRE kernel alone: "encode at up to 5GB/s and decode at up to 6GB/s", nearly the same as delta
    and double-delta.
* **Ratio.** UCR archive, 85 datasets, data quantized to 8/16 bits.
  * FIRE beats delta on 51/85 datasets at 8-bit and on 74/85 at 16-bit.
  * Wilcoxon p = .0094 (8-bit) and 4.09e-12 (16-bit).
  * Highest-ratio Sprintz is significantly better than all baselines (Zstd, LZ4, Snappy, Zlib,
    FastPFOR, SIMD-BP128, Simple8b).
  * It loses to dictionary/LZ methods on step-like data such as AMPD gas and water.
* **Key quote for the hypothesis** (§5.4): *"The limiting factor is serial dependence between
  decoding one sample and predicting the next one; this is accelerated by having wider samples
  that fill a vector register, but not by having longer blocks."*

### 1.2 pcodec / Pco (Martin Loncaric; arXiv 2502.06112, "Pcodec: Better Compression for Numerical Sequences", 2025)
* **Delta modes** (format.md: `docs/format.md`, delta encoding IDs):
  * `Consecutive` (ID 1): order 1–7 repeated differences, i.e. integer polynomial predictors.
    The inverse is *k* prefix sums.
  * `Lookback` (ID 2): an extra per-value latent naming a lag, an "LZ-like" mode at number level.
  * `Conv1` (ID 3): LPC. The stored config is 5-bit `quantization`, 64-bit bias, 5-bit order
    (≤32) and i32 weights. Decode is
    `l[i] += ((bias + Σ weight[j]·l[i−order+j]) >> quantization)`, using 2k-bit arithmetic
    (i64 for 32-bit latents). It is limited to ≤32-bit types.
* **Encoder** (`pco/src/delta/conv1.rs`): a least-squares fit on centred data, built from rolling
  autocovariance dot products. It uses a Cholesky solve with L2 regularisation of 0.1, then
  quantizes the weights.
* **Decoder.** A generic scalar loop, plus a specialised order-6 routine (added in v1.0.2, May
  2026). The routine pushes each decoded value's contribution forward into 6 partial
  predictions. Source comment: *"In practice this algorithm seems to be similar in speed to the
  more obvious one on ARM and only slightly faster on x64. Neither architecture is very inclined
  to use SIMD here."* Another comment says `// TODO: do passes over latents instead of weights so
  we can use SIMD?`.
* **Docs** (`DeltaSpec::TryConv1`):
  * Conv1 is *"best if your numbers have local trends that aren't captured by simply taking
    differences, and you are willing to accept noticeably reduced decompression speeds"*.
  * *"We highly recommend using order 6 ... relatively fast decoding implementation for it (but
    still substantially slower than other delta encodings)"*.
  * `Auto` *"does not consider Conv1 delta encoding due to decompression performance costs."*
* **Release notes** (github.com/pcodec/pcodec/releases). v1.0.2 (10 May): "Added specialized conv1
  decompression routine with better performance for order 6", "Improved numerical stability of
  conv1 configuration", "...so conv1 still isn't considered during auto delta configuration."
* **Published Pco numbers** (no Conv1 numbers are published anywhere I could find):
  * Paper, Xeon Platinum 8488C, single thread: decompression 1,432–5,450 MiB/s across 6 datasets
    (Air Quality 2,206; Taxi 2,298; r/place 2,493). Ratios are +29% to +94% over the best
    alternative (e.g. Air Quality 14.62 vs 7.54 for Blosc+Zstd).
  * Entropy coding is **4-way interleaved tANS** over batches of 256 numbers, with codes and
    offsets laid out separately to allow vectorised offset unpacking.
  * Repo CSV (M3 Max, single P-core), computed from `docs/benchmark_results/columnar_mbp_m3_max_rust1_89.csv`:

    | Dataset | Pco ratio | Pco compress | Pco decompress | Parquet+zstd1 ratio | Parquet+zstd1 decompress |
    |---|---|---|---|---|---|
    | Air Quality | 10.00 | 470 MB/s | 2,407 MB/s | 3.83 | 1,090 MB/s |
    | Taxi | 6.90 | 553 MB/s | 3,723 MB/s | 5.03 | 2,483 MB/s |
    | r/place | 6.79 | 511 MB/s | 3,842 MB/s | 4.64 | 1,910 MB/s |
* **Local measurement of Conv1** (synthetic i32; ratio = raw/compressed; speeds in MB/s of i32):

  | Data | Mode | Ratio | Compress | Decompress | ns/value |
  |---|---|---|---|---|---|
  | resonant (2 damped resonators + noise, "audio/vibration-like") | Consecutive(1) | 3.97 | 151 | 1010 | 3.96 |
  | | Consecutive(2) | 4.92 | 234 | 944 | 4.24 |
  | | Consecutive(3) (= Auto) | 5.23 | 200 | 756 | 5.29 |
  | | Conv1(2) | 5.00 | 182 | 494 | 8.10 |
  | | **Conv1(6)** | **5.63 (+7.6%)** | 133 | **398** | 10.0 |
  | | Conv1(16) | 5.96 (+14%) | 106 | 187 | 21.4 |
  | smooth (integrated AR(1) velocity + noise) | Consecutive(2) (= Auto) | 9.30 | 277 | 986 | 4.06 |
  | | **Conv1(6)** | **11.21 (+20.5%)** | 162 | **346** | 11.6 |
  | random walk | Consecutive(1) (= Auto) | 5.01 | 213 | 1041 | 3.84 |
  | | Conv1(6) | 5.01 (0%) | 138 | 390 | 10.3 |

  Conv1 at orders 4, 8 and 16 (generic path) decodes at ~180–190 MB/s, about 21 ns/value. The
  order-6 special case runs at ~10 ns/value. So Conv1 costs roughly **2.5–5.5x pco's other delta
  modes** in decode time.

### 1.3 FLAC (Xiph; format RFC 9639, 2024)
* **Format.** Per-subframe "fixed" polynomial predictors of order 0–4 have integer binomial
  coefficients, so they are linear over integers and need no shift. There is also quantized LPC
  up to order 32 with a precision of ≤15 bits and a `qlp_shift`. Residuals use partitioned Rice
  coding. Coefficients are forward-adaptive (stored per block), so the decoder is cheap and
  asymmetric.
* **Decoder SIMD history** (xiph.org/flac/changelog.html):
  * 1.3.1 (Nov 2014): "Improved decoding efficiency of all bit depths but especially so for 24
    bits for IA32".
  * 1.3.3 (Aug 2019): "Improve SIMD decoding of 24 bit files".
  * **1.4.0 (Sep 2022): "Removed all assembler and intrinsics code from the decoder to improve
    fuzzing, as they provided only a small speed benefit".**
  * 1.4.3 (Jun 2023): "Small improvement in decoder speed for BMI2-capable CPUs" (this is Rice
    decoding, not LPC).
  * 1.5.0 (Feb 2025): multithreaded *encoding*.
* **Mailing list.** M. van Beurden (flac-dev, 6 Jul 2022) reported the pre-1.4.0 git decoder as
  "up to about 4% slowdown" (64-bit) and ~10% for 24-bit in 32-bit builds vs 1.3.4. In 2013 he
  reported 1.3.0 decoding ~15% faster than 1.2.1. The ~5x figure for intra-block LPC SIMD is not
  something anyone reports. The SIMD that was removed only had a small effect.
* **Absolute decode speed.** I found no authoritative MB/s for libFLAC.
  * ERK 2023 (Železnik et al., "Overview of lossless audio codecs", i5-8300H, 61 CD tracks)
    reports FLAC --best ratio 1.794, encode 7.94 s, decode 7.99 s per file average. The other
    codecs: MPEG-4 ALS 1.830 / 16.44 / 13.01 s, Monkey's Audio 1.865 / 14.30 / 14.20 s, LINNE
    1.815 / 79.27 / 9.98 s. These are whole-tool times including I/O.
  * Local measurement through `ffmpeg -threads 1 ... -f null` on 10 min of synthetic stereo
    16-bit: FLAC -8 decodes 106 MB of PCM in 0.77 s (≥138 MB/s, ~780x realtime) and FLAC -5 in
    0.82 s. TTA takes 1.31 s, ALAC 1.53 s and WavPack (ffmpeg level 3) 2.98 s. Plain WAV
    passthrough takes 1.83 s, so ffmpeg's per-packet framework cost dominates. These are lower
    bounds only.
* **GPU.** Ye & McCreath, "FLAC decoding using GPU acceleration", ISPA 2018, report "5 times
  performance improvements" (GTX 1080 vs i7-6770K). The parallelism is *across frames*: they find
  frame boundaries via sync codes, then decode frames in parallel. They do not parallelise inside
  a frame's LPC. An ALAC-on-CUDA paper (IJECE 2018) calls ALAC "highly serialized ... not
  efficient enough to be used on GPU".

### 1.4 Other audio codecs (ratio vs speed reference points)
* **Shorten** (Robinson 1994). The fixed polynomial predictors and LPC with Rice coding are the
  template FLAC followed.
* **MPEG-4 ALS** (ISO/IEC 14496-3; Liebchen et al.).
  * Forward-adaptive quantized LPC (orders up to 1023, coefficients coded as PARCOR).
  * Long-term prediction: 5 taps at a lag that can be hundreds of samples.
  * An optional backward-adaptive RLS-LMS cascade mode.
  * Residuals use Rice or BGMC (block Gilbert-Moore, arithmetic) coding.
  * ERK 2023 numbers are above: ~2% better ratio than FLAC, ~1.6x slower decode.
* **WavPack, TTA, Monkey's Audio, OptimFROG.** These use *backward-adaptive* cascaded sign-LMS or
  NN filters, so decode cost ≈ encode cost.
  * Monkey's has the best ratio among mainstream codecs (ERK: 1.865).
  * OptimFROG max presets are reported to decode at only ~4x realtime (Hydrogenaudio wiki "Sac"
    page).
  * The general lesson is that each extra percent of ratio over FLAC costs an order of magnitude
    in decode speed.

### 1.5 Seismology: Steim1 / Steim2 (miniSEED)
* Steim1/2 store **first differences** packed into 32-bit words using 2-bit nibble codes per word.
  Steim2 has 7 sub-encodings (4–30 bit). The difference chain is carried across records (libmseed
  `mst_pack` docs).
* Reasons seismology stayed with first differences: the format must be robust to record-level
  loss, decoders must be simple and fixed-point (field digitisers), integer counts are the native
  data, and broadband seismic data at typical rates is dominated by noise.
* LPC has been studied for seismic data and beats Steim2 in the literature: Stearns et al., SPIE
  1993, "Techniques for geophysical data compression" (two-stage LP + residual coding), and a 2016
  *Computers & Geosciences* paper reporting files at 75–80% of original size, better than Steim2.
  I found no head-to-head FLAC-vs-Steim2 numbers.

### 1.6 Floats: Gorilla, Chimp, Elf, ALP, BUFF (no LPC, baseline speeds)
* **Gorilla** (Pelkonen et al., VLDB 2015). Delta-of-delta timestamps: *"about 96% of all time
  stamps can be compressed to a single bit"*. XOR floats: *"Roughly 51% of all values are
  compressed to a single bit"*. Overall *"1.37 bytes per point, a 12x reduction"*.
* **ALP** (Afroozeh, Kuffó, Boncz, SIGMOD 2024), Table 5, Ice Lake 8375C @3.5 GHz, tuples per
  cycle (compress / decompress):

  | Codec | Compress | Decompress |
  |---|---|---|
  | ALP | 0.487 | **2.609** |
  | Gorilla | 0.052 | 0.047 |
  | Chimp | 0.042 | 0.039 |
  | Chimp128 | 0.040 | 0.040 |
  | Patas | 0.060 | 0.157 |
  | Elf | 0.010 | 0.012 |
  | PDE | 0.002 | 0.387 |
  | Zstd | 0.035 | 0.101 |

  At 3.5 GHz that is ALP ≈ 9 G values/s (≈ 73 GB/s of f64, L1-resident) against Gorilla
  ≈ 0.16 G values/s. ALP's paper says Gorilla is 55x slower "since the latter has complex
  if-then-else ... and data dependencies".
* **BUFF** (Liu, Jiang, Paparrizos, Elmore, Franklin; PVLDB 14(11) 2021). Decomposed bounded
  floats with byte-sliced columns and SIMD in-situ predicates. I found no primary decode number;
  a third-party Rust port (buff-rs) claims ~600–750 M values/s.

### 1.7 LeCo (Liu, Zeng, Zhang; SIGMOD 2024, arXiv 2306.15374)
* LeCo is a linear-in-*index* regression per partition (constant = FOR, linear, polynomial up to
  degree 3, exp, log, and optionally sine terms), with bit-packed residuals. It is **not** an
  autoregression, so decode has no loop-carried dependency and gives O(1) random access.
* Ice Lake 8369B @2.7 GHz.
  * LeCo-fix decompression is "14%–34% slower than ... FOR" (an extra FP multiply per value).
  * Compression throughput, GB/s: FOR 0.81, Elias-Fano 0.58, Delta-fix 1.04, Delta-var 0.04,
    LeCo-fix 0.78, LeCo-var 0.02.
  * Random access is "an order of magnitude" faster than Delta.
* Higher-order regressors give "up to 64.7% improvement over LeCo-fix" on polynomial-pattern data.
  On `cosmos`, 2 sine terms reach 21.1% of original size vs 42.3% for the polynomial.
* This is the same niche as Vortex's Affine/LeCo encoding. LPC is complementary: it captures
  local oscillation and noise colour that index-polynomials cannot.

### 1.8 TurboPFor (powturbo)
* The README's "Time series" table (hardware not stated in the table) lists zigzag
  delta-of-delta (`bvzzenc32`) at 8,914 MB/s compress / 13,499 MB/s decompress. TurboPFor256
  delta/zigzag decompresses at ~13.3 GB/s and TurboPackV256 delta at 12.6 / 13.5 GB/s.
  The README claims "Speed > 10 GB/s compression and > 13 GB/s decompress".
* TurboTranspose/TPbyte (shuffle with integrated xor/zigzag delta) reaches ~9.4 / 9.1 GB/s.
* TurboPFor has FCM/DFCM float predictors (context hashing, not LPC). No LPC.

### 1.9 Time-series databases
* Gorilla: see above.
* **InfluxDB TSM** (docs): timestamps are delta-coded, scaled by the common divisor, then RLE or
  simple8b. Floats use Gorilla XOR. Integers use zigzag then simple8b or RLE.
* **TimescaleDB** (Tiger Data blog): integers and timestamps use "Delta-of-delta + Simple-8b with
  run-length encoding". Floats use Gorilla XOR, with dictionary and LZ fallbacks.
* **M4** (Jugel et al., VLDB 2014) is a visualisation-oriented min/max/first/last *aggregation*,
  not a lossless codec.
* No mainstream TSDB ships LPC. The highest-order predictor in wide use is delta-of-delta
  (order 2, integer coefficients).

### 1.10 General-purpose baselines (zstd README; i7-9700K @4.9 GHz, Silesia, lzbench)
| Codec | Ratio | Compress | Decompress |
|---|---|---|---|
| zstd 1.5.7 -1 | 2.896 | 510 MB/s | 1,550 MB/s |
| zstd --fast=4 | 2.146 | 665 MB/s | 2,050 MB/s |
| lz4 1.10.0 | 2.101 | 675 MB/s | 3,850 MB/s |
| snappy 1.2.1 | 2.089 | 520 MB/s | 1,500 MB/s |

---

## 2. Breaking the loop-carried dependency

### (a) Interleave K independent streams or lanes (the ANS trick)
* **Interleaved rANS** (Giesen, "Interleaved entropy coders", arXiv 1402.3392, 2014; i7-2600K
  3.4 GHz):
  * serial 202–266 MiB/s;
  * **2-way 338–501 MiB/s (1.62–1.88x)**;
  * **8-way SSE4.1 564–573 MiB/s (2.15–2.79x)**.
  * His 2015 blog post "rANS in practice" calls 2-way interleaving "(very roughly) around 1.4×
    faster" and says it can *"convert a very serial task into one that's much more amenable to
    superscalar execution."*
* **FastLanes** (Afroozeh & Boncz, PVLDB 16(9) 2023). The Unified Transposed Layout reorders
  1024 values so that *"all lanes handle completely independent DELTA sequences"*. Each lane holds
  a contiguous run of the original sequence: the 16-value example order is
  0,4,8,12,1,5,9,13,…. So the prediction distance stays 1, at the price of 1024/T bases per
  vector, which is *"overhead ... 1 bit per value"*.
  * FastLanes compares this with Lemire's D4/DM: batch-strided deltas make *"the differences ... 4x
    bigger"*, ~2 bits lost.
  * It also compares with "four cursors" (multiple bases, scalar ILP), which *"improves Scalar a
    little"*.
  * The Unified Transposed layout *"is by far fastest"*, and scalar code without the layout "can
    be >40x slower than SIMD".
* **Sprintz** gets 6 GB/s FIRE decode only because lanes = columns of a multivariate series. A
  single column gets no SIMD.
* **Local measurement: order-6 integer LPC inverse**, i64 accumulator, i32 data, `>>12`, 4 Mi
  values, transposed layout `x[t*L+lane]` where each lane is a contiguous sub-sequence:

  | Kernel | ns/value | MB/s | vs. serial |
  |---|---|---|---|
  | delta (prefix sum), serial | 0.62 | 6,460 | — |
  | delta, 32 lanes | 0.42 | 9,650 | — |
  | delta-of-delta (integer order 2), serial | 1.06 | 3,777 | — |
  | **LPC6 serial, i64 acc** | **8.4–8.8** | 452–475 | 1x |
  | LPC6 serial, i32 acc | 6.7–7.0 | 572–598 | 1.3x |
  | LPC6, 4 lanes | 2.9–3.1 | 1,309–1,382 | 2.9x |
  | LPC6, 8 lanes | 1.66–1.79 | 2,229–2,414 | 5.1x |
  | LPC6, 16 lanes | 1.10–1.19 | 3,375–3,627 | 7.6x |
  | **LPC6, 32 lanes** | **0.85–0.87** | 4,583–4,707 | **~10x** |
  | LPC6, 64 lanes, i32 acc | 0.45–0.47 | 8,439–8,871 | ~19x |

  The 32-lane i32-accumulator variant hit a codegen anomaly (2.8 ns). Kernels are hand-written,
  auto-vectorized Rust (a scratch benchmark; `lpc_kernels.rs` in this directory measures the same kernels).
  * Speed-up is near-linear in lanes until about 32 lanes, the latency-hiding limit.
  * With 16-bit-range data and ≤16-bit weights, i32 accumulation (pco Conv1 already uses 2k-bit
    arithmetic) doubles SIMD width.
* **Cost of lanes for LPC.** Each lane needs *p* warm-up samples, or must inherit them from the
  previous lane, which reintroduces a dependency. The options:
  * per-lane warm-ups coded by a cheaper predictor (FLAC stores warm-ups verbatim);
  * lanes much longer than FastLanes' 32 (e.g. 32 lanes × 1024 values = 32 Ki-value blocks, so
    order 6 costs 0.6%);
  * lane j seeded from the tail of lane j−1, decoded in a second short pass.

  With FastLanes' 1024-value vectors (32 lanes × 32 values for 32-bit), order-6 warm-ups would be
  ~19% of values.
* **Ratio cost.** Lanes have no ratio cost apart from the warm-ups, because each lane still
  predicts from its true neighbours. The weights can be shared across lanes, as in pco's per-chunk
  weights.

### (b) Parallel prefix / scan of linear recurrences
* **Blelloch**, "Prefix Sums and Their Applications" (CMU-CS-90-190, 1990), §1.4.
  * A first-order `x_i = x_{i-1}⊗a_i ⊕ b_i` becomes a scan over pairs.
  * An order-m recurrence becomes a scan over m×m companion matrices, at cost
    `O((n/p + lg p)·T_{m×m}(m))`. That is *"not optimal in m — ... a factor of O(T_M⊗M(m)/m) more
    work than the sequential algorithm."*
* **Martin & Cundy**, "Parallelizing Linear Recurrent Neural Nets Over Sequence Length", ICLR
  2018: up to 9x on GPU.
* **Caveat for lossless integer LPC.** The scan needs ⊕ to be associative and ⊗ to distribute
  over it. With quantized LPC, `x_i = r_i + ((Σ w_j x_{i−j} + bias) >> q)`, the floor/shift makes
  the map non-linear, so a matrix scan is **not bit-exact**.
  * It *is* exact for integer-coefficient predictors under wrapping arithmetic (q = 0): delta,
    delta-of-delta, pco Consecutive orders 1–7, FLAC fixed orders 0–4.
  * Those are already *k* prefix sums, which SIMD scans handle well (FastLanes/TurboPFor speeds
    above).

### (c) DSP block / look-ahead IIR formulations
* van der Horst et al., "Recursive filtering on a vector DSP with linear speedup", ASAP 2005:
  earlier vector-DSP IIR programs had speedup "bounded by the order of the filter". Their
  block-state formulation gives "doubling the vector size doubles the throughput".
* Zhai & Paris, "Fast Cascaded Recursive Filtering via a Block-Matrix Reformulation", arXiv
  2607.14054, July 2026: cyclic reduction reduces dependency depth from O(N) to O(log N).
  * "up to 10×" fewer cycles per sample than scalar for 16th-order filters on three AVX2 Intel
    microarchitectures.
  * ~618 MS/s on one Meteor Lake core, 8x over SciPy `sosfilt`.
* Nehab, Maximo, Lima, Hoppe, "GPU-efficient recursive filtering and summed-area tables", SIGGRAPH
  Asia 2011: block-parallel recursive filters.
* All of these are floating-point and tolerate reassociation error. As with (b), they are not
  bit-exact for quantized integer LPC.

### (d) Restrict to fixed integer predictors
* Delta = 1 prefix sum; delta-of-delta = 2; pco `Consecutive(k)` = k.
* Locally, delta runs at 0.62 ns/value serial and delta-of-delta at 1.06 ns/value serial. In pco,
  Consecutive(1–3) decode at ~0.75–1.05 GB/s, bounded by tANS and bit-unpacking rather than the
  prefix sums.
* FLAC's fixed orders 0–4 and Shorten's polynomial predictors are the audio equivalents.

### (e) Sprintz/FIRE's approach
* Restrict to a 1-parameter AR(2) (`δ_i = α δ_{i−1}`), learned per block, and vectorize across
  variables.
* Its kernel speed is ~delta speed only because it has many columns. For one column it is serial
  and no faster than an order-2 LPC.

---

## 3. LPC residuals plus ANS / tANS / rANS
* **pco Conv1** is LPC + binning + 4-way interleaved tANS: the most direct existing LPC+ANS
  combination in a columnar codec.
  * Local measurement: tANS and bit-unpacking alone decode at ~1 GB/s.
  * Adding the serial order-6 LPC drops this to ~0.35–0.4 GB/s.
  * So with ANS the LPC recurrence, not the entropy decoder, becomes the bottleneck. Interleaving
    the LPC lanes would put it back near tANS speed.
* **Audio.** Mainstream codecs use Rice (FLAC, Shorten, ALS default, TTA), adaptive Golomb or
  bit-level schemes (WavPack), range coders (Monkey's Audio), or BGMC (ALS). I found no deployed
  lossless audio codec that uses rANS/tANS. One ResearchGate-listed paper (biomedical signals)
  codes prediction error plus a variance estimate with ANS, reporting ratios of 2.92–3.43. I
  could not verify its details or speed.
* **Sprintz.** Huffman on top of bit-packing caps decode at ">500MB/s", versus multiple GB/s
  without it. This is a useful data point that a serial entropy stage dominates once prediction
  is cheap.
* **FSE/tANS** (zstd, LZFSE) and AV1/Daala multi-symbol range coders are not paired with LPC.
* I found **no published measurement** of an LPC + interleaved-ANS decoder where both the
  predictor and the coder are lane-interleaved.

---

## 4. Datasets where higher-order LP beats delta
* **Audio PCM.** This is the canonical case: FLAC LPC up to order 32 versus fixed order ≤4. FLAC's
  own default presets use LPC for most music subframes. ERK 2023 (61 CD tracks): FLAC 1.794,
  ALS 1.830, Monkey's 1.865.
* **Vibration, accelerometer and motion capture.** Sprintz: FIRE wins on 74/85 UCR datasets at
  16-bit. MSRC-12 (80 vars, Kinect), PAMAP (31 vars, IMU) and UCI Gas (18 vars) all favour Sprintz
  over Zstd/LZ4.
* **ECG/EEG.** Bannajak et al. 2023 (IJERPH 20:2753), adaptive fixed orders 1–4 + Golomb-Rice:
  CR 3.16 (PTB, 1 kHz/16-bit), 3.75 (EDB), 3.52 (MIT-BIH). Notably, Levinson-Durbin coefficients
  "round to [2, −1] for order 2 across all three datasets", so delta-of-delta captures most of
  the gain.
* **Seismic waveforms.** LP + residual coding beats Steim2 in the literature (§1.5). Ratios vary
  with how noisy the data is.
* **Power-grid and RF waveforms** (sinusoidal): a 2-tap LPC predicts a pure sinusoid exactly.
  LeCo's `cosmos` sine-mixture shows the same with index-based sine regressors (21% vs 42% size).
  The "Context-Aware Lossless and Lossy Compression of RF Signals" paper (PMC10098950) covers RF
  IQ data.
* **Local synthetic results** (pco): resonant signal +7.6% (order 6) and +14% (order 16) over the
  best Consecutive; smooth trajectory +20.5%; random walk 0%.
* **Where LPC does *not* help:**
  * step-like or state-switching data, such as AMPD gas/water in Sprintz and electricity
    on/off. Dictionary/LZ/RLE wins there;
  * random walks (delta is optimal);
  * low-bit-width noisy data (Sprintz 8-bit: FIRE wins only on 51/85);
  * typical DB columns (IDs, timestamps, prices). pco's benchmark datasets (Air Quality, Taxi,
    r/place, Housing, Payments, Twitter) and the ALP/Chimp float datasets are dominated by these,
    and pco `Auto` never selects Conv1.
* **Benchmark suites to reuse:** UCR Time Series Archive (Sprintz); pco's datasets and FCBench
  (`fcbench_*.csv` in the pco repo); ALP's 30 float datasets (Chimp/Elf sets plus Basel-Wind,
  Btc-Price, POI etc.); TSBS (Timescale's synthetic devops/IoT generator, low LPC value);
  MIT-BIH/PTB-XL (ECG); IRIS/FDSN miniSEED archives (seismic); and audio corpora.

---

## Summary table

| Encoding | Predictor | Entropy stage | Ratio (dataset) | Compress | Decompress | Hardware / source |
|---|---|---|---|---|---|---|
| Sprintz Delta / FIRE | delta / learned AR(2) per column | bit-pack (+Huffman) | beats Zstd-9 on UCR (rank) | 200–600 MB/s (scalar) | >3 GB/s; FIRE kernel 6 GB/s; >500 MB/s with Huffman | i7-4960HQ 2.6 GHz; Blalock 2018 |
| pco Consecutive | integer polynomial order 1–7 | 4-way tANS | 6.79–10.0 (r/place, Taxi, Air Q) | 470–553 MB/s | 2.4–3.8 GB/s | M3 Max 1 core; pco repo CSV |
| pco (paper) | auto | 4-way tANS | +29–94% vs best alternative | — | 1.4–5.5 GiB/s | Xeon 8488C; arXiv 2502.06112 |
| pco Conv1(6) | LPC order 6 (LS-fit, i32 weights, shift) | 4-way tANS | +7.6% / +20.5% / 0% vs best Consecutive (synthetic) | 133–162 MB/s | 346–398 MB/s (vs ~1 GB/s Consecutive) | local Xeon 2.1 GHz VM |
| pco Conv1(4/8/16) | LPC generic path | 4-way tANS | up to +14% | 106–157 MB/s | ~185 MB/s | local |
| FLAC | fixed 0–4 / LPC ≤32 | partitioned Rice | 1.794 (CD audio, --best) | — | ≥138 MB/s via ffmpeg (lower bound) | i5-8300H (ERK 2023); local |
| MPEG-4 ALS | LPC ≤1023 + LTP (+RLS-LMS) | Rice / BGMC | 1.830 | ~2x FLAC time | ~1.6x FLAC time | ERK 2023 |
| Monkey's Audio | cascaded NN/LMS (backward) | range coder | 1.865 | ~1.8x FLAC time | ~1.8x FLAC time | ERK 2023 |
| OptimFROG (max) | adaptive (backward) | proprietary | best | very slow | ~4x realtime | Hydrogenaudio wiki |
| Steim2 | first difference | 2-bit-nibble word packing | — | — | — | libmseed docs |
| Gorilla | DoD timestamps / XOR floats | variable-bit | 1.37 B/point (12x) | 0.052 t/cycle | 0.047 t/cycle | Ice Lake 3.5 GHz (ALP Table 5) |
| Chimp / Chimp128 / Elf / Patas | XOR | variable-bit | — | 0.04 / 0.04 / 0.01 / 0.06 t/c | 0.039 / 0.040 / 0.012 / 0.157 t/c | same |
| ALP | decimal → int + FFOR | bit-pack | best of floats on 30 datasets | 0.487 t/cycle | 2.609 t/cycle | same |
| LeCo-fix | linear in index | bit-pack | better than FOR (40.9% avg on locally-easy data) | 0.78 GB/s | 14–34% slower than FOR | Ice Lake 8369B 2.7 GHz |
| FastLanes Delta | delta, transposed lanes | bit-pack | — | — | up to 70 tuples/cycle (8-bit) | 6 platforms; PVLDB 2023 |
| TurboPFor zigzag DoD | delta-of-delta | bit-pack/PFor | — | 8.9 GB/s | 13.5 GB/s | hardware not stated (README) |
| zstd -1 / lz4 | LZ | Huffman+FSE / none | 2.896 / 2.101 (Silesia) | 510 / 675 MB/s | 1,550 / 3,850 MB/s | i7-9700K 4.9 GHz |
| LPC6 inverse kernel only | order 6, i64 acc | — | — | — | serial 0.45 GB/s → 32 lanes 4.6 GB/s → 64 lanes (i32) 8.9 GB/s | local |

`t/cycle` = tuples per CPU cycle.

---

## Design implications for a Vortex LPC encoding
1. **Store the residuals in a lane-interleaved (transposed) layout**, with one shared weight
   vector per chunk and each lane a long contiguous sub-sequence. This is the LPC analogue of
   interleaved rANS, is bit-exact, and measured ~10x locally.
   * Use lanes longer than 32 values (e.g. 32 lanes × 1024) so warm-ups cost <1%.
   * Alternatively, code warm-ups with delta. FastLanes' 1024-value granularity alone gives a
     ~19% warm-up overhead at order 6.
2. **Use i32 accumulation** when `bits(data) + bits(weights) + log2(order) ≤ 31`, which typical
   16-bit-range sensor and audio data satisfy. Otherwise use i64 (pco's choice).
3. **Pick the order from 2/4/6/8**, and have the compressor try Delta/DoD (FastLanes) first. In
   pco and ECG data, LPC rarely beats DoD by more than ~10–20% except on oscillatory waveforms.
4. **Bit-pack the residuals** (FastLanes BitPacking, decodable at ≫ LPC speed) for speed. Leave
   entropy coding to a Pco-style path when ratio matters, since ANS rather than LPC becomes the
   limit once LPC is lane-interleaved.
5. **Do not rely on scan or companion-matrix tricks for quantized LPC.** They are exact only for
   integer-coefficient (no-shift) predictors, which are already prefix sums.

---

## Sources
* Sprintz: https://arxiv.org/abs/1808.02515 (PDF https://arxiv.org/pdf/1808.02515); code https://smarturl.it/sprintz
* Pcodec paper: https://arxiv.org/abs/2502.06112 ; HTML https://arxiv.org/html/2502.06112
* pcodec repo, format and Conv1 source: https://github.com/pcodec/pcodec (`docs/format.md`, `pco/src/delta/conv1.rs`, `pco/src/chunk_config.rs`, `docs/benchmark_results/*.csv`)
* pcodec releases: https://github.com/pcodec/pcodec/releases ; docs https://pcodec.readthedocs.io ; numcodecs-wasm pco https://numcodecs-wasm.readthedocs.io/en/latest/api/numcodecs_wasm_pco/
* FLAC changelog: https://xiph.org/flac/changelog.html ; flac-dev 2022-07 https://lists.xiph.org/pipermail/flac-dev/2022-July/006551.html ; flac-dev 2013-03 https://lists.xiph.org/pipermail/flac-dev/2013-March/003856.html
* FLAC GPU (Ye & McCreath 2018): https://researchportalplus.anu.edu.au/en/publications/flac-decoding-using-gpu-acceleration/
* ALAC CUDA (IJECE 2018): https://ijece.iaescore.com/index.php/IJECE/article/view/10100
* ERK 2023 "Overview of lossless audio codecs": https://erk.fe.uni-lj.si/2023/papers/zeleznik(overview_of).pdf
* MPEG-4 ALS: https://en.wikipedia.org/wiki/Audio_Lossless_Coding ; https://www.researchgate.net/publication/240320710_The_MPEG-4_audio_lossless_coding_ALS_standard_-_Technology_and_applications
* OptimFROG/Sac: https://wiki.hydrogenaud.io/index.php?title=Sac ; https://en.wikipedia.org/wiki/OptimFROG
* Steim/miniSEED (libmseed mst_pack): https://manpages.ubuntu.com/manpages/jammy/man3/msr_pack.3.html
* Seismic LP: https://ui.adsabs.harvard.edu/abs/1993SPIE.1941..212S/abstract ; https://www.sciencedirect.com/science/article/abs/pii/S0098300416305891
* Gorilla: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
* ALP: https://ir.cwi.nl/pub/33334/33334.pdf
* BUFF: https://www.vldb.org/pvldb/vol14/p2586-liu.pdf ; https://docs.rs/buff-rs
* LeCo: https://arxiv.org/abs/2306.15374
* FastLanes: https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf
* TurboPFor: https://github.com/powturbo/TurboPFor-Integer-Compression
* InfluxDB TSM: https://docs.influxdata.com/influxdb/v1/concepts/storage_engine/
* TimescaleDB: https://www.tigerdata.com/blog/time-series-compression-algorithms-explained
* zstd benchmarks: https://github.com/facebook/zstd/blob/dev/README.md
* Interleaved rANS: https://arxiv.org/abs/1402.3392 (https://ar5iv.labs.arxiv.org/html/1402.3392) ; https://fgiesen.wordpress.com/2015/12/21/rans-in-practice/ ; https://fgiesen.wordpress.com/2014/02/18/rans-with-static-probability-distributions/
* Blelloch 1990: https://www.cs.cmu.edu/~guyb/papers/Ble93.pdf
* Martin & Cundy 2018: https://arxiv.org/abs/1709.04057
* Vector-DSP IIR (2005): https://research.tue.nl/en/publications/recursive-filtering-on-a-vector-dsp-with-linear-speedup/
* Block-matrix IIR (2026): https://arxiv.org/abs/2607.14054
* Nehab et al. 2011: https://hhoppe.com/proj/recursive/
* ECG adaptive LP (Bannajak 2023): https://pmc.ncbi.nlm.nih.gov/articles/PMC9915293/
* RF signals: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC10098950/
* LP+ANS biomedical paper: a web-search snippet only, with no reliable URL attribution, so it is not cited as a source.

## Not found or not verified
* No published Conv1 compression-gain or decode-speed numbers from the pcodec author (no blog,
  paper section or PR text found). The Conv1 numbers here are local measurements.
* No authoritative absolute libFLAC decode MB/s on named hardware. No WavPack/TTA numbers beyond
  the local ffmpeg lower bounds.
* No primary BUFF decode throughput.
* No head-to-head FLAC-vs-Steim2 seismic ratios.
* No measured LPC decoder that interleaves both the predictor and an ANS coder.
* The TurboPFor README table does not state its hardware.
