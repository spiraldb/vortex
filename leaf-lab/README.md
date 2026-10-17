# Leaf entropy-coder lab

Handoff for an experiment stacked on `ji/entropy-bins`: is tANS the right id coder for
`vortex-entropy-bins`, and which other entropy coders are worth having as a **leaf** (bottom of
the cascade) in Vortex? Pre-transforms (FoR, Dict, RunEnd, Delta, ALP...) are out of scope: the
cascade already chooses them, so every coder here only sees what the default BtrBlocks cascade
left in a leaf.

This crate is a scratch research harness, not a production encoding. It is a workspace member so
it builds against the branch's crates; drop it before merging `ji/entropy-bins`.

## TL;DR

- The pco bins do the work. With identical bins and offsets, every static id coder lands within
  ~1 point of the order-0 entropy bound: tANS (entropy-bins) and 16-lane rANS 80.0% of today's
  leaf bytes, Tunstall-16 80.6%, Huffman 81.0%, bound 79.2%. tANS is the right choice.
- Only adaptive coding beats the static bound: an adaptive binary range coder reset per block
  reaches 77.7% (order 0) / 77.0% (order 1), but decodes serially, ~15x slower than tANS.
- Golomb-Rice / Exp-Golomb only help geometric leaves (delta-coded RunEnd ends, sparse patches)
  and lose to the binned coders even there. 96% overall: not worth a scheme.
- Tunstall is the GPU-shaped alternative: fixed-width codewords, no per-lane state, 0.6 points
  behind tANS. 32 lanes (warp width) cost +0.25 pt for Huffman, +0.75 pt for rANS (states).
- On whole files, entropy-bins as a pure leaf (lag 0, 4K blocks) removes 12.7% of all compressed
  bytes; order-1 adaptive would remove 14.5%. With its fused lag, entropy-bins removes 19.6%:
  the lag is a transform, and its gain says the cascade leaves delta structure on the table.
- Amenable leaves: plain BitPacked (`*Timing`, `ClientEventTime`), dict codes (u8 codes the
  cascade left uncompressed go to ~25%), RunEnd ends, sparse patches, ALP ints, RLE side arrays,
  FSST lengths. Not amenable: FoR residuals of hashes (`WatchID`, `HID`) and integer dict values.

## Corpus

`extract` reads parquet, compresses each column in 1 MB slices with the default BtrBlocks
compressor (`unrestricted()`, entropy-bins not enabled) and dumps every integer leaf the cascade
ended on (`fastlanes.bitpacked` or `vortex.primitive`, >= 4096 values) with its tree path.

- ClickBench `hits_0`, `hits_1` and NYC yellow taxi 2023-01; batches 0 and 4 (131072 rows) of each.
- 399 leaves, 22.6M values, 32.3 MB of leaf bytes = 63% of the 51.1 MB the cascade produced for
  those columns (the rest is FSST/varbin bytes and other non-integer data).
- 45 leaves are 64-bit, 354 narrow. Most bytes are plain BitPacked (7.9 MB), dict codes (7.7 MB),
  FoR residuals (6.5 MB) and integer dict values (3.9 MB).

## Coders

All coders run per independently decodable block of 4096 values with one model per 2^18-value
chunk, as entropy-bins does. Sizes include every header the decoder needs, model tables, and a
4-byte block index. Every coder roundtrip is asserted on every leaf.

| Name in results | What |
|---|---|
| `cascade` | The leaf as the cascade stored it (bytes from `nbytes()`) |
| `bitpacked` | FastLanes BitPacked at its best width with patches, re-encoded for timing |
| `eb`, `eb-lag0`, `eb-4k` | vortex-entropy-bins BALANCED; forced lag 0; forced lag 0 and 4K blocks. Buffers plus an estimate of the bins metadata |
| `bins+H0` | Static order-0 entropy of the ids with the same bins, plus offsets and tables: the floor for any static id coder |
| `bins+huff12x{16,32}` | Canonical Huffman, 12-bit length limit, 16/32 interleaved bit streams, table decode |
| `bins+rans12x{16,32}` | Static rANS, 12-bit probabilities, 16/32 lanes of 32-bit state sharing one 16-bit word stream |
| `bins+tunstall{12,16}` | Tunstall (variable-to-fixed), 2^12 / 2^16 dictionary entries |
| `bins+adaptive-o{0,1}` | LZMA-style adaptive binary range coder over a binary-tree binarization of the id; order 1 conditions on the previous id. Reset every block |
| `sym+*` | Same coders on the values directly (no bins), only when the value range is < 4096 |
| `rice`, `expgolomb` | On value minus block minimum, one parameter per 128 values, per-miniblock bit offsets stored for parallel decode |

Bins are trained exactly like entropy-bins (pco Classic mode, no delta, level 8, at most 64
bins); `bins+*` differ from each other only in the id coder.

## Results

`python3 scripts/analyze.py results/results.jsonl all` reproduces these tables.

### Compression ratio (leaf raw bytes / coded bytes; % of today's leaf bytes)

Raw is the leaf's own width (a u8 dict code counts 1 byte), not the original column.

| Coder | All | 64-bit | Narrow | Bits/value |
|---|---|---|---|---|
| Cascade today | 2.34x (100%) | 2.47x | 2.24x | 11.44 |
| Rice | 2.43x (96.3%) | 2.51x | 2.37x | 11.01 |
| Exp-Golomb | 2.43x (96.5%) | 2.49x | 2.38x | 11.04 |
| Huffman x16 | 2.89x (81.0%) | 2.75x | 3.02x | 9.26 |
| Tunstall-16 | 2.91x (80.6%) | 2.76x | 3.04x | 9.22 |
| rANS x16 | 2.93x (80.0%) | 2.77x | 3.08x | 9.15 |
| tANS (eb-4k, lag 0) | 2.93x (80.0%) | 2.77x | 3.08x | 9.15 |
| Static H0 bound | 2.96x (79.2%) | 2.78x | 3.13x | 9.06 |
| Adaptive order 0 | 3.02x (77.7%) | 2.79x | 3.24x | 8.89 |
| Adaptive order 1 | 3.04x (77.0%) | 2.80x | 3.28x | 8.81 |
| *eb with fused lag* | *3.40x (68.9%)* | *3.12x* | *3.68x* | *7.88* |

Small-alphabet leaves (285): coding symbols directly instead of through bins gains ~3% for
Huffman (6.74x -> 6.97x) and rANS (7.11x -> 7.33x); Tunstall-16 loses (7.00x -> 6.19x).

### By leaf category (ratio)

| Category | Cascade | Rice | Huffman | Tunstall-16 | rANS | tANS | H0 | Adaptive o0 | Adaptive o1 | *eb + lag* |
|---|---|---|---|---|---|---|---|---|---|---|
| Plain BitPacked | 2.75 | 3.02 | 4.19 | 4.24 | 4.27 | 4.28 | 4.33 | 4.42 | 4.52 | *5.26* |
| Dict codes | 1.28 | 1.29 | 1.73 | 1.71 | 1.75 | 1.75 | 1.78 | 1.85 | 1.88 | *2.36* |
| FoR residuals | 2.67 | 2.66 | 2.71 | 2.73 | 2.73 | 2.73 | 2.74 | 2.76 | 2.79 | *3.19* |
| Int dict values | 1.00 | 1.00 | 1.01 | 1.01 | 1.01 | 1.01 | 1.01 | 1.01 | 1.01 | *1.01* |
| ALP-encoded ints | 5.34 | 6.00 | 7.29 | 7.17 | 7.35 | 7.35 | 7.45 | 7.40 | 7.06 | *7.30* |
| RLE indices/values | 4.79 | 4.50 | 5.79 | 6.58 | 6.44 | 6.49 | 6.76 | 6.82 | 7.03 | *6.93* |
| RunEnd values | 1.36 | 1.37 | 1.61 | 1.61 | 1.62 | 1.61 | 1.63 | 1.64 | 1.65 | *1.68* |
| RunEnd ends (delta) | 4.23 | 5.93 | 6.60 | 6.49 | 6.56 | 6.54 | 6.72 | 6.89 | 6.77 | *6.39* |
| FSST lengths/offsets | 2.77 | 3.05 | 3.41 | 3.37 | 3.41 | 3.41 | 3.46 | 3.50 | 3.48 | *3.33* |
| Sparse patches | 3.29 | 4.35 | 5.77 | 5.72 | 5.75 | 5.72 | 5.88 | 6.05 | 6.10 | *5.57* |

### Timing (one core, Xeon 2.1 GHz with AVX-512, best of >= 3 runs)

Only BitPacked and entropy-bins are tuned (entropy-bins uses AVX-512). The other coders are
straightforward scalar Rust; their full decode is held back by a scalar per-value offset merge, so
"ids only" is the better measure of the coder itself.

| Coder | Encode Mv/s | Encode incl. bin training | Decode Mv/s | Decode GB/s | Ids-only decode Mv/s |
|---|---|---|---|---|---|
| BitPacked | 416 | — | 6,132 | 21.1 | — |
| eb with lag | 17 | — | 1,091 | 3.65 | — |
| eb-4k (lag 0) | 33 | — | 1,296 | 4.34 | — |
| Huffman x16 | 135 | 42 | 228 | 0.76 | 841 |
| rANS x16 | 203 | 47 | 139 | 0.47 | 257 |
| Tunstall-12 | 130 | 42 | 223 | 0.75 | 813 |
| Tunstall-16 | 9 | 8 | 233 | 0.78 | 953 |
| Adaptive o0 / o1 | 69 / 68 | 32 / 32 | 66 / 65 | 0.22 | 83 / 82 |
| Rice | 78 | — | 189 | 0.63 | — |
| Exp-Golomb | 67 | — | 125 | 0.42 | — |

Bin training plus the id/offset split runs at 61 Mv/s and dominates every binned coder's encode.
Tunstall-16 encode is slow because it rebuilds a 65536-entry dictionary per chunk.

## Reproduce

```bash
leaf-lab/scripts/fetch_data.sh /tmp/leaf-data
cargo build --release -p leaf-lab
target/release/extract /tmp/leaf-data/leaves /tmp/leaf-data/hits_0.parquet /tmp/leaf-data/hits_1.parquet /tmp/leaf-data/taxi.parquet
target/release/lab /tmp/leaf-data/leaves            # ~4.5 min; optional 2nd arg filters leaf ids
python3 leaf-lab/scripts/analyze.py /tmp/leaf-data/leaves/results.jsonl all
```

`results/` holds the run these numbers come from: `manifest.jsonl` (one line per leaf: source
column, tree path, dtype, cascade bytes), `columns.jsonl` (per column compressed size and top
encoding) and `results.jsonl` (per leaf, per coder: `bytes`, decode `ns`, `ids_ns`, `enc_ns`).
The raw leaf values (73 MB) are not committed; `extract` regenerates them.

## Layout

- `src/bin/extract.rs`: parquet -> cascade -> leaf dump.
- `src/bin/lab.rs`: runs every coder on every leaf, checks roundtrips, writes `results.jsonl`.
- `src/bins.rs`: pco bin training and the id/offset split and merge.
- `src/huff.rs`, `src/rans.rs`, `src/tunstall.rs`, `src/rc.rs`: id coders behind `SymbolCoder`.
- `src/golomb.rs`: Rice and Exp-Golomb.
- `scripts/analyze.py`: tables above.

## Caveats

- Corpus is two datasets; no TPC-H/TPC-DS, no float-heavy data beyond what ALP turned into ints.
- entropy-bins metadata (bins, weights) is estimated from retrained bins; its buffers are exact.
- The scalar prototypes understate achievable decode speed for Huffman, rANS and Tunstall; tuned
  SIMD/GPU versions would need to be built before comparing speed against tANS seriously.
- Adaptive models reset every 4K block; larger blocks would favour them, at a random-access cost.

## Possible next steps

1. Decide whether an adaptive (order-0/1) leaf is worth a slow, serial "cold storage" scheme:
   ~3 points of leaf bytes for ~15x slower decode.
2. A GPU decoder for entropy-bins' layout, or a Tunstall variant, if GPU scan is the target.
3. Investigate why the cascade misses the delta structure the fused lag finds (dict codes of
   clustered columns, sorted timestamps like `ClientEventTime`): that is the larger lever
   (another ~11 points) and belongs in the cascade, not the leaf.
4. Leaves the cascade leaves as `vortex.primitive` (e.g. u8 dict codes) are the cheapest wins for
   any entropy-coded leaf scheme.
