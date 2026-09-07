# Pull and Push: Evaluation Output

Raw output of the two TPC-H evaluators behind [`morsel-pull-and-push.md`](morsel-pull-and-push.md),
run on 2026-09-03 on a 14-core Apple Silicon host with segments in memory. Wall times are the
median of three iterations with the interval in brackets. Every configuration reproduced V1's
dtype, row count, and ordered content before it was timed.

## Push crate, both execution modes

```bash
TPCH_EXECUTION_MODES=pull,push TPCH_MORSEL_ONLY=1 TPCH_INCLUDE_V1=1 TPCH_ITERATIONS=3 \
  cargo run --release -p vortex-morsel-push --features _test-harness --bin tpch-push-eval -- 1
```

**Evaluator banner**

lineitem SF=1: 6001215 rows (6001215 generated), 16 columns,          733 natural splits; generated in 1907.041ms, written in 3003.389ms
written through the btrblocks compressing pipeline (repartition 8192 rows -> coalesce 1048576B -> compress -> buffer -> chunk -> flat); no zone maps, no dict layout
segment payloads: 1789 segments, 174419156 bytes total, 3780/102412/393492 bytes min/median/max
host: 14 available logical CPUs; segments in memory; one untimed warm-up + 3 grouped iterations per configuration, median reported
both executors use workers prepared outside the timed interval

schema: {l_orderkey=i64, l_partkey=i64, l_suppkey=i64, l_linenumber=i32, l_quantity=decimal(15,2), l_extendedprice=decimal(15,2), l_discount=decimal(15,2), l_tax=decimal(15,2), l_returnflag=utf8, l_linestatus=utf8, l_shipdate=vortex.date[days](i32), l_commitdate=vortex.date[days](i32), l_receiptdate=vortex.date[days](i32), l_shipinstruct=utf8, l_shipmode=utf8, l_comment=utf8}

### Q6 — 114160 rows out (1.90% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 15.164ms [14.793ms,16.554ms] | 1.00x | 9.000ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 2.802ms [2.690ms,3.316ms] | 0.18x | 0.793ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 1.566ms [1.395ms,1.843ms] | 0.10x | 0.325ms | 46 | 0/0/0 | 4.72 [4,5] | 1.22 [1,1] | 1.22 [1,1] | 0.26 (12/46, max 1) | 56 | 34559892 | 34559892 | 0/0/0 | 105/0/0 | 56 | 8.133ms | 167 | 63 |
| D  morsel-push (x14, 131072r) | 1.595ms [1.563ms,1.936ms] | 0.11x | 0.338ms | 46 | 703/460/0 | 4.80 [4,5] | 1.26 [1,1] | 1.26 [1,1] | 0.28 (13/46, max 1) | 58 | 34559892 | 34559892 | 460/0/0 | 103/0/0 | 58 | 7.840ms | 166 | 64 |

### Q1 — 5916591 rows out (98.59% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 5.416ms [5.111ms,5.486ms] | 1.00x | 3.086ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.544ms [1.515ms,1.623ms] | 0.29x | 0.435ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 1.026ms [0.971ms,1.347ms] | 0.19x | 0.218ms | 46 | 0/0/0 | 8.80 [8,9] | 8.02 [8,8] | 1.02 [1,1] | 0.48 (21/46, max 2) | 369 | 39911260 | 39911260 | 0/0/0 | 22/0/0 | 369 | 22.338ms | 394 | 20 |
| D  morsel-push (x14, 131072r) | 1.031ms [0.915ms,1.132ms] | 0.19x | 0.195ms | 46 | 1279/736/0 | 8.91 [8,9] | 8.02 [8,8] | 1.02 [1,1] | 0.80 (11/46, max 8) | 369 | 39911260 | 39911260 | 736/0/0 | 22/0/0 | 369 | 19.620ms | 396 | 18 |

### Q14 — 75983 rows out (1.27% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 5.923ms [5.768ms,6.941ms] | 1.00x | 3.718ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.315ms [1.310ms,1.389ms] | 0.22x | 0.317ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 0.824ms [0.742ms,0.836ms] | 0.14x | 0.167ms | 46 | 0/0/0 | 3.87 [3,4] | 3.07 [3,3] | 1.07 [1,1] | 0.46 (21/46, max 1) | 141 | 43548036 | 43548036 | 0/0/0 | 20/0/0 | 141 | 12.569ms | 168 | 16 |
| D  morsel-push (x14, 131072r) | 0.766ms [0.748ms,0.855ms] | 0.13x | 0.170ms | 46 | 512/322/0 | 3.83 [3,4] | 3.00 [3,3] | 1.00 [1,1] | 0.13 (2/46, max 3) | 138 | 43548036 | 43548036 | 322/0/0 | 23/0/0 | 138 | 3.587ms | 168 | 16 |

### Q15 — 225954 rows out (3.77% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 5.908ms [5.582ms,7.065ms] | 1.00x | 3.744ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.261ms [1.255ms,1.631ms] | 0.21x | 0.316ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 0.862ms [0.839ms,1.058ms] | 0.15x | 0.157ms | 46 | 0/0/0 | 3.93 [3,4] | 3.00 [3,3] | 1.00 [1,1] | 0.13 (6/46, max 1) | 138 | 40547204 | 40547204 | 0/0/0 | 23/0/0 | 138 | 6.454ms | 173 | 11 |
| D  morsel-push (x14, 131072r) | 0.793ms [0.711ms,0.888ms] | 0.13x | 0.183ms | 46 | 509/322/0 | 3.78 [3,4] | 3.00 [3,3] | 1.00 [1,1] | 0.07 (1/46, max 3) | 138 | 40547204 | 40547204 | 322/0/0 | 23/0/0 | 138 | 5.397ms | 166 | 18 |

### Q12 — 108434 rows out (1.81% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 15.747ms [15.341ms,16.457ms] | 1.00x | 9.255ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 2.819ms [2.779ms,3.082ms] | 0.18x | 0.596ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 1.833ms [1.804ms,1.858ms] | 0.12x | 0.381ms | 46 | 0/0/0 | 8.96 [4,10] | 3.00 [3,3] | 1.00 [1,1] | 0.02 (1/46, max 1) | 138 | 39693404 | 39693404 | 0/0/0 | 69/0/0 | 138 | 13.419ms | 226 | 234 |
| D  morsel-push (x14, 131072r) | 2.075ms [1.808ms,2.075ms] | 0.13x | 0.436ms | 46 | 1299/782/0 | 8.54 [3,10] | 3.00 [3,3] | 1.00 [1,1] | 0.24 (4/46, max 3) | 138 | 39693404 | 39693404 | 782/0/0 | 69/0/0 | 138 | 15.849ms | 215 | 245 |

### Q19 — 3599028 rows out (59.97% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 21.387ms [21.112ms,21.589ms] | 1.00x | 3.559ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 5.131ms [5.088ms,5.290ms] | 0.24x | 1.122ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 2.178ms [2.051ms,2.421ms] | 0.10x | 0.447ms | 46 | 0/0/0 | 14.96 [13,15] | 13.00 [11,13] | 1.04 [1,1] | 0.09 (4/46, max 1) | 598 | 43035536 | 43035536 | 0/0/0 | 44/0/0 | 598 | 77.979ms | 642 | 46 |
| D  morsel-push (x14, 131072r) | 2.092ms [2.029ms,2.482ms] | 0.10x | 0.433ms | 46 | 2353/1284/0 | 14.96 [13,15] | 12.98 [11,13] | 1.02 [1,1] | 0.33 (2/46, max 13) | 597 | 43035536 | 43035536 | 1284/0/0 | 45/0/0 | 597 | 50.679ms | 642 | 46 |

### scan-6col — 6001215 rows out (100.00% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 2.161ms [2.026ms,2.219ms] | 1.00x | 1.378ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 0.891ms [0.838ms,0.892ms] | 0.41x | 0.358ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 0.387ms [0.371ms,0.414ms] | 0.18x | 0.082ms | 46 | 0/0/0 | 6.00 [6,6] | 0.00 [0,0] | 0.00 [0,0] | 0.00 (0/46, max 0) | 0 | 59933416 | 59933416 | 0/0/0 | 276/0/0 | 0 | 0.000ms | 276 | 0 |
| D  morsel-push (x14, 131072r) | 0.499ms [0.425ms,0.669ms] | 0.23x | 0.074ms | 46 | 598/322/0 | 6.00 [6,6] | 0.00 [0,0] | 0.00 [0,0] | 0.00 (0/46, max 0) | 0 | 59933416 | 59933416 | 322/0/0 | 276/0/0 | 0 | 0.000ms | 276 | 0 |

### selective — 260 rows out (0.00% selectivity)

| executor | wall | vs V1 | ttfb | morsels | push transitions/inline/spill | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | fast/cold/mask-clones | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 8.184ms [7.714ms,8.211ms] | 1.00x | 4.681ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.681ms [1.514ms,1.841ms] | 0.21x | 0.359ms | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel-pull (x14, 131072r) | 1.194ms [1.148ms,1.222ms] | 0.15x | 0.199ms | 46 | 0/0/0 | 4.76 [4,5] | 3.09 [2,2] | 2.09 [1,1] | 0.43 (20/46, max 1) | 142 | 44919780 | 44919780 | 0/0/0 | 65/0/0 | 142 | 10.770ms | 210 | 20 |
| D  morsel-push (x14, 131072r) | 1.155ms [1.107ms,2.436ms] | 0.14x | 0.277ms | 46 | 716/460/0 | 4.78 [4,5] | 2.98 [2,2] | 1.98 [1,1] | 0.57 (19/46, max 2) | 137 | 44919780 | 44919780 | 460/0/0 | 70/0/0 | 137 | 13.480ms | 209 | 21 |

Every configuration reproduced V1's dtype, row count and ordered content exactly.

## Pull crate, default rows

```bash
TPCH_ITERATIONS=3 cargo run --release -p vortex-morsel --features _test-harness --bin tpch-eval -- 1
```

**Evaluator banner**

lineitem SF=1: 6001215 rows (6001215 generated), 16 columns,          733 natural splits; generated in 1890.613ms, written in 2972.629ms
written through the btrblocks compressing pipeline (repartition 8192 rows -> coalesce 1048576B -> compress -> buffer -> chunk -> flat); no zone maps, no dict layout
segment payloads: 1789 segments, 174419156 bytes total, 3780/102412/393492 bytes min/median/max
host: 14 available logical CPUs; segments in memory; one untimed warm-up + 3 grouped iterations per configuration, median reported
both executors use workers prepared outside the timed interval

schema: {l_orderkey=i64, l_partkey=i64, l_suppkey=i64, l_linenumber=i32, l_quantity=decimal(15,2), l_extendedprice=decimal(15,2), l_discount=decimal(15,2), l_tax=decimal(15,2), l_returnflag=utf8, l_linestatus=utf8, l_shipdate=vortex.date[days](i32), l_commitdate=vortex.date[days](i32), l_receiptdate=vortex.date[days](i32), l_shipinstruct=utf8, l_shipmode=utf8, l_comment=utf8}

morsel lookahead: 16 morsels (TPCH_LOOKAHEAD)
### Q6 — 114160 rows out (1.90% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 14.640ms [14.537ms,15.042ms] | 1.00x | 8.859ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 2.765ms [2.569ms,2.933ms] | 0.19x | 0.536ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 11.520ms [11.411ms,12.125ms] | 0.79x | 0.296ms | 46 | 6.00 [5,7] | 3.50 [3,4] | 2.00 [2,2] | 0.00 (0/46, max 0) | 161 | 34559892 | 34559892 | 115/0/0 | 46 | 0.421ms | 161 | 161 |
| D  morsel (x1, splits, no-reuse) | 11.720ms [11.078ms,12.092ms] | 0.80x | 0.232ms | 46 | 7.00 [7,7] | 3.50 [3,4] | 2.00 [2,2] | 0.00 (0/46, max 0) | 161 | 34559892 | 34559892 | 115/0/0 | 46 | 0.530ms | 322 | 0 |
| D  morsel (x14, splits) | 1.589ms [1.515ms,1.754ms] | 0.11x | 0.364ms | 46 | 6.50 [5,7] | 3.50 [3,4] | 2.00 [2,2] | 0.28 (13/46, max 1) | 161 | 34559892 | 34559892 | 70/0/0 | 91 | 3.666ms | 166 | 156 |
| D  morsel (x14, 131072r) | 1.431ms [1.404ms,1.666ms] | 0.10x | 0.340ms | 46 | 6.57 [5,7] | 3.50 [3,4] | 2.00 [2,2] | 0.15 (7/46, max 1) | 161 | 34559892 | 34559892 | 105/0/0 | 56 | 4.055ms | 168 | 154 |

### Q1 — 5916591 rows out (98.59% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 5.586ms [5.034ms,6.409ms] | 1.00x | 3.334ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.449ms [1.449ms,1.613ms] | 0.26x | 0.370ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 4.727ms [4.019ms,5.764ms] | 0.85x | 0.104ms | 92 | 4.25 [2,7] | 4.25 [2,7] | 1.25 [1,2] | 0.00 (0/92, max 0) | 391 | 39911260 | 39911260 | 23/0/0 | 368 | 2.323ms | 391 | 253 |
| D  morsel (x1, splits, no-reuse) | 4.474ms [4.202ms,5.757ms] | 0.80x | 0.052ms | 92 | 7.00 [7,7] | 4.25 [2,7] | 1.25 [1,2] | 0.00 (0/92, max 0) | 391 | 39911260 | 39911260 | 23/0/0 | 368 | 2.843ms | 644 | 0 |
| D  morsel (x14, splits) | 1.315ms [1.028ms,1.317ms] | 0.24x | 0.169ms | 92 | 6.50 [2,7] | 4.25 [2,7] | 1.25 [1,2] | 0.27 (25/92, max 1) | 391 | 39911260 | 39911260 | 274/0/0 | 117 | 6.793ms | 444 | 200 |
| D  morsel (x14, 131072r) | 0.886ms [0.878ms,0.943ms] | 0.16x | 0.144ms | 46 | 8.87 [8,9] | 8.50 [8,9] | 1.50 [1,2] | 0.11 (5/46, max 1) | 391 | 39911260 | 39911260 | 329/0/0 | 62 | 4.755ms | 393 | 21 |

### Q14 — 75983 rows out (1.27% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 5.799ms [5.709ms,6.033ms] | 1.00x | 3.588ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.436ms [1.385ms,1.493ms] | 0.25x | 0.428ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 4.730ms [4.081ms,4.831ms] | 0.82x | 0.100ms | 46 | 4.00 [3,5] | 3.50 [3,4] | 1.50 [1,2] | 0.00 (0/46, max 0) | 161 | 43548036 | 43548036 | 23/0/0 | 138 | 1.451ms | 161 | 69 |
| D  morsel (x1, splits, no-reuse) | 4.540ms [4.407ms,4.551ms] | 0.78x | 0.098ms | 46 | 5.00 [5,5] | 3.50 [3,4] | 1.50 [1,2] | 0.00 (0/46, max 0) | 161 | 43548036 | 43548036 | 23/0/0 | 138 | 1.134ms | 230 | 0 |
| D  morsel (x14, splits) | 0.730ms [0.710ms,0.841ms] | 0.13x | 0.204ms | 46 | 4.72 [3,5] | 3.50 [3,4] | 1.50 [1,2] | 0.26 (10/46, max 2) | 161 | 43548036 | 43548036 | 53/0/0 | 108 | 4.400ms | 168 | 62 |
| D  morsel (x14, 131072r) | 0.709ms [0.675ms,0.724ms] | 0.12x | 0.142ms | 46 | 4.61 [3,5] | 3.50 [3,4] | 1.50 [1,2] | 0.17 (8/46, max 1) | 161 | 43548036 | 43548036 | 101/0/0 | 60 | 1.666ms | 168 | 62 |

### Q15 — 225954 rows out (3.77% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 6.039ms [5.623ms,6.084ms] | 1.00x | 3.762ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.286ms [1.259ms,1.479ms] | 0.21x | 0.501ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 4.441ms [4.418ms,4.482ms] | 0.74x | 0.101ms | 46 | 4.00 [3,5] | 3.50 [3,4] | 1.50 [1,2] | 0.00 (0/46, max 0) | 161 | 40547204 | 40547204 | 23/0/0 | 138 | 1.253ms | 161 | 69 |
| D  morsel (x1, splits, no-reuse) | 4.670ms [4.641ms,4.785ms] | 0.77x | 0.098ms | 46 | 5.00 [5,5] | 3.50 [3,4] | 1.50 [1,2] | 0.00 (0/46, max 0) | 161 | 40547204 | 40547204 | 23/0/0 | 138 | 1.075ms | 230 | 0 |
| D  morsel (x14, splits) | 0.736ms [0.715ms,0.769ms] | 0.12x | 0.186ms | 46 | 4.63 [3,5] | 3.50 [3,4] | 1.50 [1,2] | 0.28 (13/46, max 1) | 161 | 40547204 | 40547204 | 47/0/0 | 114 | 2.273ms | 170 | 60 |
| D  morsel (x14, 131072r) | 0.699ms [0.698ms,0.750ms] | 0.12x | 0.200ms | 46 | 4.54 [3,5] | 3.50 [3,4] | 1.50 [1,2] | 0.37 (16/46, max 2) | 161 | 40547204 | 40547204 | 35/0/0 | 126 | 3.874ms | 168 | 62 |

### Q12 — 108434 rows out (1.81% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 15.227ms [14.560ms,15.552ms] | 1.00x | 9.157ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 2.849ms [2.721ms,2.862ms] | 0.19x | 0.860ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 15.565ms [14.902ms,15.772ms] | 1.02x | 0.192ms | 92 | 3.50 [1,10] | 2.25 [1,5] | 1.25 [1,2] | 0.00 (0/92, max 0) | 207 | 39693404 | 39693404 | 69/0/0 | 138 | 1.191ms | 207 | 713 |
| D  morsel (x1, splits, no-reuse) | 16.166ms [15.528ms,16.294ms] | 1.06x | 0.184ms | 92 | 10.00 [10,10] | 2.25 [1,5] | 1.25 [1,2] | 0.00 (0/92, max 0) | 207 | 39693404 | 39693404 | 69/0/0 | 138 | 1.637ms | 920 | 0 |
| D  morsel (x14, splits) | 2.304ms [2.273ms,2.342ms] | 0.15x | 0.325ms | 92 | 6.47 [2,10] | 2.25 [1,5] | 1.25 [1,2] | 0.20 (18/92, max 1) | 207 | 39693404 | 39693404 | 96/0/0 | 111 | 6.515ms | 248 | 672 |
| D  morsel (x14, 131072r) | 1.998ms [1.776ms,2.124ms] | 0.13x | 0.429ms | 46 | 8.74 [3,11] | 4.50 [3,6] | 1.50 [1,2] | 0.35 (15/46, max 2) | 207 | 39693404 | 39693404 | 98/0/0 | 109 | 11.020ms | 215 | 291 |

### Q19 — 3599028 rows out (59.97% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 21.390ms [20.632ms,24.760ms] | 1.00x | 4.263ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 4.777ms [4.691ms,4.875ms] | 0.22x | 1.375ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 13.968ms [13.780ms,14.474ms] | 0.65x | 0.116ms | 366 | 2.01 [1,8] | 1.75 [1,6] | 1.13 [1,2] | 0.00 (1/366, max 1) | 642 | 43035536 | 43035536 | 46/0/0 | 596 | 5.565ms | 642 | 2286 |
| D  morsel (x1, splits, no-reuse) | 19.133ms [18.595ms,19.887ms] | 0.89x | 0.062ms | 366 | 8.00 [8,8] | 1.75 [1,6] | 1.13 [1,2] | 0.00 (1/366, max 1) | 642 | 43035536 | 43035536 | 46/0/0 | 596 | 5.904ms | 2928 | 0 |
| D  morsel (x14, splits) | 3.914ms [3.877ms,4.285ms] | 0.18x | 0.186ms | 366 | 5.57 [1,8] | 1.75 [1,6] | 1.13 [1,2] | 0.18 (65/366, max 1) | 642 | 43035536 | 43035536 | 319/0/0 | 323 | 6.060ms | 793 | 2135 |
| D  morsel (x14, 131072r) | 1.466ms [1.423ms,1.593ms] | 0.07x | 0.465ms | 46 | 15.96 [14,16] | 13.96 [12,14] | 2.00 [2,2] | 0.41 (19/46, max 1) | 642 | 43035536 | 43035536 | 369/0/0 | 273 | 13.598ms | 642 | 92 |

### scan-6col — 6001215 rows out (100.00% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 1.918ms [1.914ms,2.340ms] | 1.00x | 1.219ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 0.826ms [0.777ms,0.829ms] | 0.43x | 0.314ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 0.474ms [0.471ms,0.495ms] | 0.25x | 0.023ms | 46 | 6.00 [6,6] | 6.00 [6,6] | 1.00 [1,1] | 0.00 (0/46, max 0) | 276 | 59933416 | 59933416 | 276/0/0 | 0 | 0.000ms | 276 | 0 |
| D  morsel (x1, splits, no-reuse) | 0.463ms [0.463ms,0.464ms] | 0.24x | 0.012ms | 46 | 6.00 [6,6] | 6.00 [6,6] | 1.00 [1,1] | 0.00 (0/46, max 0) | 276 | 59933416 | 59933416 | 276/0/0 | 0 | 0.000ms | 276 | 0 |
| D  morsel (x14, splits) | 0.268ms [0.239ms,1.336ms] | 0.14x | 0.075ms | 46 | 6.00 [6,6] | 6.00 [6,6] | 1.00 [1,1] | 0.00 (0/46, max 0) | 276 | 59933416 | 59933416 | 276/0/0 | 0 | 0.000ms | 276 | 0 |
| D  morsel (x14, 131072r) | 0.255ms [0.250ms,0.263ms] | 0.13x | 0.053ms | 46 | 6.00 [6,6] | 6.00 [6,6] | 1.00 [1,1] | 0.00 (0/46, max 0) | 276 | 59933416 | 59933416 | 276/0/0 | 0 | 0.000ms | 276 | 0 |

### selective — 260 rows out (0.00% selectivity)

| executor | wall | vs V1 | ttfb | morsels | named IO/morsel | new requests/morsel | IO batches/morsel | blocked/morsel | physical reads | physical bytes | segment bytes | nowait hit/miss/unsupported | pending polls | async wait | decodes | reuses |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| A  V1 (1 thread) | 8.015ms [7.942ms,8.533ms] | 1.00x | 4.717ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| A' V1 (tokio x14) | 1.621ms [1.515ms,1.778ms] | 0.20x | 0.346ms | — | — | — | — | — | — | — | — | — | — | — | — | — |
| D  morsel (x1, splits) | 5.714ms [5.513ms,6.143ms] | 0.71x | 0.138ms | 46 | 5.00 [4,6] | 4.50 [4,5] | 2.00 [2,2] | 0.00 (0/46, max 0) | 207 | 44919780 | 44919780 | 115/0/0 | 92 | 0.904ms | 207 | 69 |
| D  morsel (x1, splits, no-reuse) | 5.857ms [5.729ms,6.167ms] | 0.73x | 0.119ms | 46 | 6.00 [6,6] | 4.50 [4,5] | 2.00 [2,2] | 0.00 (0/46, max 0) | 207 | 44919780 | 44919780 | 115/0/0 | 92 | 0.749ms | 276 | 0 |
| D  morsel (x14, splits) | 0.934ms [0.871ms,0.939ms] | 0.12x | 0.195ms | 46 | 5.72 [4,6] | 4.50 [4,5] | 2.00 [2,2] | 0.11 (4/46, max 2) | 207 | 44919780 | 44919780 | 160/0/0 | 47 | 2.054ms | 213 | 63 |
| D  morsel (x14, 131072r) | 0.912ms [0.896ms,0.931ms] | 0.11x | 0.204ms | 46 | 5.80 [4,6] | 4.50 [4,5] | 2.00 [2,2] | 0.17 (8/46, max 1) | 207 | 44919780 | 44919780 | 152/0/0 | 55 | 2.127ms | 214 | 62 |

Every configuration reproduced V1's dtype, row count and ordered content exactly.

## Hint model in the pull crate: before and after

The pull crate stopped filtering at its leaves on 2026-09-07. The row mask a node executes
under is a hint; only the flat leaf reads it, to name no read for a range nobody wants and to
answer such a range with placeholder rows instead of waiting; every batch is dense over its
range; and the filter root applies the conjuncts' mask once. Both evaluators were run against a
binary built from the commit before the change and one built from the change, interleaved
before/after in two rounds on the same 14-core host, five iterations per configuration. Each
cell is the ratio of the smaller of the two rounds' minimums, so it favours neither binary. The
V1 and push rows come from code the change does not touch, so they bound the noise.

| Rows | Geomean after/before |
| --- | --: |
| Pull crate, all 107 configurations | 1.03 |
| Pull crate, single-thread configurations | 1.01 |
| Pull crate, 14-thread configurations | 1.04 |
| V1 control, 46 configurations | 1.00 |
| Push control, 45 configurations | 0.99 |
| Same binary, round 2 over round 1 | 1.08 |

Every TPC-H configuration is within 5 percent. What remains above the noise is a handful of
14-thread `morsel-eval` rows on the wide-numeric workload at 65536-row morsels, `WN5
selective-wide` in particular, at 1.2x to 1.4x across repeated alternations while its
single-thread rows are flat. Per-morsel traces explain it: on that workload a morsel spends
about 0.7 ms planning (twenty columns, six to eight chunks each, one registration per read
through one lock, fourteen workers at once) and about 0.2 ms executing, and the planning total
for the same binary swings between 9 ms and 15 ms from one run to the next. Execution time per
morsel is equal or lower after the change. The gap is a pre-existing lock convoy in planning
that the new binary happens to land in more often, and the fix belongs in registration, not in
the value path.

Two false starts are worth recording. Applying the mask to the projection's struct in one
call routed sparse masks through the chunked filter kernel's per-index take path and cost Q15
15 percent single-threaded; filtering each field and chunk by its own slice of the mask
(`filter_rows`) restored it. And the first version of the hint model let chunked skip unwanted
chunks and then described the holes in every batch with a mask; the masks, their intersection
in struct, and the rank-domain compression at the root tripled small allocations and contended
malloc across workers. Dense batches with leaf placeholders removed all of that.

```bash
TPCH_ITERATIONS=5 target/release/tpch-eval 1     # built with --features _test-harness
target/release/morsel-eval
MORSEL_EVAL_QUERY="WN5 selective-wide" MORSEL_EVAL_MORSEL_ROWS=65536 MORSEL_EVAL_ITERATIONS=15 target/release/morsel-eval
```
