# I/O frontier design and findings

This note records the contract and the SF10 evidence for the grouped I/O frontier scheduler.
It describes the prototype on `ji/morsel-io-groups`; it is not a stable public API.

## Model

An execution node exposes I/O incrementally through `ExecNode::next_io`. The owner of a logical
task brackets its child walk with `IoPrewalkCx::group_begin` and `group_end`. The current group
kinds are:

- `Pruning`: auxiliary statistics needed to produce a pruning mask.
- `Conjunct`: one cascade conjunct, or all conjuncts in parallel mode.
- `Projection`: the columns materialized for rows surviving the filter.

`ExecPlan::frontier(range)` returns an `IoFrontierCursor` at that range's first group:

- `next_io(budget)` enumerates a bounded piece of the current group;
- `right()` moves right after that group is fully enumerated;
- `down(range)` reuses the cursor arena for another row range.

This is deliberately a cursor rather than a materialized range-by-group rectangle. It gives the
scheduler the two choices it needs: move down to expose the same critical group for subsequent
ranges, or move right to expose later work for the current range.

The cursor returns borrowed slices of `IoKey` backed by a reusable buffer. `down` resets traversal
state without dropping that buffer's capacity. The node-facing API is only `group_begin(kind)`,
`read(key)`, and `group_end()`; source activation and raw-cell lifetime stay in their existing
execution and I/O layers.

## Completion and push

Group discovery is not group completion. A group names stored inputs. Once
its reads complete, sources push decoded batches through the physical pipeline. The gate emitted
by that pushed pipeline is the coverage-scoped completion credit for the preceding task. The gate
contains the authoritative row selection and activates the next predicate or projection sources.
There is no global "pipeline idle" test.

Speculative reads only start the same keyed cells early. A later gate promotes those cells to
required work; it does not submit a second read. Correctness therefore remains entirely in the
push/gate path, while the frontier cursor controls scheduling and coalescing.

The scheduler claims a configurable, bounded number of subsequent row frontiers per refill and
submits equal-depth groups together. A claimed cursor is not published to workers until every cell
in the wave has been registered, preventing a worker from racing the batch with one-off required
reads. When the published cursor is consumed, one worker refills the full down window. Group order
is preserved, but adjacent segment reads from different row ranges are visible to the storage
coalescer in one atomic submission.

## Right-frontier policy

A fixed right depth is useful for measurement but is not plan-independent. At SF10, S2 (the
current group plus two groups to the right) misses Q12's fourth conjunct. S3 fixes Q12, but crosses
from the third conjunct into projection for the highly selective synthetic query and reads almost
200 MB that cold execution does not need.

The adaptive policy therefore:

1. admits every `Conjunct` group;
2. initially leaves `Projection` demand-driven;
3. after at least eight completed morsels, admits projection for later refills only when at least
   75% of completed morsels produced output.

The policy keeps two atomic counters. Its raw buffers are governed by the same bounded frontier
and exact-use retirement described below. Fixed S0-S4 policies remain available through
`with_speculative_frontiers` for experiments.

## Decoded sharing and lifetime

The current executor does have scan-local decoded sharing for flat segments. `SharedCells` assigns
each decoded cell a static lease count derived from overlapping morsels. A morsel reuses the
decoded array, and `retire_morsel` releases its lease; the final release drops the array. This is
the existing retain/release boundary between splits that refer to the same flat segment.

Raw I/O cells now have the same exact planned-use lifetime. Non-final releases decrement an atomic
counter without taking a registry lock. The final release removes the cell, drops ready bytes
synchronously, or cancels and drops a queued/in-flight segment future. A scan-owned driver receives
`Shutdown` and is joined when the scan drops, so no future survives into the next query run. Shared
external query services are shut down only by their owner, not by each subscan.

This is still not the general retain/close API needed by all encodings:

- decode publication is not a general single-flight primitive;
- dictionary layouts are not supported by the push builder, so dictionary values do not yet have
  a shared decoded-value lifetime;
- an execution node cannot explicitly retain a shared decoded dependency and close it early.

A follow-up design should introduce a typed scan-scoped `DecodedLease<T>` returned by a
single-flight `get_or_decode` operation. Cloning a lease is `retain`; dropping it or calling
`close` releases one consumer. The registry entry should be keyed by the physical segment plus
decode identity, so split overlaps and dictionary-value references converge on one value. Plan
construction should register the exact consumer count where it is statically known; dynamic
consumers can hold ordinary cloned leases. The final lease should drop both the decoded array and,
when no other decoder needs it, its raw I/O buffer. Morsel retirement remains the safety net, but
nodes may close earlier after their last pushed batch.

## Historical SF10 evaluation

The measurements in this section predate exact raw-cell retirement and the strict admission cap.
They are retained as scheduling history only: that version held ready raw cells until scan drop and
its `256`-range refill could widen a smaller configured window. Do not use these numbers for final
performance or memory conclusions; the bounded rerun at the end of this note supersedes them.

The final run used 59,986,052 rows, 7,323 natural-split morsels, four logical CPUs, three resident
morsels per thread, 32-range refills, 64 KiB storage blocks, I/O depth 16, five alternating
iterations, and median wall time. The cold run used a newly written 1,812,657,104-byte pack with
`F_NOCACHE`; the hot run followed on the same pack. Every configuration reproduced V1's dtype,
row count, and ordered content exactly.

Cold medians (milliseconds / physical MB):

| workload | V1 Tokio | fixed S2 | fixed S3 | adaptive |
|---|---:|---:|---:|---:|
| Q6 | 302.9 / 520.7 | 306.7 / 369.5 | 233.0 / 358.9 | 233.2 / 358.7 |
| Q1 | 218.8 / 416.8 | 225.9 / 415.0 | 225.0 / 414.8 | 226.6 / 414.8 |
| Q14 | 234.8 / 574.7 | 201.2 / 463.3 | 199.5 / 463.3 | 201.2 / 464.4 |
| Q15 | 391.9 / 545.5 | 367.6 / 433.3 | 365.8 / 433.3 | 364.1 / 433.9 |
| Q12 | 451.1 / 405.5 | 568.1 / 419.8 | 405.4 / 402.0 | 414.0 / 402.1 |
| Q19 | 236.8 / 466.1 | 237.5 / 471.5 | 236.9 / 471.2 | 239.2 / 472.5 |
| scan-6col | 225.5 / 651.8 | 132.4 / 651.8 | 133.4 / 651.8 | 128.9 / 651.8 |
| selective | 262.5 / 614.5 | 202.6 / 266.7 | 190.8 / 461.3 | 201.3 / 266.6 |

Hot medians (milliseconds / physical MB):

| workload | V1 Tokio | fixed S2 | fixed S3 | adaptive |
|---|---:|---:|---:|---:|
| Q6 | 260.1 / 517.7 | 178.1 / 368.0 | 152.1 / 357.8 | 151.7 / 357.7 |
| Q1 | 196.4 / 416.0 | 160.8 / 418.9 | 160.3 / 418.3 | 160.2 / 419.6 |
| Q14 | 155.8 / 569.9 | 116.1 / 464.0 | 115.3 / 463.8 | 116.5 / 463.9 |
| Q15 | 155.0 / 540.0 | 115.1 / 434.1 | 114.6 / 433.9 | 114.3 / 434.3 |
| Q12 | 317.3 / 404.3 | 245.9 / 417.6 | 199.7 / 402.1 | 200.2 / 402.2 |
| Q19 | 219.5 / 466.1 | 162.9 / 472.7 | 162.5 / 473.2 | 162.9 / 473.5 |
| scan-6col | 143.2 / 651.8 | 81.9 / 651.8 | 83.0 / 651.8 | 82.7 / 651.8 |
| selective | 215.1 / 605.3 | 137.9 / 265.7 | 127.2 / 462.0 | 137.9 / 267.1 |

The important mechanism is read shape, not additional data. Q12 S2 to adaptive reduces cold
physical reads from 10,774 to 2,038 and bytes from 419.8 MB to 402.1 MB. On the selective query,
adaptive remains at the S2 demand boundary instead of accepting fixed S3's 194.7 MB projection
over-read. Across all eight workloads, adaptive is 1.18x faster than V1 Tokio cold and 1.47x
faster hot by geometric mean (1.12x and 1.44x over the seven filtered workloads).

Reproduction commands:

```bash
env RUSTC_WRAPPER= TPCH_THREADS=4 TPCH_ITERATIONS=5 \
  TPCH_FRONTIER_ADAPTIVE_COMPARE=1 TPCH_MORSEL_ROWS=1 \
  TPCH_FRONTIER_REFILL_RANGES=32 TPCH_BLOCK_BYTES=65536 \
  TPCH_DISK_PATH=/private/tmp/vortex-frontier-final-sf10.vortex \
  TPCH_CACHE_MODE=cold TPCH_IO_DEPTH=16 \
  target/release/tpch-push-eval 10

env RUSTC_WRAPPER= TPCH_REUSE_DISK_PACK=1 TPCH_THREADS=4 TPCH_ITERATIONS=5 \
  TPCH_FRONTIER_ADAPTIVE_COMPARE=1 TPCH_MORSEL_ROWS=1 \
  TPCH_FRONTIER_REFILL_RANGES=32 TPCH_BLOCK_BYTES=65536 \
  TPCH_DISK_PATH=/private/tmp/vortex-frontier-final-sf10.vortex \
  TPCH_CACHE_MODE=hot TPCH_IO_DEPTH=16 \
  target/release/tpch-push-eval 10
```

One invalid cold-after-hot attempt was discarded: `F_NOCACHE` prevents normal caching for new
reads but does not reliably evict pages already made hot. All reported cold results therefore come
from fresh packs and precede their hot runs.

## Lock and scheduling follow-up

The follow-up profile concentrated on the scheduler rather than decode kernels. Three changes were
kept:

- The scan-wide raw-I/O cell registry is split into 16 shards by dense `SegmentId`, so planning,
  completion, and lookup of unrelated cells do not share one map lock.
- Background I/O no longer mirrors every already-issued read in `Scheduler::io_work`. The
  scan-wide `IoCell` is authoritative; the map remains only for pull/nowait execution that must
  enumerate unissued demand.
- Each I/O cell now has one lock covering terminal state and waiters. Completion changes state and
  takes its waiter list atomically, then wakes outside the lock.

An unfiltered frontier walk also registers and submits one sorted, bounded refill wave at a time.
It was already going to read all of those segments, so this changes neither logical demand nor
lifetime; it only lets the physical source coalesce across row-range boundaries without building a
scan-sized request vector.

On the SF10 hot selective case, the same five-run benchmark moved from 141.792 ms to 139.589 ms
after sharding, 136.195 ms after removing the duplicate background map, and 135.779 ms after
combining cell locks. A Samply run over repeated scans found:

| worker CPU category | before | after |
|---|---:|---:|
| contended mutex slow path | 4.31% | 0.64% |
| I/O registration | 2.04% | 0.87% |
| I/O cell lookup | 0.66% | 0.22% |
| I/O batch submission | 0.86% | ~0.00% |
| assign-next scheduling | 4.71% | 3.75% |
| push engine | 71.49% | 74.16% |

The first capture created 528 short-lived worker-thread generations, while the final capture reused
four workers, so the percentages are directional rather than an isolated A/B for each lock. The
stepwise wall measurements above isolate the retained code changes. Park/channel stacks were about
10-12% in both captures, but sampled CPU deltas are charged to the stack that subsequently parks;
that number is not evidence of 12% active channel contention.

An inline `SmallVec` for group I/O uses was rejected (137.345 ms versus 135.779 ms). Earlier
required-read accumulator, all-idle flush, and hot inline-read experiments were also rejected; they
were neutral or slower and are not present in the implementation.

## Larger bounded scheduling windows

At 2 ms injected latency per coalesced physical request, using the production object-store
coalescing policy (1 MiB distance, 16 MiB maximum) and depth 192 showed that three resident morsels
exposed only 26 reads at once. More right speculation was not the only answer: more row-subsequent
(`down`) frontiers exposed independent required work without reading projection.

Selective SF10, fixed S2, three resident morsels per thread:

| down frontiers/thread | wall | physical reads | physical MB |
|---:|---:|---:|---:|
| 0 | 1,393.7 ms | 4,412 | 329.2 |
| 1 | 1,297.8 ms | 3,976 | 322.1 |
| 2 | 1,199.5 ms | 3,916 | 312.9 |
| 4 | 1,153.3 ms | 3,939 | 306.2 |
| 8 | 872.5 ms | 4,239 | 269.8 |
| 12 | 823.1 ms | 4,115 | 268.4 |
| 24 | 801.9 ms | 4,218 | 267.2 |

With eight down frontiers per thread, increasing resident execution arenas from three to six
reduced wall time from 874.5 ms to 566.6 ms; eight arenas reached 555.8 ms and twelve reached
510.0 ms. Six is the useful knee. Twelve arenas without down lookahead reached 702.2 ms but read
386.6 MB, versus 267.2 MB for three arenas plus 24 down frontiers. This supports separating the
small execution-state window from the larger I/O-only frontier window.

Increasing the atomic refill from 32 to 256 ranges then removed storage batch fragmentation. For
example, filtered Q19 fell from 1,373 physical reads to 181 under fixed S2. Bounded unfiltered
scan-6col changed from 7,323 scheduler start batches to 29; under injected latency it used 96
physical reads rather than the temporary whole-scan batch's 40, but was faster because planning and
I/O overlapped and it never built a scan-sized vector. Q12 initially remained slow because fixed S2
enumerates only three of its four conjunct groups; adaptive-right enumerates all conjuncts and
reduced Q12 from 7,775 reads / 1,408 ms to 434 reads / 361 ms without speculating projection.

This historical pass selected six resident morsels per thread, eight additional down frontiers per
thread, adaptive right traversal, and 256-range refills. The final bounded pass below changes the
down cap and refill trigger.

## Historical pre-retirement end-to-end results

Local file settings used depth 16 and the normal 64 KiB / 2 MiB file coalescer. Times are medians
of three alternating runs. The preceding adaptive implementation used three residents, no extra
down frontier, and 32-range refills.

| workload | hot V1 Tokio | hot optimized | cold V1 Tokio | cold optimized |
|---|---:|---:|---:|---:|
| Q6 | 256.342 ms | 132.001 ms | 265.875 ms | 157.242 ms |
| Q1 | 195.287 ms | 125.289 ms | 207.312 ms | 133.958 ms |
| Q14 | 152.141 ms | 87.356 ms | 176.254 ms | 102.093 ms |
| Q15 | 154.283 ms | 89.050 ms | 267.611 ms | 161.939 ms |
| Q12 | 318.051 ms | 189.998 ms | 451.725 ms | 251.391 ms |
| Q19 | 217.188 ms | 137.006 ms | 227.578 ms | 141.794 ms |
| scan-6col | 145.930 ms | 67.964 ms | 267.280 ms | 111.637 ms |
| selective | 211.464 ms | 116.288 ms | 276.167 ms | 133.799 ms |
| geometric mean | 199.456 ms | 112.938 ms | 258.158 ms | 144.052 ms |

The optimized row is 1.766x faster than V1 Tokio hot and 1.792x cold. Relative to the preceding
adaptive frontier implementation, geometric-mean wall time falls 16.6% hot (135.394 to 112.938 ms)
and 39.2% on valid fresh-pack cold runs (236.980 to 144.052 ms). The final cold run wrote a fresh
pack before measuring; reused-pack cold-after-hot runs were discarded. Even fresh local NVMe does
not model object-store request latency, which is evaluated separately below.

The injected-latency run used 2 ms per physical request, depth 192, and the normal object-store
coalescer. It applies the delay at the common `VortexReadAt` physical boundary, identically for V1
and morsel execution; no inline/cache path can bypass it.

| workload | V1 Tokio | optimized | speedup | optimized reads / physical MB |
|---|---:|---:|---:|---:|
| Q6 | 2,003.931 ms | 332.148 ms | 6.03x | 281 / 355.8 |
| Q1 | 812.322 ms | 309.727 ms | 2.62x | 1,078 / 416.7 |
| Q14 | 1,311.478 ms | 303.625 ms | 4.32x | 537 / 466.3 |
| Q15 | 1,301.739 ms | 294.640 ms | 4.42x | 492 / 436.0 |
| Q12 | 758.724 ms | 360.923 ms | 2.10x | 434 / 403.8 |
| Q19 | 359.297 ms | 312.172 ms | 1.15x | 1,025 / 471.1 |
| scan-6col | 307.670 ms | 120.144 ms | 2.56x | 96 / 651.8 |
| selective | 2,565.365 ms | 446.656 ms | 5.74x | 3,529 / 302.3 |
| geometric mean | 937.742 ms | 293.885 ms | 3.19x | — |

The worst-case selective RSS measurement under that injected latency was +181.4 MiB for the tuned
window versus +185.5 MiB for the old three-resident/32-refill policy. Output credit accounted for
less than 1 MiB. Both windows are explicitly bounded, and the faster policy did not increase
measured peak memory because larger coalesced ranges replaced many small queued reads.

Reproduction of the optimized local and latency rows:

```bash
env TPCH_REUSE_DISK_PACK=1 TPCH_THREADS=4 TPCH_ITERATIONS=3 \
  TPCH_MORSEL_ROWS=1 TPCH_INCLUDE_V1=1 \
  TPCH_RESIDENT_MORSELS_PER_THREAD=6 TPCH_FRONTIERS_PER_THREAD=8 \
  TPCH_ADAPTIVE_FRONTIERS=1 TPCH_FRONTIER_REFILL_RANGES=256 \
  TPCH_BLOCK_BYTES=65536 \
  TPCH_DISK_PATH=/private/tmp/vortex-frontier-dynamic-sf10-64k.vortex \
  TPCH_CACHE_MODE=hot TPCH_IO_DEPTH=16 \
  target/release_debug/tpch-push-eval 10

env TPCH_THREADS=4 TPCH_ITERATIONS=3 \
  TPCH_MORSEL_ROWS=1 TPCH_INCLUDE_V1=1 \
  TPCH_RESIDENT_MORSELS_PER_THREAD=6 TPCH_FRONTIERS_PER_THREAD=8 \
  TPCH_ADAPTIVE_FRONTIERS=1 TPCH_FRONTIER_REFILL_RANGES=256 \
  TPCH_BLOCK_BYTES=65536 \
  TPCH_DISK_PATH=/private/tmp/vortex-frontier-locks-final-cold-sf10.vortex \
  TPCH_CACHE_MODE=cold TPCH_IO_DEPTH=16 \
  target/release_debug/tpch-push-eval 10

env TPCH_REUSE_DISK_PACK=1 TPCH_THREADS=4 TPCH_ITERATIONS=3 \
  TPCH_MORSEL_ROWS=1 TPCH_INCLUDE_V1=1 \
  TPCH_RESIDENT_MORSELS_PER_THREAD=6 TPCH_FRONTIERS_PER_THREAD=8 \
  TPCH_ADAPTIVE_FRONTIERS=1 TPCH_FRONTIER_REFILL_RANGES=256 \
  TPCH_BLOCK_BYTES=65536 \
  TPCH_DISK_PATH=/private/tmp/vortex-frontier-dynamic-sf10-64k.vortex \
  TPCH_CACHE_MODE=hot TPCH_IO_DEPTH=192 TPCH_IO_LATENCY_US=2000 \
  TPCH_COALESCE_DISTANCE=1048576 TPCH_COALESCE_MAX_BYTES=16777216 \
  target/release_debug/tpch-push-eval 10
```

## Final bounded-retention SF10 rerun

This is the authoritative result for the current code. It uses four workers, six resident morsels
per worker, a hard cap of 64 down frontiers per worker (256 total), adaptive right traversal, and
at most 256 ranges per atomic refill. The scheduler refills only when the published down window is
consumed and does not publish the next cursor until registration finishes. It therefore recovers
large coalescing waves without admitting beyond the configured cap.

The evaluator drops the generated in-memory segment fixture before disk measurements and consumes
timed output batches immediately after counting their rows. Exactness is checked separately by
materializing and comparing ordered V1 and morsel output. All 24 final query/environment rows
reproduced V1's dtype, row count, and ordered contents exactly. Every morsel row ended with zero
raw cells and zero raw bytes.

| environment | V1 Tokio geometric mean | morsel geometric mean | speedup | morsel speedup range | peak raw bytes |
|---|---:|---:|---:|---:|---:|
| hot, depth 16 | 200.0 ms | 135.0 ms | 1.48x | 1.26x-1.71x | 6.2-22.6 MiB |
| fresh cold, depth 16 | 263.3 ms | 165.9 ms | 1.59x | 1.38x-1.89x | 6.1-23.4 MiB |
| 2 ms/read, depth 192 | 957.5 ms | 331.5 ms | 2.89x | 1.11x-5.75x | 6.3-22.6 MiB |

The dense latency scan demonstrates the published-wave mechanism directly: 30 scheduler start
batches became 180 physical reads and exactly 651,800,216 physical bytes, versus 7,220 reads before
wave publication was synchronized. Wall time was 226.1 ms versus 311.9 ms for V1 Tokio. Q19 is the
narrowest latency win (323.4 versus 359.1 ms); Q6 is the largest (351.1 versus 2,018.6 ms).

Incremental process RSS on the dense scan was +70.9 MiB for V1 Tokio, +12.6 MiB for morsel D=8,
and +43.6 MiB for latency-mode morsel D=64. The D=64 result split into 23.1 MiB peak raw I/O and
22.6 MiB peak output credit. The process baseline is intentionally excluded because constructing
the SF10 fixture leaves allocator pages resident even after its segment buffers are dropped.

Final logs:

- `/private/tmp/vortex-bounded-final-hot-sf10.log`
- `/private/tmp/vortex-bounded-final-fresh-cold2-sf10.log`
- `/private/tmp/vortex-bounded-final-latency-sf10.log`
