// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

// LPC reconstruction kernels: serial vs. L independent lanes (transposed, FastLanes-style layout
// where each lane is a contiguous run of the signal), for several predictor orders.
use std::hint::black_box;
use std::time::Instant;

const N: usize = 1 << 22;
const Q: u32 = 12;

/// Serial order-P inverse with the history kept in registers (rotated array).
#[inline(never)]
fn serial<const P: usize>(w: &[i32; P], x: &mut [i32]) {
    let mut h = [0i32; P];
    h.copy_from_slice(&x[..P]);
    for v in x[P..].iter_mut() {
        let mut s = 1i32 << (Q - 1);
        for j in 0..P {
            s = s.wrapping_add(w[j].wrapping_mul(h[j]));
        }
        let y = v.wrapping_add(s >> Q);
        *v = y;
        for j in 0..P - 1 {
            h[j] = h[j + 1];
        }
        h[P - 1] = y;
    }
}

/// Same with an i64 accumulator (needed when residual bits + coefficient bits + log2(P) > 31).
#[inline(never)]
fn serial64<const P: usize>(w: &[i64; P], x: &mut [i32]) {
    let mut h = [0i64; P];
    for j in 0..P {
        h[j] = x[j] as i64;
    }
    for v in x[P..].iter_mut() {
        let mut s = 1i64 << (Q - 1);
        for j in 0..P {
            s += w[j] * h[j];
        }
        let y = v.wrapping_add((s >> Q) as i32);
        *v = y;
        for j in 0..P - 1 {
            h[j] = h[j + 1];
        }
        h[P - 1] = y as i64;
    }
}

/// L lanes, layout x[t * L + lane]. The first P rows are warm-ups.
#[inline(never)]
fn lanes<const P: usize, const L: usize>(w: &[i32; P], x: &mut [i32]) {
    let rows = x.len() / L;
    for t in P..rows {
        let (hist, cur) = x.split_at_mut(t * L);
        let cur: &mut [i32; L] = (&mut cur[..L]).try_into().unwrap();
        let mut acc = [1i32 << (Q - 1); L];
        for j in 0..P {
            let row: &[i32; L] = hist[(t - P + j) * L..(t - P + j + 1) * L].try_into().unwrap();
            for l in 0..L {
                acc[l] = acc[l].wrapping_add(w[j].wrapping_mul(row[l]));
            }
        }
        for l in 0..L {
            cur[l] = cur[l].wrapping_add(acc[l] >> Q);
        }
    }
}

/// AVX-512 version of `lanes`: `L / 16` accumulators per row, taps in the outer loop.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn lanes_avx512<const P: usize, const L: usize>(w: &[i32; P], x: &mut [i32]) {
    use std::arch::x86_64::*;
    const V: usize = 16;
    let rows = x.len() / L;
    let ptr = x.as_mut_ptr();
    let mut wv = [_mm512_setzero_si512(); P];
    for j in 0..P {
        wv[j] = _mm512_set1_epi32(w[j]);
    }
    let bias = _mm512_set1_epi32(1 << (Q - 1));
    for t in P..rows {
        let mut acc = [bias; 8]; // up to 128 lanes
        for j in 0..P {
            let row = unsafe { ptr.add((t - P + j) * L) };
            for g in 0..L / V {
                let v = unsafe { _mm512_loadu_si512(row.add(g * V).cast()) };
                acc[g] = _mm512_add_epi32(acc[g], _mm512_mullo_epi32(wv[j], v));
            }
        }
        let cur = unsafe { ptr.add(t * L) };
        for g in 0..L / V {
            let r = unsafe { _mm512_loadu_si512(cur.add(g * V).cast()) };
            let y = _mm512_add_epi32(r, _mm512_srai_epi32::<{ Q }>(acc[g]));
            unsafe { _mm512_storeu_si512(cur.add(g * V).cast(), y) };
        }
    }
}

fn lanes_simd<const P: usize, const L: usize>(w: &[i32; P], x: &mut [i32]) {
    assert!(L % 16 == 0 && L <= 128 && is_x86_feature_detected!("avx512f"));
    unsafe { lanes_avx512::<P, L>(w, x) }
}

/// Pack residuals for `L` lanes at `w` bits: lane `l`'s k-th 32-bit word is `out[k * L + l]`,
/// so row `t` of every lane sits at the same bit offset (FastLanes-style vertical packing).
/// Values are stored as `r - min` so they are non-negative.
fn pack_lanes<const L: usize>(x: &[i32], w: u32, min: i32) -> Vec<u32> {
    let rows = x.len() / L;
    let words = (rows * w as usize).div_ceil(32) + 1;
    let mut out = vec![0u32; words * L];
    for t in 0..rows {
        let bit = t * w as usize;
        let (k, off) = (bit / 32, (bit % 32) as u32);
        for l in 0..L {
            let v = (x[t * L + l].wrapping_sub(min)) as u32 as u64;
            let both = v << off;
            out[k * L + l] |= both as u32;
            out[(k + 1) * L + l] |= (both >> 32) as u32;
        }
    }
    out
}

/// Fused decode: unpack row `t` of `L` lanes from `packed`, add the reference, then add the
/// order-P prediction from the previous P decoded rows. Rows `0..P` hold the warm-ups after the
/// same unpack (the predictor is skipped for them). `P == 0` is a plain unpack + FoR.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_lpc_avx512<const P: usize, const L: usize>(
    w_bits: u32,
    min: i32,
    packed: &[u32],
    coef: &[i32; P],
    out: &mut [i32],
) {
    use std::arch::x86_64::*;
    const V: usize = 16;
    let rows = out.len() / L;
    let o = out.as_mut_ptr();
    let src = packed.as_ptr();
    let mut wv = [_mm512_setzero_si512(); P];
    for j in 0..P {
        wv[j] = _mm512_set1_epi32(coef[j]);
    }
    let bias = _mm512_set1_epi32(1 << (Q - 1));
    let mask = _mm512_set1_epi32(((1u64 << w_bits) - 1) as i32);
    let minv = _mm512_set1_epi32(min);
    for t in 0..rows {
        let bit = t * w_bits as usize;
        let (k, off) = (bit / 32, (bit % 32) as u32);
        let offv = _mm_cvtsi32_si128(off as i32);
        let hiv = _mm_cvtsi32_si128((32 - off) as i32);
        let mut res = [_mm512_setzero_si512(); 8];
        for g in 0..L / V {
            let lo = unsafe { _mm512_loadu_si512(src.add(k * L + g * V).cast()) };
            let hi = unsafe { _mm512_loadu_si512(src.add((k + 1) * L + g * V).cast()) };
            let v = _mm512_or_si512(_mm512_srl_epi32(lo, offv), _mm512_sll_epi32(hi, hiv));
            res[g] = _mm512_add_epi32(_mm512_and_si512(v, mask), minv);
        }
        if P > 0 && t >= P {
            let mut acc = [bias; 8];
            for j in 0..P {
                let row = unsafe { o.add((t - P + j) * L) };
                for g in 0..L / V {
                    let v = unsafe { _mm512_loadu_si512(row.add(g * V).cast()) };
                    acc[g] = _mm512_add_epi32(acc[g], _mm512_mullo_epi32(wv[j], v));
                }
            }
            for g in 0..L / V {
                res[g] = _mm512_add_epi32(res[g], _mm512_srai_epi32::<{ Q }>(acc[g]));
            }
        }
        for g in 0..L / V {
            unsafe { _mm512_storeu_si512(o.add(t * L + g * V).cast(), res[g]) };
        }
    }
}

/// Unpack + order-P LPC vs. unpack alone, for residuals of `w` bits, checked against the
/// unfused kernels.
fn fused_row<const P: usize>(w: u32) {
    const L: usize = 32;
    let n = 1 << 20;
    let mut seed = 0x2545F4914F6CDD1Du64;
    let res: Vec<i32> = (0..n)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % (1u64 << w)) as i32 - (1 << (w - 1))
        })
        .collect();
    let min = -(1 << (w - 1));
    let packed = pack_lanes::<L>(&res, w, min);
    let coef = weights::<P>();
    let mut out = vec![0i32; n];
    assert!(is_x86_feature_detected!("avx512f"));
    unsafe { unpack_lpc_avx512::<P, L>(w, min, &packed, &coef, &mut out) };
    let mut expect = res.clone();
    if P > 0 {
        lanes::<P, L>(&coef, &mut expect);
    }
    assert_eq!(out, expect);
    let none = [0i32; 0];
    let best = |f: &mut dyn FnMut()| {
        let mut b = f64::INFINITY;
        for _ in 0..9 {
            let t = Instant::now();
            f();
            b = b.min(t.elapsed().as_secs_f64());
        }
        b * 1e9 / n as f64
    };
    let unpack = best(&mut || unsafe { unpack_lpc_avx512::<0, L>(w, min, &packed, &none, black_box(&mut out)) });
    let fused = best(&mut || unsafe { unpack_lpc_avx512::<P, L>(w, min, &packed, &coef, black_box(&mut out)) });
    println!("fused  width {w:2}  order {P:2}  unpack+FoR {unpack:5.2}  unpack+LPC {fused:5.2}  ns/value (32 lanes, 4 MiB out)");
}

/// Transpose 16 rows of 16 i32 in registers (4 rounds of 2x2 block swaps with vpermt2d).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
fn transpose16(m: &mut [std::arch::x86_64::__m512i; 16]) {
    use std::arch::x86_64::*;
    for d in [8usize, 4, 2, 1] {
        let lo: [i32; 16] = std::array::from_fn(|p| if p & d == 0 { p as i32 } else { (16 + p - d) as i32 });
        let hi: [i32; 16] = std::array::from_fn(|p| if p & d == 0 { (p + d) as i32 } else { (16 + p) as i32 });
        let (lo, hi) = unsafe { (_mm512_loadu_si512(lo.as_ptr().cast()), _mm512_loadu_si512(hi.as_ptr().cast())) };
        for i in 0..16 {
            if i & d == 0 {
                let (a, b) = (m[i], m[i + d]);
                m[i] = _mm512_permutex2var_epi32(a, lo, b);
                m[i + d] = _mm512_permutex2var_epi32(a, hi, b);
            }
        }
    }
}

/// Fused decode to the *original* order: 16 lanes, lane `l` is `out[l * R .. (l + 1) * R]`.
/// Rows are unpacked and predicted into a 64-row ring in L1, and every 16 rows the 16x16 tile
/// is transposed in registers and stored as 16 contiguous values per lane.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_lpc_ordered_avx512<const P: usize>(
    w_bits: u32,
    min: i32,
    packed: &[u32],
    coef: &[i32; P],
    out: &mut [i32],
) {
    use std::arch::x86_64::*;
    const L: usize = 16;
    const H: usize = 64;
    let r = out.len() / L;
    assert!(r % 16 == 0 && P + 16 <= H);
    let src = packed.as_ptr();
    let mut ring = [_mm512_setzero_si512(); H];
    let mut wv = [_mm512_setzero_si512(); P];
    for j in 0..P {
        wv[j] = _mm512_set1_epi32(coef[j]);
    }
    let bias = _mm512_set1_epi32(1 << (Q - 1));
    let mask = _mm512_set1_epi32(((1u64 << w_bits) - 1) as i32);
    let minv = _mm512_set1_epi32(min);
    for t0 in (0..r).step_by(16) {
        for t in t0..t0 + 16 {
            let bit = t * w_bits as usize;
            let (k, off) = (bit / 32, (bit % 32) as u32);
            let lo = unsafe { _mm512_loadu_si512(src.add(k * L).cast()) };
            let hi = unsafe { _mm512_loadu_si512(src.add((k + 1) * L).cast()) };
            let v = _mm512_or_si512(
                _mm512_srl_epi32(lo, _mm_cvtsi32_si128(off as i32)),
                _mm512_sll_epi32(hi, _mm_cvtsi32_si128((32 - off) as i32)),
            );
            let mut y = _mm512_add_epi32(_mm512_and_si512(v, mask), minv);
            if t >= P {
                let mut acc = bias;
                for j in 0..P {
                    acc = _mm512_add_epi32(acc, _mm512_mullo_epi32(wv[j], ring[(t - P + j) % H]));
                }
                y = _mm512_add_epi32(y, _mm512_srai_epi32::<{ Q }>(acc));
            }
            ring[t % H] = y;
        }
        let mut tile: [__m512i; 16] = std::array::from_fn(|i| ring[(t0 + i) % H]);
        transpose16(&mut tile);
        for (l, v) in tile.iter().enumerate() {
            unsafe { _mm512_storeu_si512(out.as_mut_ptr().add(l * r + t0).cast(), *v) };
        }
    }
}

fn ordered_row<const P: usize>(w: u32, r: usize) {
    const L: usize = 16;
    let n = 1 << 20;
    let mut seed = 0x2545F4914F6CDD1Du64;
    let res: Vec<i32> = (0..n)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % (1u64 << w)) as i32 - (1 << (w - 1))
        })
        .collect();
    let min = -(1 << (w - 1));
    // Blocks of 16 lanes x r values; each block is packed and decoded independently.
    let block = L * r;
    let packed: Vec<Vec<u32>> = res.chunks(block).map(|c| pack_lanes::<L>(c, w, min)).collect();
    let coef = weights::<P>();
    let mut out = vec![0i32; n];
    assert!(is_x86_feature_detected!("avx512f"));
    for (b, c) in res.chunks(block).enumerate() {
        // Reference: decode in the transposed layout, then untranspose.
        let mut t_major = c.to_vec();
        if P > 0 {
            lanes::<P, L>(&coef, &mut t_major);
        }
        let o = &mut out[b * block..(b + 1) * block];
        unsafe { unpack_lpc_ordered_avx512::<P>(w, min, &packed[b], &coef, o) };
        for i in 0..block {
            assert_eq!(o[i], t_major[(i % r) * L + i / r]);
        }
    }
    let mut best = f64::INFINITY;
    for _ in 0..9 {
        let t = Instant::now();
        for (b, p) in packed.iter().enumerate() {
            let o = &mut out[b * block..(b + 1) * block];
            unsafe { unpack_lpc_ordered_avx512::<P>(w, min, p, &coef, black_box(o)) };
        }
        best = best.min(t.elapsed().as_secs_f64());
    }
    println!(
        "ordered width {w:2}  order {P:2}  lane length {r:5}  unpack+LPC+transpose {:5.2} ns/value (4 MiB out)",
        best * 1e9 / n as f64
    );
}

/// Like `lanes`, but every lane has its own coefficients (one model per 1024-value chunk).
#[inline(never)]
fn lanes_own<const P: usize, const L: usize>(w: &[[i32; L]; P], x: &mut [i32]) {
    let rows = x.len() / L;
    for t in P..rows {
        let (hist, cur) = x.split_at_mut(t * L);
        let cur: &mut [i32; L] = (&mut cur[..L]).try_into().unwrap();
        let mut acc = [1i32 << (Q - 1); L];
        for j in 0..P {
            let row: &[i32; L] = hist[(t - P + j) * L..(t - P + j + 1) * L].try_into().unwrap();
            for l in 0..L {
                acc[l] = acc[l].wrapping_add(w[j][l].wrapping_mul(row[l]));
            }
        }
        for l in 0..L {
            cur[l] = cur[l].wrapping_add(acc[l] >> Q);
        }
    }
}

#[inline(never)]
fn lanes64<const P: usize, const L: usize>(w: &[i64; P], x: &mut [i32]) {
    let rows = x.len() / L;
    for t in P..rows {
        let (hist, cur) = x.split_at_mut(t * L);
        let cur: &mut [i32; L] = (&mut cur[..L]).try_into().unwrap();
        let mut acc = [1i64 << (Q - 1); L];
        for j in 0..P {
            let row: &[i32; L] = hist[(t - P + j) * L..(t - P + j + 1) * L].try_into().unwrap();
            for l in 0..L {
                acc[l] += w[j] * row[l] as i64;
            }
        }
        for l in 0..L {
            cur[l] = cur[l].wrapping_add((acc[l] >> Q) as i32);
        }
    }
}

#[inline(never)]
fn delta_serial(x: &mut [i32]) {
    for i in 1..x.len() {
        x[i] = x[i].wrapping_add(x[i - 1]);
    }
}

#[inline(never)]
fn delta_lanes<const L: usize>(x: &mut [i32]) {
    let rows = x.len() / L;
    for t in 1..rows {
        let (hist, cur) = x.split_at_mut(t * L);
        let prev = &hist[(t - 1) * L..];
        for l in 0..L {
            cur[l] = cur[l].wrapping_add(prev[l]);
        }
    }
}

fn time(base: &[i32], f: &mut dyn FnMut(&mut [i32])) -> f64 {
    let mut buf = base.to_vec();
    let mut best = f64::INFINITY;
    let mut copy = f64::INFINITY;
    for _ in 0..9 {
        let t = Instant::now();
        buf.copy_from_slice(base);
        copy = copy.min(t.elapsed().as_secs_f64());
        let t = Instant::now();
        buf.copy_from_slice(base);
        f(black_box(&mut buf));
        best = best.min(t.elapsed().as_secs_f64());
    }
    black_box(&buf);
    (best - copy).max(1e-12) * 1e9 / base.len() as f64
}

fn weights<const P: usize>() -> [i32; P] {
    // A stable-ish predictor with sum of weights = 1 << Q (DC gain 1).
    let mut w = [0i32; P];
    if P == 0 {
        return w;
    }
    w[P - 1] = 3 << (Q - 1);
    if P >= 2 {
        w[P - 2] = -(1 << (Q - 1));
    }
    for j in 0..P.saturating_sub(2) {
        w[j] = if j % 2 == 0 { 7 } else { -7 };
    }
    w
}

fn row<const P: usize>(base: &[i32]) {
    let w = weights::<P>();
    let w64: [i64; P] = std::array::from_fn(|j| w[j] as i64);
    let s32 = time(base, &mut |x| serial::<P>(&w, x));
    let s64 = time(base, &mut |x| serial64::<P>(&w64, x));
    let l8 = time(base, &mut |x| lanes::<P, 8>(&w, x));
    let l16 = time(base, &mut |x| lanes::<P, 16>(&w, x));
    let l32 = time(base, &mut |x| lanes::<P, 32>(&w, x));
    let l64 = time(base, &mut |x| lanes::<P, 64>(&w, x));
    let l32w = time(base, &mut |x| lanes64::<P, 32>(&w64, x));
    let own: [[i32; 64]; P] = std::array::from_fn(|j| std::array::from_fn(|l| w[j] + (l as i32 % 3) - 1));
    let l64own = time(base, &mut |x| lanes_own::<P, 64>(&own, x));
    let v16 = time(base, &mut |x| lanes_simd::<P, 16>(&w, x));
    let v32 = time(base, &mut |x| lanes_simd::<P, 32>(&w, x));
    let v64 = time(base, &mut |x| lanes_simd::<P, 64>(&w, x));
    let v128 = time(base, &mut |x| lanes_simd::<P, 128>(&w, x));
    println!(
        "order {P:2}  serial-i32 {s32:6.2}  serial-i64 {s64:6.2}  lanes8 {l8:5.2}  lanes16 {l16:5.2}  lanes32 {l32:5.2}  lanes64 {l64:5.2}  lanes32-i64 {l32w:5.2}  lanes64-own-weights {l64own:5.2}  avx512 16/32/64/128 {v16:5.2} {v32:5.2} {v64:5.2} {v128:5.2}  ns/value"
    );
}

/// Check that the lane kernel equals the serial kernel run on each de-interleaved lane.
fn check<const P: usize>(base: &[i32]) {
    const L: usize = 64;
    let w = weights::<P>();
    let n = base.len() / L * L;
    let mut x = base[..n].to_vec();
    lanes::<P, L>(&w, &mut x);
    let mut y = base[..n].to_vec();
    lanes_simd::<P, L>(&w, &mut y);
    assert_eq!(x, y);
    for l in 0..L {
        let mut lane: Vec<i32> = (0..n / L).map(|t| base[t * L + l]).collect();
        serial::<P>(&w, &mut lane);
        for t in 0..n / L {
            assert_eq!(lane[t], x[t * L + l]);
        }
    }
}

fn main() {
    let mut seed = 0x9E3779B97F4A7C15u64;
    let base: Vec<i32> = (0..N)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % 64) as i32 - 32
        })
        .collect();
    check::<4>(&base[..1 << 16]);
    check::<8>(&base[..1 << 16]);
    check::<32>(&base[..1 << 16]);
    println!("{} values, ns/value excluding a memcpy of the input", N);
    println!(
        "delta      serial {:6.2}  lanes32 {:5.2}",
        time(&base, &mut |x| delta_serial(x)),
        time(&base, &mut |x| delta_lanes::<32>(x))
    );
    row::<1>(&base);
    row::<2>(&base);
    row::<4>(&base);
    row::<8>(&base);
    row::<16>(&base);
    row::<32>(&base);
    for w in [6, 10, 14] {
        fused_row::<1>(w);
        fused_row::<4>(w);
        fused_row::<8>(w);
        fused_row::<16>(w);
        fused_row::<32>(w);
    }
    for r in [1024, 65536] {
        ordered_row::<0>(10, r);
        ordered_row::<4>(10, r);
        ordered_row::<8>(10, r);
        ordered_row::<16>(10, r);
        ordered_row::<32>(10, r);
    }
}
