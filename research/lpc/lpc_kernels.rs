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
    println!(
        "order {P:2}  serial-i32 {s32:6.2}  serial-i64 {s64:6.2}  lanes8 {l8:5.2}  lanes16 {l16:5.2}  lanes32 {l32:5.2}  lanes64 {l64:5.2}  lanes32-i64 {l32w:5.2}  lanes64-own-weights {l64own:5.2}  ns/value"
    );
}

/// Check that the lane kernel equals the serial kernel run on each de-interleaved lane.
fn check<const P: usize>(base: &[i32]) {
    const L: usize = 64;
    let w = weights::<P>();
    let n = base.len() / L * L;
    let mut x = base[..n].to_vec();
    lanes::<P, L>(&w, &mut x);
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
}
