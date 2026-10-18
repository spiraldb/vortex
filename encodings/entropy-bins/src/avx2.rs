// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! AVX2 kernels for CPUs without the AVX-512 subset `x86.rs` needs (Haswell to Alder Lake, Zen 1
//! to 3). Every kernel produces exactly what the portable decoder in `decode.rs` does.
//!
//! The 16 tANS lanes are two 8-lane halves. AVX2 has no 128-entry byte permute, no `lzcnt` and
//! no expand, so a step gathers one packed `xs | nb << 8 | sym << 16` entry per lane, and a
//! refill expands the next words into the lanes that need them with a 256-entry permute table.
//! The merge for outputs of at most 32 bits looks bins up with permutes, reads offsets from a
//! 32-byte window and keeps a lag's prefix sum in registers; wider values use the unrolled
//! portable merge, whose loads beat 64-bit gathers.
//!
//! Vector loads and gathers read past a block's words and offsets; the array's data buffer ends
//! with [`TAIL_PADDING`](crate::coder::TAIL_PADDING) zero bytes, which covers every over-read.

#![allow(
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::arch::x86_64::*;

use crate::coder::IdTable;
use crate::coder::LANES;
use crate::coder::MAX_LAG;
use crate::decode::BlockView;
use crate::decode::ChunkDecoder;
use crate::decode::OutInt;

/// For each 8-bit mask, the permute that moves word `k` into the lane holding the mask's `k`-th
/// set bit.
static EXPAND: [[u32; 8]; 256] = {
    let mut t = [[0u32; 8]; 256];
    let mut m = 0;
    while m < 256 {
        let mut j = 0;
        let mut c = 0;
        while j < 8 {
            if (m >> j) & 1 == 1 {
                t[m][j] = c;
                c += 1;
            }
            j += 1;
        }
        m += 1;
    }
    t
};

/// Decode the bin ids of `V` blocks in lockstep (all with the same length), each into a buffer
/// of at least the block length rounded up to a multiple of 16.
///
/// # Safety
///
/// The CPU must support AVX2 and POPCNT; every `outs[v]` must point to at least that many
/// writable bytes; every block must be non-uniform and come from a data buffer with tail padding.
pub(crate) unsafe fn ids16<const V: usize>(
    t: &IdTable,
    blocks: [&BlockView<'_>; V],
    outs: [*mut u8; V],
    limit: usize,
) {
    // SAFETY: forwarded from the caller.
    unsafe {
        match (t.word_bits, t.r) {
            (16, 8) => ids_kernel::<V, 8, 16>(t, blocks, outs, limit),
            (16, 5) => ids_kernel::<V, 5, 16>(t, blocks, outs, limit),
            (16, 4) => ids_kernel::<V, 4, 16>(t, blocks, outs, limit),
            (16, 3) => ids_kernel::<V, 3, 16>(t, blocks, outs, limit),
            (16, _) => ids_kernel::<V, 2, 16>(t, blocks, outs, limit),
            (_, 8) => ids_kernel::<V, 8, 8>(t, blocks, outs, limit),
            (_, 4) => ids_kernel::<V, 4, 8>(t, blocks, outs, limit),
            (_, 2) => ids_kernel::<V, 2, 8>(t, blocks, outs, limit),
            _ => ids_kernel::<V, 1, 8>(t, blocks, outs, limit),
        }
    }
}

#[target_feature(enable = "avx2,popcnt")]
unsafe fn ids_kernel<const V: usize, const R: usize, const W: u32>(
    t: &IdTable,
    blocks: [&BlockView<'_>; V],
    outs: [*mut u8; V],
    limit: usize,
) {
    // SAFETY: gathers index the 128-entry table with `x & 127`; word loads stay within the
    // padded data buffer and stores within the caller's block-sized buffers.
    unsafe {
        let tab = t.dec32.as_ptr().cast::<i32>();
        let idx_mask = _mm256_set1_epi32(127);
        let ff = _mm256_set1_epi32(0xff);
        let ones = _mm256_set1_epi32(-1);
        let cw = _mm256_set1_epi32(W as i32);
        let wm1 = _mm256_set1_epi32(W as i32 - 1);
        let ll = _mm256_set1_epi32(1i32 << t.s);
        let zero = _mm256_setzero_si256();
        let mut p = [std::ptr::null::<u8>(); V];
        let mut end = [std::ptr::null::<u8>(); V];
        let mut buf = [[zero; 2]; V];
        let mut avail = [[zero; 2]; V];
        let mut x = [[ll; 2]; V];
        let mut stop = [[zero; 2]; V];
        for v in 0..V {
            p[v] = blocks[v].words.as_ptr();
            end[v] = p[v].add(blocks[v].words_len);
            for h in 0..2 {
                let st = _mm_loadl_epi64(blocks[v].states.as_ptr().add(8 * h).cast());
                x[v][h] = _mm256_add_epi32(_mm256_cvtepu8_epi32(st), ll);
                stop[v][h] = _mm256_cvtepu16_epi32(_mm_loadu_si128(
                    blocks[v].stop.as_ptr().add(8 * h).cast(),
                ));
            }
        }
        let steps = blocks[0].n.min(limit).div_ceil(LANES);
        let mut step = 0;
        while step < steps {
            let rv = _mm256_set1_epi32((step / R) as i32);
            for v in 0..V {
                for h in 0..2 {
                    let live = _mm256_cmpgt_epi32(stop[v][h], rv);
                    // Unsigned `avail < W`: a lane's last step may wrap `avail` below zero.
                    let low = _mm256_cmpeq_epi32(_mm256_min_epu32(avail[v][h], wm1), avail[v][h]);
                    let need = _mm256_and_si256(live, low);
                    let m = _mm256_movemask_ps(_mm256_castsi256_ps(need)) as usize;
                    let words = if W == 16 {
                        _mm256_cvtepu16_epi32(_mm_loadu_si128(p[v].cast()))
                    } else {
                        _mm256_cvtepu8_epi32(_mm_loadl_epi64(p[v].cast()))
                    };
                    let perm = _mm256_loadu_si256(EXPAND[m].as_ptr().cast());
                    let nw = _mm256_and_si256(_mm256_permutevar8x32_epi32(words, perm), need);
                    buf[v][h] = _mm256_or_si256(buf[v][h], _mm256_sllv_epi32(nw, avail[v][h]));
                    avail[v][h] = _mm256_add_epi32(avail[v][h], _mm256_and_si256(need, cw));
                    // As in the AVX-512 kernel, a corrupt stream must not walk past its words.
                    p[v] = p[v]
                        .add(m.count_ones() as usize * (W as usize / 8))
                        .min(end[v]);
                }
            }
            let round_end = (step + R).min(steps);
            while step < round_end {
                for v in 0..V {
                    let mut sym = [zero; 2];
                    for h in 0..2 {
                        let e =
                            _mm256_i32gather_epi32::<4>(tab, _mm256_and_si256(x[v][h], idx_mask));
                        let xs = _mm256_and_si256(e, ff);
                        let nb = _mm256_and_si256(_mm256_srli_epi32::<8>(e), ff);
                        let bits = _mm256_andnot_si256(_mm256_sllv_epi32(ones, nb), buf[v][h]);
                        buf[v][h] = _mm256_srlv_epi32(buf[v][h], nb);
                        avail[v][h] = _mm256_sub_epi32(avail[v][h], nb);
                        x[v][h] = _mm256_or_si256(_mm256_sllv_epi32(xs, nb), bits);
                        sym[h] = _mm256_srli_epi32::<16>(e);
                    }
                    // [a0-3 b0-3 | a4-7 b4-7] -> [a0-7 | b0-7] as u16 -> 16 bytes in lane order.
                    let w16 = _mm256_packus_epi32(sym[0], sym[1]);
                    let w16 = _mm256_permute4x64_epi64::<0b11_01_10_00>(w16);
                    let b8 = _mm256_packus_epi16(w16, w16);
                    let b8 = _mm256_permute4x64_epi64::<0b00_00_10_00>(b8);
                    _mm_storeu_si128(outs[v].add(step * LANES).cast(), _mm256_castsi256_si128(b8));
                }
                step += 1;
            }
        }
    }
}

/// Merge a decoded block's ids with its offsets into `out`, if a kernel applies. Returns `false`
/// when the caller must use the portable merge. `seeds` is as for
/// [`merge_scalar`](crate::decode::merge_scalar).
pub(crate) fn merge<T: OutInt>(
    d: &ChunkDecoder,
    b: &BlockView<'_>,
    ids: &[u8],
    out: &mut [T],
    seeds: &[u64],
) -> bool {
    let nb = d.widths.len();
    let n = b.n;
    // Wider values go to the unrolled portable merge: on AVX2 its loads beat 64-bit gathers.
    if T::BYTES > 4 || nb > 32 {
        return false;
    }
    if out.len() < n || ids.len() < n.div_ceil(LANES) * LANES + 16 || seeds.len() > MAX_LAG {
        return false;
    }
    // Window loads and gathers read up to 8 bytes at an offset's byte; valid blocks have later blocks or tail
    // padding behind them.
    if b.offsets.len() < 64 + crate::decode::offset_bytes_portable(d, &ids[..n], b.offsets.len()) {
        return false;
    }
    // A chunk's widest bins are often rare outliers: check the widths this block uses instead.
    let max_width = if d.max_width > 28 {
        ids[..n]
            .iter()
            .map(|&id| d.widths[usize::from(id)])
            .max()
            .unwrap_or(0)
    } else {
        d.max_width
    };
    if max_width > 31 {
        return false;
    }
    dispatch32(d, b, ids, out, seeds, max_width);
    true
}

/// Run the `merge32` instance for the block's bin count, widths and output width.
fn dispatch32<T: OutInt>(
    d: &ChunkDecoder,
    b: &BlockView<'_>,
    ids: &[u8],
    out: &mut [T],
    seeds: &[u64],
    max_width: u32,
) {
    let nb = d.widths.len();
    let n = b.n;
    let o = out.as_mut_ptr().cast::<u8>();
    macro_rules! go32 {
        ($tb:literal, $rd:expr) => {{
            const RD: u8 = $rd as u8;
            macro_rules! out {
                ($prefix: literal) => {
                    match T::BYTES {
                        // SAFETY: AVX2 is available (checked by the caller), `out` holds `n`
                        // values, `ids` holds the block's ids plus slack, and offsets come from
                        // a padded buffer.
                        1 => unsafe {
                            merge32::<$tb, RD, 1, $prefix>(d, ids, b.offsets, n, o, seeds)
                        },
                        2 => unsafe {
                            merge32::<$tb, RD, 2, $prefix>(d, ids, b.offsets, n, o, seeds)
                        },
                        _ => unsafe {
                            merge32::<$tb, RD, 4, $prefix>(d, ids, b.offsets, n, o, seeds)
                        },
                    }
                };
            }
            if seeds.is_empty() {
                out!(false)
            } else {
                out!(true)
            }
        }};
    }
    macro_rules! go32_tb {
        ($rd:expr) => {
            if nb <= 8 {
                go32!(0, $rd)
            } else if nb <= 16 {
                go32!(1, $rd)
            } else {
                go32!(2, $rd)
            }
        };
    }
    if max_width <= 28 {
        go32_tb!(Read32::Window);
    } else {
        go32_tb!(Read32::Gather8);
    }
}

/// Lane permutes and masks for an in-register lag-`k` prefix sum over `L` lanes (as in the AVX-512
/// kernels): step `t` adds lane `j - (k << t)` into lane `j`, and the carry broadcasts each lane's
/// last earlier value in its residue class (`L - k + j % k`) from the previous vector.
struct Lag<const L: usize> {
    shift_idx: [[i32; 8]; 3],
    shift_mask: [[i32; 8]; 3],
    steps: usize,
    carry_idx: [i32; 8],
    seed_mask: [i32; 8],
}

impl<const L: usize> Lag<L> {
    fn new(lag: usize) -> Self {
        let mut plan = Self {
            shift_idx: [[0; 8]; 3],
            shift_mask: [[0; 8]; 3],
            steps: 0,
            carry_idx: [0; 8],
            seed_mask: [0; 8],
        };
        let k = lag.max(1);
        for t in 0..3 {
            let s = k << t;
            if s >= L {
                break;
            }
            plan.steps = t + 1;
            for j in s..L {
                plan.shift_idx[t][j] = (j - s) as i32;
                plan.shift_mask[t][j] = -1;
            }
        }
        for j in 0..L {
            plan.carry_idx[j] = (L - k + j % k) as i32;
            if j < lag {
                plan.seed_mask[j] = -1;
            }
        }
        plan
    }
}

/// Store the low `OUT` bytes of 8 u32 lanes; `k < 8` stores only the first `k` values.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn store32<const OUT: usize>(dst: *mut u8, o: __m256i, k: usize) {
    // SAFETY: the caller's buffer holds `k` values of `OUT` bytes at `dst`.
    unsafe {
        let mut tmp = [0u8; 32];
        let full = k == 8;
        let target = if full { dst } else { tmp.as_mut_ptr() };
        match OUT {
            4 => _mm256_storeu_si256(target.cast(), o),
            2 => {
                let sh = _mm256_setr_epi8(
                    0, 1, 4, 5, 8, 9, 12, 13, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 4, 5, 8, 9, 12,
                    13, -1, -1, -1, -1, -1, -1, -1, -1,
                );
                let s = _mm256_permute4x64_epi64::<0b00_00_10_00>(_mm256_shuffle_epi8(o, sh));
                _mm_storeu_si128(target.cast(), _mm256_castsi256_si128(s));
            }
            _ => {
                let sh = _mm256_setr_epi8(
                    0, 4, 8, 12, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 4, 8, 12, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                );
                let s = _mm256_shuffle_epi8(o, sh);
                let s = _mm256_permutevar8x32_epi32(s, _mm256_setr_epi32(0, 4, 0, 0, 0, 0, 0, 0));
                _mm_storel_epi64(target.cast(), _mm256_castsi256_si128(s));
            }
        }
        if !full {
            std::ptr::copy_nonoverlapping(tmp.as_ptr(), dst, k * OUT);
        }
    }
}

/// Look up 8 u32 lanes of a 32-entry table held in four registers: `TB` 0 (<= 8 entries), 1
/// (<= 16) or 2 (<= 32). Permutes and blends instead of a gather, which is slow before Ice Lake
/// and on Zen 1 to 3.
#[inline]
#[target_feature(enable = "avx2")]
fn lookup32<const TB: u8>(t: &[__m256i; 4], id: __m256i) -> __m256i {
    let a = _mm256_permutevar8x32_epi32(t[0], id);
    if TB == 0 {
        return a;
    }
    // Bit 3 (then 4) of the id, moved to the sign bit, picks the register.
    let blend = |x: __m256i, y: __m256i, sel: __m256i| {
        _mm256_castps_si256(_mm256_blendv_ps(
            _mm256_castsi256_ps(x),
            _mm256_castsi256_ps(y),
            _mm256_castsi256_ps(sel),
        ))
    };
    let b3 = _mm256_slli_epi32::<28>(id);
    let lo = blend(a, _mm256_permutevar8x32_epi32(t[1], id), b3);
    if TB == 1 {
        return lo;
    }
    let hi = blend(
        _mm256_permutevar8x32_epi32(t[2], id),
        _mm256_permutevar8x32_epi32(t[3], id),
        b3,
    );
    blend(lo, hi, _mm256_slli_epi32::<27>(id))
}

/// How a 32-bit merge reads offsets: from a 32-byte window with a funnel shift (widths <= 28, so
/// 8 offsets fit), or by gathering 8 bytes per offset.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Read32 {
    Window,
    Gather8,
}

/// 8 values per step in u32 lanes for outputs of at most 32 bits; arithmetic wraps mod 2^32,
/// which is exact after truncation. Requires <= 32 bins and widths <= 31.
#[target_feature(enable = "avx2")]
unsafe fn merge32<const TB: u8, const RD: u8, const OUT: usize, const PREFIX: bool>(
    d: &ChunkDecoder,
    ids: &[u8],
    offsets: &[u8],
    n: usize,
    out: *mut u8,
    seeds: &[u64],
) {
    // SAFETY: table loads read 32-entry arrays; offset loads stay within the caller's padded
    // buffer; stores cover `n` values.
    unsafe {
        let ld = |p: *const u32| -> [__m256i; 4] {
            std::array::from_fn(|r| _mm256_loadu_si256(p.add(8 * r).cast()))
        };
        let wt = ld(d.w32.as_ptr());
        let lt = ld(d.tl32.as_ptr());
        let zero = _mm256_setzero_si256();
        let ones = _mm256_set1_epi32(-1);
        let seven = _mm256_set1_epi32(7);
        let one = _mm256_set1_epi32(1);
        let thirty_one = _mm256_set1_epi32(31);
        let thirty_two = _mm256_set1_epi32(32);
        let three = _mm256_set1_epi32(3);
        let evens = _mm256_setr_epi32(0, 2, 4, 6, 0, 2, 4, 6);
        let bp = offsets.as_ptr();
        let ip = ids.as_ptr();
        let lag = Lag::<8>::new(seeds.len());
        let ld8 = |a: &[i32; 8]| _mm256_loadu_si256(a.as_ptr().cast());
        let sh_idx: [__m256i; 3] = std::array::from_fn(|t| ld8(&lag.shift_idx[t]));
        let sh_mask: [__m256i; 3] = std::array::from_fn(|t| ld8(&lag.shift_mask[t]));
        let carry_idx = ld8(&lag.carry_idx);
        let seed_mask = ld8(&lag.seed_mask);
        let mut seed32 = [0i32; 8];
        for (s, &v) in seed32.iter_mut().zip(seeds) {
            *s = v as i32;
        }
        let seedv = ld8(&seed32);
        let mut carry_v = zero;
        let mut base = 0u32;
        let mut i = 0;
        while i < n {
            let id = _mm256_cvtepu8_epi32(_mm_loadl_epi64(ip.add(i).cast()));
            let w = lookup32::<TB>(&wt, id);
            let lv = lookup32::<TB>(&lt, id);
            let mut incl = _mm256_add_epi32(w, _mm256_slli_si256::<4>(w));
            incl = _mm256_add_epi32(incl, _mm256_slli_si256::<8>(incl));
            let carry = _mm256_permutevar8x32_epi32(incl, three);
            incl = _mm256_add_epi32(incl, _mm256_blend_epi32::<0b1111_0000>(zero, carry));
            let ex = _mm256_sub_epi32(incl, w);
            let r = if RD == Read32::Window as u8 {
                let win = _mm256_loadu_si256(bp.add((base >> 3) as usize).cast());
                let rel = _mm256_add_epi32(_mm256_set1_epi32((base & 7) as i32), ex);
                let q = _mm256_srli_epi32::<5>(rel);
                let sh = _mm256_and_si256(rel, thirty_one);
                let lo = _mm256_permutevar8x32_epi32(win, q);
                let hi = _mm256_permutevar8x32_epi32(win, _mm256_add_epi32(q, one));
                // Shifting by 32 yields zero, so an aligned offset takes nothing from `hi`; when
                // `q + 1` wraps, the bits it brings lie above the offset's width.
                _mm256_or_si256(
                    _mm256_srlv_epi32(lo, sh),
                    _mm256_sllv_epi32(hi, _mm256_sub_epi32(thirty_two, sh)),
                )
            } else {
                let bit = _mm256_add_epi32(_mm256_set1_epi32(base as i32), ex);
                let byte = _mm256_srli_epi32::<3>(bit);
                let sh = _mm256_and_si256(bit, seven);
                {
                    let bp64 = bp.cast::<i64>();
                    let lo = _mm256_i32gather_epi64::<1>(bp64, _mm256_castsi256_si128(byte));
                    let hi = _mm256_i32gather_epi64::<1>(bp64, _mm256_extracti128_si256::<1>(byte));
                    let lo =
                        _mm256_srlv_epi64(lo, _mm256_cvtepu32_epi64(_mm256_castsi256_si128(sh)));
                    let hi = _mm256_srlv_epi64(
                        hi,
                        _mm256_cvtepu32_epi64(_mm256_extracti128_si256::<1>(sh)),
                    );
                    _mm256_blend_epi32::<0b1111_0000>(
                        _mm256_permutevar8x32_epi32(lo, evens),
                        _mm256_permutevar8x32_epi32(hi, evens),
                    )
                }
            };
            let r = _mm256_andnot_si256(_mm256_sllv_epi32(ones, w), r);
            let mut o = _mm256_add_epi32(lv, r);
            if PREFIX {
                if i == 0 {
                    o = _mm256_blendv_epi8(o, seedv, seed_mask);
                }
                for t in 0..lag.steps {
                    let moved = _mm256_permutevar8x32_epi32(o, sh_idx[t]);
                    o = _mm256_add_epi32(o, _mm256_and_si256(moved, sh_mask[t]));
                }
                o = _mm256_add_epi32(o, carry_v);
                carry_v = _mm256_permutevar8x32_epi32(o, carry_idx);
            }
            store32::<OUT>(out.add(i * OUT), o, (n - i).min(8));
            base = base.wrapping_add(_mm256_extract_epi32::<7>(incl) as u32);
            i += 8;
        }
    }
}
