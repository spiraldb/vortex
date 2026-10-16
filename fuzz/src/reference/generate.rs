// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Small, encoding-friendly reference data generators.
//!
//! Inputs are kept deliberately simple (short arrays, small alphabets, a handful of data shapes)
//! so that every encoding accepts them and failures stay easy to read.

use arbitrary::Unstructured;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::half::f16;

use super::value::Value;

/// The range of the Timestamp extension type, in microseconds.
pub const MIN_TIMESTAMP_MICROS: i128 = -377_705_023_200_000_000;
pub const MAX_TIMESTAMP_MICROS: i128 = 253_402_207_199_999_999;

/// Maximum number of rows generated for a single reference array.
pub const MAX_LEN: usize = 256;

/// The overall shape of generated values. Different encodings only kick in for some shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// Arbitrary values across the whole domain.
    Random,
    /// Values within a narrow window above a random base (frame of reference, bit packing).
    Narrow,
    /// Runs of repeated values (run-end, RLE).
    Runs,
    /// An arithmetic sequence (sequence, delta).
    Sequence,
    /// A handful of distinct values (dictionary).
    FewDistinct,
}

impl Shape {
    pub fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        Ok(match u.int_in_range(0..=4)? {
            0 => Shape::Random,
            1 => Shape::Narrow,
            2 => Shape::Runs,
            3 => Shape::Sequence,
            _ => Shape::FewDistinct,
        })
    }
}

/// Generator options for one array.
#[derive(Debug, Clone, Copy)]
pub struct GenOptions {
    pub len: usize,
    pub shape: Shape,
    /// Probability numerator (out of 8) of a null at each position; zero for non-nullable dtypes.
    pub null_eighths: u8,
}

impl GenOptions {
    pub fn arbitrary(
        u: &mut Unstructured<'_>,
        nullability: Nullability,
        max_len: usize,
    ) -> arbitrary::Result<Self> {
        Ok(Self {
            len: u.int_in_range(0..=max_len)?,
            shape: Shape::arbitrary(u)?,
            null_eighths: match nullability {
                Nullability::NonNullable => 0,
                Nullability::Nullable => u.int_in_range(0..=4)?,
            },
        })
    }
}

/// Generate `opts.len` reference values of a leaf dtype.
pub fn gen_values(
    u: &mut Unstructured<'_>,
    dtype: &DType,
    opts: GenOptions,
) -> arbitrary::Result<Vec<Value>> {
    let mut base = gen_shaped(u, dtype, opts)?;
    if opts.null_eighths > 0 && dtype.is_nullable() {
        for v in base.iter_mut() {
            if u.ratio(opts.null_eighths, 8)? {
                *v = Value::Null;
            }
        }
    }
    Ok(base)
}

fn gen_shaped(
    u: &mut Unstructured<'_>,
    dtype: &DType,
    opts: GenOptions,
) -> arbitrary::Result<Vec<Value>> {
    let len = opts.len;
    match opts.shape {
        Shape::Runs => {
            let mut out = Vec::with_capacity(len);
            while out.len() < len {
                let v = gen_one(u, dtype, opts.shape, out.len())?;
                let run = u.int_in_range(1..=16)?.min(len - out.len());
                out.extend(std::iter::repeat_n(v, run));
            }
            Ok(out)
        }
        Shape::FewDistinct => {
            let n = u.int_in_range(1..=4)?;
            let dict = (0..n)
                .map(|i| gen_one(u, dtype, Shape::Random, i))
                .collect::<arbitrary::Result<Vec<_>>>()?;
            (0..len).map(|_| Ok(u.choose(&dict)?.clone())).collect()
        }
        Shape::Sequence => {
            let (start, step) = match dtype {
                DType::Primitive(p, _) if p.is_int() => {
                    let (lo, hi) = int_bounds(*p);
                    let step = i128::from(u.int_in_range(-3i8..=3)?);
                    let lo = lo.saturating_sub(step.min(0) * len as i128);
                    let hi = hi.saturating_sub(step.max(0) * len as i128);
                    if lo > hi {
                        return gen_shaped(
                            u,
                            dtype,
                            GenOptions {
                                shape: Shape::Random,
                                ..opts
                            },
                        );
                    }
                    (clamp_pick(u, lo, hi)?, step)
                }
                _ => {
                    return (0..len)
                        .map(|i| gen_one(u, dtype, Shape::Sequence, i))
                        .collect();
                }
            };
            Ok((0..len)
                .map(|i| Value::Int(start + step * i as i128))
                .collect())
        }
        Shape::Narrow => {
            let base = match dtype {
                DType::Primitive(p, _) if p.is_int() => {
                    let (lo, hi) = int_bounds(*p);
                    Some((clamp_pick(u, lo, hi.saturating_sub(255).max(lo))?, hi))
                }
                _ => None,
            };
            match base {
                Some((base, hi)) => (0..len)
                    .map(|_| {
                        let width = i128::from(u.int_in_range(0u8..=u8::MAX)?);
                        Ok(Value::Int((base + width).min(hi)))
                    })
                    .collect(),
                None => (0..len)
                    .map(|i| gen_one(u, dtype, Shape::Narrow, i))
                    .collect(),
            }
        }
        Shape::Random => (0..len)
            .map(|i| gen_one(u, dtype, Shape::Random, i))
            .collect(),
    }
}

/// Generate a single non-null value of a leaf dtype.
pub fn gen_one(
    u: &mut Unstructured<'_>,
    dtype: &DType,
    shape: Shape,
    idx: usize,
) -> arbitrary::Result<Value> {
    Ok(match dtype {
        DType::Bool(_) => Value::Bool(u.arbitrary()?),
        DType::Primitive(p, _) if p.is_int() => {
            let (lo, hi) = int_bounds(*p);
            Value::Int(clamp_pick(u, lo, hi)?)
        }
        DType::Primitive(p, _) => Value::Float(gen_float(u, *p, shape, idx)?),
        DType::Decimal(d, _) => {
            // Wider decimals (e.g. sum results) still get values that fit in an i128.
            let max = 10i128.pow(u32::from(d.precision().min(38))) - 1;
            Value::Decimal(clamp_pick(u, -max, max)?)
        }
        DType::Utf8(_) => Value::Bytes(gen_string(u)?.into_bytes()),
        DType::Binary(_) => {
            let n = u.int_in_range(0..=12)?;
            Value::Bytes(
                (0..n)
                    .map(|_| u.arbitrary::<u8>())
                    .collect::<arbitrary::Result<_>>()?,
            )
        }
        // The only generated extension type is a microsecond timestamp.
        DType::Extension(ext) => match gen_one(u, ext.storage_dtype(), shape, idx)? {
            Value::Int(v) => Value::Int(v.clamp(MIN_TIMESTAMP_MICROS, MAX_TIMESTAMP_MICROS)),
            v => v,
        },
        _ => Value::Null,
    })
}

/// Strings from a small alphabet so FSST/OnPair symbol tables and LIKE patterns hit.
pub fn gen_string(u: &mut Unstructured<'_>) -> arbitrary::Result<String> {
    const ALPHABET: &[char] = &['a', 'b', 'c', 'd', ' ', '_', '%', 'é', '🦀'];
    let n = u.int_in_range(0..=16)?;
    (0..n).map(|_| Ok(*u.choose(ALPHABET)?)).collect()
}

fn gen_float(
    u: &mut Unstructured<'_>,
    ptype: PType,
    shape: Shape,
    idx: usize,
) -> arbitrary::Result<f64> {
    let v = match shape {
        // Decimal-like values that ALP encodes losslessly.
        Shape::Narrow | Shape::Sequence => {
            let cents = i64::from(u.int_in_range(-100_000i32..=100_000)?);
            let v = cents as f64 / 100.0;
            if shape == Shape::Sequence {
                v + idx as f64
            } else {
                v
            }
        }
        _ => match u.int_in_range(0..=9)? {
            0 => f64::NAN,
            1 => f64::INFINITY,
            2 => f64::NEG_INFINITY,
            3 => -0.0,
            _ => match ptype {
                PType::F16 => f64::from(f16::from_bits(u.arbitrary()?)),
                PType::F32 => f64::from(f32::from_bits(u.arbitrary()?)),
                _ => f64::from_bits(u.arbitrary()?),
            },
        },
    };
    Ok(round_to(ptype, v))
}

/// Round a value to the precision of the target float type, so the reference matches storage.
pub fn round_to(ptype: PType, v: f64) -> f64 {
    match ptype {
        PType::F16 => f64::from(f16::from_f64(v)),
        #[expect(
            clippy::cast_possible_truncation,
            reason = "rounding to storage precision"
        )]
        PType::F32 => f64::from(v as f32),
        _ => v,
    }
}

pub fn int_bounds(ptype: PType) -> (i128, i128) {
    match ptype {
        PType::U8 => (0, i128::from(u8::MAX)),
        PType::U16 => (0, i128::from(u16::MAX)),
        PType::U32 => (0, i128::from(u32::MAX)),
        PType::U64 => (0, i128::from(u64::MAX)),
        PType::I8 => (i128::from(i8::MIN), i128::from(i8::MAX)),
        PType::I16 => (i128::from(i16::MIN), i128::from(i16::MAX)),
        PType::I32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
        PType::I64 => (i128::from(i64::MIN), i128::from(i64::MAX)),
        _ => (0, 0),
    }
}

/// Pick a value in `lo..=hi`, biased towards small magnitudes and the bounds.
fn clamp_pick(u: &mut Unstructured<'_>, lo: i128, hi: i128) -> arbitrary::Result<i128> {
    Ok(match u.int_in_range(0..=5)? {
        0 => lo,
        1 => hi,
        2 | 3 => i128::from(u.int_in_range(-64i8..=64)?).clamp(lo, hi),
        _ => u.int_in_range(lo..=hi)?,
    })
}
