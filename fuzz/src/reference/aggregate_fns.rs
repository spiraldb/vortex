// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Aggregate functions and their plain Rust reference implementations.
//!
//! Every registered aggregate function has an [`AggregateKind`]. Aggregates run through the
//! generic accumulator, either over the whole array or over two halves whose partial states are
//! merged, and the result is compared with a reference computed over [`Value`]s.

use std::num::NonZeroUsize;

use arbitrary::Error::IncorrectFormat;
use arbitrary::Unstructured;
use strum::EnumIter;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::aggregate_fn::AggregateFn;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::EmptyOptions;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::all_nan::AllNan as AllNanFn;
use vortex_array::aggregate_fn::fns::all_non_distinct::AllNonDistinct as AllNonDistinctFn;
use vortex_array::aggregate_fn::fns::all_non_distinct::all_non_distinct;
use vortex_array::aggregate_fn::fns::all_non_nan::AllNonNan as AllNonNanFn;
use vortex_array::aggregate_fn::fns::all_non_null::AllNonNull as AllNonNullFn;
use vortex_array::aggregate_fn::fns::all_null::AllNull as AllNullFn;
use vortex_array::aggregate_fn::fns::bounded_max::BoundedMax as BoundedMaxFn;
use vortex_array::aggregate_fn::fns::bounded_max::BoundedMaxOptions;
use vortex_array::aggregate_fn::fns::bounded_min::BoundedMin as BoundedMinFn;
use vortex_array::aggregate_fn::fns::bounded_min::BoundedMinOptions;
use vortex_array::aggregate_fn::fns::count::Count as CountFn;
use vortex_array::aggregate_fn::fns::first::First as FirstFn;
use vortex_array::aggregate_fn::fns::is_constant::IsConstant as IsConstantFn;
use vortex_array::aggregate_fn::fns::is_sorted::IsSorted as IsSortedFn;
use vortex_array::aggregate_fn::fns::is_sorted::IsSortedOptions;
use vortex_array::aggregate_fn::fns::last::Last as LastFn;
use vortex_array::aggregate_fn::fns::max::Max as MaxFn;
use vortex_array::aggregate_fn::fns::min::Min as MinFn;
use vortex_array::aggregate_fn::fns::min_max::MinMax as MinMaxFn;
use vortex_array::aggregate_fn::fns::nan_count::NanCount as NanCountFn;
use vortex_array::aggregate_fn::fns::null_count::NullCount as NullCountFn;
use vortex_array::aggregate_fn::fns::sum::Sum as SumFn;
use vortex_array::aggregate_fn::fns::sum_v2::SumV2 as SumV2Fn;
use vortex_array::aggregate_fn::fns::uncompressed_size_in_bytes::UncompressedSizeInBytes as UncompressedSizeInBytesFn;
use vortex_array::aggregate_fn::fns::uncompressed_size_in_bytes::UncompressedSizeOpts;
use vortex_array::aggregate_fn::fns::uncompressed_size_in_bytes::UncompressedSizePrecision;
use vortex_array::dtype::DType;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::encodings::Case;
use super::encodings::is_leaf;
use super::encodings::operand;
use super::value::Value;
use super::value::from_scalar;

/// Every aggregate function the reference fuzzer exercises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum AggregateKind {
    AllNan,
    AllNonDistinct,
    AllNonNan,
    AllNonNull,
    AllNull,
    BoundedMax,
    BoundedMin,
    Count,
    First,
    IsConstant,
    IsSorted,
    Last,
    Max,
    Min,
    MinMax,
    NanCount,
    NullCount,
    Sum,
    SumV2,
    UncompressedSizeInBytes,
}

impl AggregateKind {
    /// The aggregate function ids this kind exercises.
    pub fn ids(self) -> &'static [&'static str] {
        use AggregateKind::*;
        match self {
            AllNan => &["vortex.all_nan"],
            AllNonDistinct => &["vortex.all_non_distinct"],
            AllNonNan => &["vortex.all_non_nan"],
            AllNonNull => &["vortex.all_non_null"],
            AllNull => &["vortex.all_null"],
            BoundedMax => &["vortex.bounded_max"],
            BoundedMin => &["vortex.bounded_min"],
            Count => &["vortex.count"],
            First => &["vortex.first"],
            IsConstant => &["vortex.is_constant"],
            IsSorted => &["vortex.is_sorted"],
            Last => &["vortex.last"],
            Max => &["vortex.max"],
            Min => &["vortex.min"],
            MinMax => &["vortex.min_max"],
            NanCount => &["vortex.nan_count"],
            NullCount => &["vortex.null_count"],
            Sum => &["vortex.sum"],
            SumV2 => &["vortex.sum_v2"],
            UncompressedSizeInBytes => &["vortex.uncompressed_size_in_bytes"],
        }
    }

    /// Generate an aggregate of this kind over `input`, or `IncorrectFormat` if the input's dtype
    /// is not supported.
    pub fn generate(
        self,
        u: &mut Unstructured<'_>,
        input: &Case,
        ctx: &mut ExecutionCtx,
    ) -> arbitrary::Result<AggregateOp> {
        use AggregateKind::*;
        let dtype = &input.dtype;
        let float = dtype.is_float();
        let ordered = is_leaf(dtype);
        let summable = matches!(dtype, DType::Bool(_) | DType::Primitive(..))
            || matches!(dtype, DType::Decimal(d, _) if d.precision() <= 28);
        let supported = match self {
            AllNan | AllNonNan | NanCount => float,
            AllNull | AllNonNull | NullCount | UncompressedSizeInBytes => true,
            AllNonDistinct => super::encodings::buildable(dtype),
            First | Last | IsConstant => ordered,
            IsSorted | Min | Max | MinMax | BoundedMin | BoundedMax => ordered,
            Count => true,
            Sum | SumV2 => summable,
        };
        if !supported {
            return Err(IncorrectFormat);
        }
        let other = if self == AllNonDistinct {
            Some(if u.arbitrary()? {
                // The same values in a canonical array, optionally with one value replaced.
                let mut values = input.values.clone();
                if !values.is_empty() && u.arbitrary()? {
                    let i = u.choose_index(values.len())?;
                    values[i] = if dtype.is_nullable() && u.arbitrary()? {
                        Value::Null
                    } else {
                        super::generate::gen_one(u, dtype, super::generate::Shape::Random, 0)?
                    };
                }
                let array =
                    super::value::to_array(dtype, &values, ctx).map_err(|_| IncorrectFormat)?;
                Case {
                    kind: super::encodings::EncodingKind::Primitive,
                    dtype: dtype.clone(),
                    values,
                    array,
                }
            } else {
                operand(u, dtype, input.values.len(), ctx)?
            })
        } else {
            None
        };
        let len = input.values.len();
        let op = AggregateOp {
            kind: self,
            skip_nans: u.arbitrary()?,
            strict: u.arbitrary()?,
            max_bytes: NonZeroUsize::new(u.int_in_range(1..=8)?).ok_or(IncorrectFormat)?,
            split: match u.int_in_range(0..=2)? {
                0 => Split::Whole,
                1 => Split::Batches(u.int_in_range(0..=len)?),
                _ => Split::Merged(u.int_in_range(0..=len)?),
            },
            other,
        };
        if op.other.is_none() && op.aggregate_fn().return_dtype(dtype).is_none() {
            return Err(IncorrectFormat);
        }
        Ok(op)
    }
}

/// How the input is fed to the accumulator.
#[derive(Debug, Clone, Copy)]
pub enum Split {
    /// One batch.
    Whole,
    /// Two batches into the same accumulator.
    Batches(usize),
    /// Two accumulators, one per batch, merged.
    Merged(usize),
}

/// A concrete aggregate over the current array.
#[derive(Debug, Clone)]
pub struct AggregateOp {
    pub kind: AggregateKind,
    pub skip_nans: bool,
    pub strict: bool,
    pub max_bytes: NonZeroUsize,
    pub split: Split,
    pub other: Option<Case>,
}

impl AggregateOp {
    fn aggregate_fn(&self) -> AggregateFnRef {
        use AggregateKind::*;
        let num = NumericalAggregateOpts {
            skip_nans: self.skip_nans,
        };
        match self.kind {
            AllNan => AggregateFn::new(AllNanFn, EmptyOptions).erased(),
            AllNonDistinct => AggregateFn::new(AllNonDistinctFn, EmptyOptions).erased(),
            AllNonNan => AggregateFn::new(AllNonNanFn, EmptyOptions).erased(),
            AllNonNull => AggregateFn::new(AllNonNullFn, EmptyOptions).erased(),
            AllNull => AggregateFn::new(AllNullFn, EmptyOptions).erased(),
            BoundedMax => AggregateFn::new(
                BoundedMaxFn,
                BoundedMaxOptions {
                    max_bytes: self.max_bytes,
                },
            )
            .erased(),
            BoundedMin => AggregateFn::new(
                BoundedMinFn,
                BoundedMinOptions {
                    max_bytes: self.max_bytes,
                },
            )
            .erased(),
            Count => AggregateFn::new(CountFn, num).erased(),
            First => AggregateFn::new(FirstFn, EmptyOptions).erased(),
            IsConstant => AggregateFn::new(IsConstantFn, EmptyOptions).erased(),
            IsSorted => AggregateFn::new(
                IsSortedFn,
                IsSortedOptions {
                    strict: self.strict,
                },
            )
            .erased(),
            Last => AggregateFn::new(LastFn, EmptyOptions).erased(),
            Max => AggregateFn::new(MaxFn, num).erased(),
            Min => AggregateFn::new(MinFn, num).erased(),
            MinMax => AggregateFn::new(MinMaxFn, num).erased(),
            NanCount => AggregateFn::new(NanCountFn, EmptyOptions).erased(),
            NullCount => AggregateFn::new(NullCountFn, EmptyOptions).erased(),
            Sum => AggregateFn::new(SumFn, num).erased(),
            SumV2 => AggregateFn::new(SumV2Fn, num).erased(),
            UncompressedSizeInBytes => AggregateFn::new(
                UncompressedSizeInBytesFn,
                UncompressedSizeOpts {
                    precision: UncompressedSizePrecision::Exact,
                },
            )
            .erased(),
        }
    }

    /// Run the aggregate through the generic accumulator.
    pub fn vortex(&self, input: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Value> {
        // The aggregate expects a `{lhs, rhs}` struct whose validity is the shared validity of
        // both sides; the public function builds exactly that.
        if let Some(other) = &self.other {
            return Ok(Value::Bool(all_non_distinct(input, &other.array, ctx)?));
        }
        let f = self.aggregate_fn();
        let input = input.clone();
        let mut acc = f.accumulator(input.dtype())?;
        match self.split {
            Split::Whole => acc.accumulate(&input, ctx)?,
            Split::Batches(k) => {
                acc.accumulate(&input.slice(0..k)?, ctx)?;
                acc.accumulate(&input.slice(k..input.len())?, ctx)?;
            }
            Split::Merged(k) => {
                let mut rest = f.accumulator(input.dtype())?;
                acc.accumulate(&input.slice(0..k)?, ctx)?;
                rest.accumulate(&input.slice(k..input.len())?, ctx)?;
                acc.merge_from(rest.as_mut())?;
            }
        }
        Ok(from_scalar(&acc.finish()?))
    }

    /// Check a Vortex result against the reference.
    pub fn check(
        &self,
        input: &[Value],
        dtype: &DType,
        actual: &Value,
        input_array: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        use AggregateKind::*;
        let non_null = || input.iter().filter(|v| !v.is_null());
        let is_nan = |v: &Value| v.as_float().is_some_and(f64::is_nan);
        let count = |n: usize| Value::Int(n as i128);
        let expected = match self.kind {
            AllNan => Value::Bool(input.iter().all(is_nan)),
            AllNonNan => Value::Bool(!input.iter().any(is_nan)),
            AllNull => Value::Bool(input.iter().all(Value::is_null)),
            AllNonNull => Value::Bool(!input.iter().any(Value::is_null)),
            AllNonDistinct => {
                let other = &self
                    .other
                    .as_ref()
                    .map(|o| o.values.clone())
                    .unwrap_or_default();
                Value::Bool(input.iter().zip(other).all(|(a, b)| a == b))
            }
            NullCount => count(input.iter().filter(|v| v.is_null()).count()),
            NanCount => count(input.iter().filter(|v| is_nan(v)).count()),
            Count => count(
                non_null()
                    .filter(|v| !(self.skip_nans && is_nan(v)))
                    .count(),
            ),
            First => non_null().next().cloned().unwrap_or(Value::Null),
            Last => input
                .iter()
                .rev()
                .find(|v| !v.is_null())
                .cloned()
                .unwrap_or(Value::Null),
            IsConstant => Value::Bool(match input.len() {
                0 => false,
                _ => input.iter().all(|v| v == &input[0]),
            }),
            IsSorted => Value::Bool(is_sorted(input, self.strict)),
            Min | Max | MinMax | BoundedMin | BoundedMax => {
                let candidates: Vec<&Value> = non_null()
                    .filter(|v| !(self.kind_skips_nans() && is_nan(v)))
                    .collect();
                let any_nan = non_null().any(is_nan);
                let (min, max) = if !self.kind_skips_nans() && any_nan {
                    (Value::Float(f64::NAN), Value::Float(f64::NAN))
                } else {
                    (
                        candidates
                            .iter()
                            .min_by(|a, b| a.total_cmp(b))
                            .map_or(Value::Null, |v| (*v).clone()),
                        candidates
                            .iter()
                            .max_by(|a, b| a.total_cmp(b))
                            .map_or(Value::Null, |v| (*v).clone()),
                    )
                };
                match self.kind {
                    Min => min,
                    Max => max,
                    MinMax => {
                        if min.is_null() {
                            Value::Null
                        } else {
                            Value::Tuple(vec![min, max])
                        }
                    }
                    BoundedMin => return check_bounded(&min, actual, self.max_bytes.get(), false),
                    _ => return check_bounded(&max, actual, self.max_bytes.get(), true),
                }
            }
            Sum | SumV2 => return check_sum(self, dtype, input, actual),
            UncompressedSizeInBytes => {
                // The size depends on the physical layout (and batches round their validity
                // bitmaps separately), so the only reference is the array's own canonical form:
                // encodings that answer from metadata must agree with it.
                if !matches!(self.split, Split::Whole) {
                    return Ok(());
                }
                let canonical = input_array.clone().execute::<Canonical>(ctx)?.into_array();
                self.vortex(&canonical, ctx)?
            }
        };
        if actual == &expected {
            Ok(())
        } else {
            Err(vortex_err!("expected {expected} got {actual}"))
        }
    }

    fn kind_skips_nans(&self) -> bool {
        // Bounded min/max always skip NaNs.
        self.skip_nans
            || matches!(
                self.kind,
                AggregateKind::BoundedMin | AggregateKind::BoundedMax
            )
    }
}

/// Nulls first, then non-decreasing (or increasing when strict) by the total order.
fn is_sorted(values: &[Value], strict: bool) -> bool {
    if values.len() <= 1 {
        return true;
    }
    values.windows(2).all(|w| {
        let ord = w[0].total_cmp(&w[1]);
        if strict { ord.is_lt() } else { ord.is_le() }
    })
}

/// A bounded min must be a prefix of the true min, and a bounded max must be at least the true
/// max; both fit in `max_bytes` and equal the true value when it fits.
fn check_bounded(
    exact: &Value,
    actual: &Value,
    max_bytes: usize,
    is_max: bool,
) -> VortexResult<()> {
    let ok = match (exact, actual) {
        (Value::Null, Value::Null) => true,
        (Value::Bytes(e), Value::Bytes(a)) => {
            a.len() <= max_bytes
                && if e.len() <= max_bytes {
                    a == e
                } else if is_max {
                    a.as_slice() > e.as_slice()
                } else {
                    e.starts_with(a)
                }
        }
        // No upper bound fits in `max_bytes` (e.g. all 0xFF bytes).
        (Value::Bytes(e), Value::Null) => is_max && e.len() > max_bytes,
        (e, a) => e == a,
    };
    if ok {
        Ok(())
    } else {
        Err(vortex_err!(
            "bounded {} of {exact} with {max_bytes} bytes is {actual}",
            if is_max { "max" } else { "min" }
        ))
    }
}

fn check_sum(op: &AggregateOp, dtype: &DType, input: &[Value], actual: &Value) -> VortexResult<()> {
    let valid: Vec<&Value> = input.iter().filter(|v| !v.is_null()).collect();
    let v2 = op.kind == AggregateKind::SumV2;
    if valid.is_empty() {
        let expected = if v2 {
            Value::Null
        } else if dtype.is_float() {
            Value::Float(0.0)
        } else if matches!(dtype, DType::Decimal(..)) {
            Value::Decimal(0)
        } else {
            Value::Int(0)
        };
        return if actual == &expected {
            Ok(())
        } else {
            Err(vortex_err!(
                "sum of no values: expected {expected} got {actual}"
            ))
        };
    }
    let expected = match dtype {
        DType::Bool(_) => {
            Value::Int(valid.iter().filter(|v| v.as_bool() == Some(true)).count() as i128)
        }
        DType::Primitive(p, _) if p.is_float() => {
            let floats: Vec<f64> = valid
                .iter()
                .filter_map(|v| v.as_float())
                .filter(|f| !(op.skip_nans && f.is_nan()))
                .collect();
            let exact: f64 = floats.iter().sum();
            let Value::Float(a) = actual else {
                return Err(vortex_err!(
                    "float sum: expected about {exact} got {actual}"
                ));
            };
            // Summation order differs between kernels, so only require a close result.
            let magnitude: f64 = floats.iter().map(|f| f.abs()).sum();
            let close = (exact.is_nan() && a.is_nan())
                || exact == *a
                || (exact - a).abs() <= magnitude * 1e-9;
            return if close || magnitude.is_infinite() {
                Ok(())
            } else {
                Err(vortex_err!("float sum: expected about {exact} got {a}"))
            };
        }
        DType::Primitive(p, _) => {
            let (lo, hi) = if p.is_unsigned_int() {
                (0, i128::from(u64::MAX))
            } else {
                (i128::from(i64::MIN), i128::from(i64::MAX))
            };
            let ints: Vec<i128> = valid.iter().filter_map(|v| v.as_int()).collect();
            let total: i128 = ints.iter().sum();
            // Overflow is detected while accumulating, so an intermediate sum that leaves the
            // range can make the result null even when the total fits. Some order overflows
            // exactly when the positive or the negative values alone leave the range.
            let positive: i128 = ints.iter().filter(|v| **v > 0).sum();
            let negative: i128 = ints.iter().filter(|v| **v < 0).sum();
            if actual.is_null() && (positive > hi || negative < lo) {
                return Ok(());
            }
            if (lo..=hi).contains(&total) {
                Value::Int(total)
            } else {
                Value::Null
            }
        }
        DType::Decimal(..) => Value::Decimal(valid.iter().filter_map(|v| v.as_int()).sum()),
        _ => return Err(vortex_err!("unsupported sum over {dtype}")),
    };
    if actual == &expected {
        Ok(())
    } else {
        Err(vortex_err!("sum: expected {expected} got {actual}"))
    }
}

#[cfg(test)]
mod tests {
    use arbitrary::Unstructured;
    use strum::IntoEnumIterator;
    use vortex_array::VortexSessionExecute;
    use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
    use vortex_utils::aliases::hash_set::HashSet;

    use super::*;
    use crate::SESSION;
    use crate::reference::FuzzArrayReference;
    use crate::reference::encodings::EncodingKind;
    use crate::reference::encodings::Limits;
    use crate::reference::ops::RefAction;
    use crate::reference::run_array_reference;
    use crate::reference::tests::panic_message;
    use crate::reference::tests::test_bytes;

    /// Every aggregate function registered in the fuzz session must have a reference.
    #[test]
    fn every_registered_aggregate_fn_has_a_reference() {
        let covered: HashSet<&str> = AggregateKind::iter()
            .flat_map(|k| k.ids().iter().copied())
            .collect();
        let registered: Vec<String> = SESSION
            .aggregate_fns()
            .registry()
            .read(|map| map.keys().map(ToString::to_string).collect());
        let missing: Vec<_> = registered
            .iter()
            .filter(|id| !covered.contains(id.as_str()))
            .collect();
        assert!(
            missing.is_empty(),
            "aggregate functions without a reference: {missing:?}"
        );
    }

    /// Every aggregate kind must be generated for some input and agree with its reference.
    #[test]
    fn every_aggregate_kind_is_exercised() {
        let mut ctx = SESSION.create_execution_ctx();
        let encodings = EncodingKind::iter().collect::<Vec<_>>();
        let mut failures = Vec::new();
        for kind in AggregateKind::iter() {
            let mut exercised = 0;
            for seed in 0..3000u64 {
                let data = test_bytes(seed);
                let mut u = Unstructured::new(&data);
                let Ok(encoding) = u.choose(&encodings).copied() else {
                    continue;
                };
                let Ok(case) = encoding.generate(&mut u, None, Limits::default(), &mut ctx) else {
                    continue;
                };
                let Ok(op) = kind.generate(&mut u, &case, &mut ctx) else {
                    continue;
                };
                let input = FuzzArrayReference {
                    case,
                    actions: vec![RefAction::Aggregate(op)],
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_array_reference(input).map_err(|e| e.to_string())
                }));
                match result {
                    Ok(Ok(_)) => exercised += 1,
                    Ok(Err(e)) => {
                        failures.push(format!("{kind:?} seed {seed}: {e}"));
                        break;
                    }
                    Err(e) => {
                        failures.push(format!(
                            "{kind:?} seed {seed}: panicked: {}",
                            panic_message(&e)
                        ));
                        break;
                    }
                }
                if exercised >= 20 {
                    break;
                }
            }
            if exercised == 0 {
                failures.push(format!("{kind:?} was never exercised"));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    }
}
