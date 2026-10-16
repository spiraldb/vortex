// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Canonical Vortex → Arrow conversion for the temporal extension types.
//!
//! `Date`, `Time`, `Timestamp`, `Duration`, and `Interval` are Vortex builtin extension types that
//! map directly to native Arrow temporal types (`Date32`/`Date64`, `Time32`/`Time64`, `Timestamp`,
//! `Duration`, `Interval`). These conversions live in the canonical executor rather than in the
//! plugin layer because they aren't Arrow extensions and the mapping is fully determined by the
//! source `ExtDType`.

use std::sync::Arc;

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::IntervalDayTimeArray;
use arrow_array::IntervalMonthDayNanoArray;
use arrow_array::IntervalYearMonthArray;
use arrow_array::PrimitiveArray as ArrowPrimitiveArray;
use arrow_array::types::ArrowTemporalType;
use arrow_array::types::ArrowTimestampType;
use arrow_array::types::Date32Type;
use arrow_array::types::Date64Type;
use arrow_array::types::DurationMicrosecondType;
use arrow_array::types::DurationMillisecondType;
use arrow_array::types::DurationNanosecondType;
use arrow_array::types::DurationSecondType;
use arrow_array::types::Time32MillisecondType;
use arrow_array::types::Time32SecondType;
use arrow_array::types::Time64MicrosecondType;
use arrow_array::types::Time64NanosecondType;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::types::TimestampSecondType;
use arrow_buffer::IntervalDayTime;
use arrow_buffer::IntervalMonthDayNano;
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;
use arrow_schema::IntervalUnit;
use arrow_schema::TimeUnit as ArrowTimeUnit;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::NativePType;
use vortex_array::extension::datetime::AnyTemporal;
use vortex_array::extension::datetime::Interval;
use vortex_array::extension::datetime::TemporalMetadata;
use vortex_array::extension::datetime::TimeUnit;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;

use crate::null_buffer::to_null_buffer;

pub(super) fn to_arrow_date(
    array: ArrayRef,
    target: &DataType,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    validate_temporal_extension(&array, target)?;
    Ok(match target {
        DataType::Date32 => Arc::new(to_temporal_primitive::<Date32Type>(array, ctx)?),
        DataType::Date64 => Arc::new(to_temporal_primitive::<Date64Type>(array, ctx)?),
        _ => unreachable!("to_arrow_date called with non-Date type {target}"),
    })
}

pub(super) fn to_arrow_time(
    array: ArrayRef,
    target: &DataType,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    validate_temporal_extension(&array, target)?;
    Ok(match target {
        DataType::Time32(ArrowTimeUnit::Second) => {
            Arc::new(to_temporal_primitive::<Time32SecondType>(array, ctx)?)
        }
        DataType::Time32(ArrowTimeUnit::Millisecond) => {
            Arc::new(to_temporal_primitive::<Time32MillisecondType>(array, ctx)?)
        }
        DataType::Time64(ArrowTimeUnit::Microsecond) => {
            Arc::new(to_temporal_primitive::<Time64MicrosecondType>(array, ctx)?)
        }
        DataType::Time64(ArrowTimeUnit::Nanosecond) => {
            Arc::new(to_temporal_primitive::<Time64NanosecondType>(array, ctx)?)
        }
        _ => unreachable!("to_arrow_time called with non-Time type {target}"),
    })
}

pub(super) fn to_arrow_timestamp(
    array: ArrayRef,
    target: &DataType,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    validate_temporal_extension(&array, target)?;
    let DataType::Timestamp(unit, tz) = target else {
        unreachable!("to_arrow_timestamp called with non-Timestamp type {target}");
    };
    Ok(match unit {
        ArrowTimeUnit::Second => with_timezone::<TimestampSecondType>(array, tz.as_ref(), ctx)?,
        ArrowTimeUnit::Millisecond => {
            with_timezone::<TimestampMillisecondType>(array, tz.as_ref(), ctx)?
        }
        ArrowTimeUnit::Microsecond => {
            with_timezone::<TimestampMicrosecondType>(array, tz.as_ref(), ctx)?
        }
        ArrowTimeUnit::Nanosecond => {
            with_timezone::<TimestampNanosecondType>(array, tz.as_ref(), ctx)?
        }
    })
}

pub(super) fn to_arrow_duration(
    array: ArrayRef,
    target: &DataType,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    validate_temporal_extension(&array, target)?;
    let DataType::Duration(unit) = target else {
        unreachable!("to_arrow_duration called with non-Duration type {target}");
    };
    Ok(match unit {
        ArrowTimeUnit::Second => Arc::new(to_temporal_primitive::<DurationSecondType>(array, ctx)?),
        ArrowTimeUnit::Millisecond => Arc::new(to_temporal_primitive::<DurationMillisecondType>(
            array, ctx,
        )?),
        ArrowTimeUnit::Microsecond => Arc::new(to_temporal_primitive::<DurationMicrosecondType>(
            array, ctx,
        )?),
        ArrowTimeUnit::Nanosecond => {
            Arc::new(to_temporal_primitive::<DurationNanosecondType>(array, ctx)?)
        }
    })
}

/// Export an interval array as the requested Arrow interval type.
///
/// `MonthDayNano` holds every Vortex interval. The narrower `YearMonth` and `DayTime` types are
/// produced only when no valid value would lose a part or sub-millisecond precision.
pub(super) fn to_arrow_interval(
    array: ArrayRef,
    target: &DataType,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    validate_temporal_extension(&array, target)?;
    let DataType::Interval(unit) = target else {
        unreachable!("to_arrow_interval called with non-Interval type {target}");
    };

    let len = array.len();
    let storage = array
        .cast(Interval::storage_dtype(array.dtype().nullability()))?
        .execute::<StructArray>(ctx)?;
    let validity = storage
        .as_ref()
        .validity()?
        .execute_mask(storage.as_ref().len(), ctx)?;
    let mut field = |idx: usize| {
        storage
            .unmasked_field(idx)
            .clone()
            .execute::<PrimitiveArray>(ctx)
    };
    let months = field(0)?.into_buffer::<i32>();
    let days = field(1)?.into_buffer::<i32>();
    let nanoseconds = field(2)?.into_buffer::<i64>();

    Ok(match unit {
        IntervalUnit::MonthDayNano => {
            let values = months
                .iter()
                .zip(days.iter())
                .zip(nanoseconds.iter())
                .map(|((&months, &days), &nanoseconds)| {
                    IntervalMonthDayNano::new(months, days, nanoseconds)
                })
                .collect::<ScalarBuffer<_>>();
            Arc::new(IntervalMonthDayNanoArray::new(
                values,
                to_null_buffer(validity),
            ))
        }
        IntervalUnit::YearMonth => {
            vortex_ensure!(
                (0..len).all(|i| !validity.value(i) || (days[i] == 0 && nanoseconds[i] == 0)),
                "Cannot convert Interval to Arrow {target}: a value has days or nanoseconds"
            );
            Arc::new(IntervalYearMonthArray::new(
                months.into_arrow_scalar_buffer(),
                to_null_buffer(validity),
            ))
        }
        IntervalUnit::DayTime => {
            let values = (0..len)
                .map(|i| {
                    if !validity.value(i) {
                        return Ok(IntervalDayTime::ZERO);
                    }
                    vortex_ensure!(
                        months[i] == 0 && nanoseconds[i] % 1_000_000 == 0,
                        "Cannot convert Interval to Arrow {target}: a value has months or \
                         sub-millisecond nanoseconds"
                    );
                    let milliseconds = i32::try_from(nanoseconds[i] / 1_000_000).map_err(|_| {
                        vortex_err!(
                            "Cannot convert Interval to Arrow {target}: milliseconds overflow"
                        )
                    })?;
                    Ok(IntervalDayTime::new(days[i], milliseconds))
                })
                .collect::<VortexResult<ScalarBuffer<_>>>()?;
            Arc::new(IntervalDayTimeArray::new(values, to_null_buffer(validity)))
        }
    })
}

fn with_timezone<T>(
    array: ArrayRef,
    tz: Option<&Arc<str>>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef>
where
    T: ArrowTimestampType,
    T::Native: NativePType,
{
    Ok(Arc::new(
        to_temporal_primitive::<T>(array, ctx)?.with_timezone_opt(tz.cloned()),
    ))
}

fn to_temporal_primitive<T>(
    array: ArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowPrimitiveArray<T>>
where
    T: ArrowTemporalType,
    T::Native: NativePType,
{
    let dtype = DType::Primitive(T::Native::PTYPE, array.dtype().nullability());
    let array = array.cast(dtype)?;
    let primitive = array.execute::<PrimitiveArray>(ctx)?;
    let validity = primitive
        .as_ref()
        .validity()?
        .execute_mask(primitive.as_ref().len(), ctx)?;
    let buffer = primitive.into_buffer::<T::Native>();
    Ok(ArrowPrimitiveArray::<T>::new(
        buffer.into_arrow_scalar_buffer(),
        to_null_buffer(validity),
    ))
}

/// Verify that the source array is compatible with the target Arrow temporal type.
///
/// Vortex does not silently reinterpret across mismatched temporal units or timezones, so a
/// source with temporal extension metadata must agree exactly with `target`. Non-extension
/// sources are passed through; the cast in `to_temporal_primitive` will surface any width
/// mismatch.
fn validate_temporal_extension(array: &ArrayRef, target: &DataType) -> VortexResult<()> {
    let Some(ext) = array.dtype().as_extension_opt() else {
        return Ok(());
    };
    let Some(temporal) = ext.metadata_opt::<AnyTemporal>() else {
        vortex_bail!(
            "Cannot convert extension {} to Arrow type {target}",
            ext.id()
        );
    };
    match (temporal, target) {
        (TemporalMetadata::Date(TimeUnit::Days), DataType::Date32) => Ok(()),
        (TemporalMetadata::Date(TimeUnit::Milliseconds), DataType::Date64) => Ok(()),
        (
            TemporalMetadata::Time(unit),
            DataType::Time32(arrow_unit) | DataType::Time64(arrow_unit),
        ) if matches!(
            (unit, arrow_unit),
            (TimeUnit::Seconds, ArrowTimeUnit::Second)
                | (TimeUnit::Milliseconds, ArrowTimeUnit::Millisecond)
                | (TimeUnit::Microseconds, ArrowTimeUnit::Microsecond)
                | (TimeUnit::Nanoseconds, ArrowTimeUnit::Nanosecond)
        ) =>
        {
            Ok(())
        }
        (TemporalMetadata::Timestamp(unit, src_tz), DataType::Timestamp(arrow_unit, tgt_tz)) => {
            let src_arrow_unit = crate::dtype::to_arrow_time_unit(*unit)?;
            if src_arrow_unit != *arrow_unit {
                vortex_bail!(
                    "Cannot convert Timestamp({unit}) to Arrow Timestamp({arrow_unit:?}): unit mismatch"
                );
            }
            if src_tz != tgt_tz {
                vortex_bail!(
                    "Cannot convert Timestamp(tz={src_tz:?}) to Arrow Timestamp(tz={tgt_tz:?}): timezone mismatch"
                );
            }
            Ok(())
        }
        (TemporalMetadata::Interval, DataType::Interval(_)) => Ok(()),
        (TemporalMetadata::Duration(unit), DataType::Duration(arrow_unit)) => {
            let src_arrow_unit = crate::dtype::to_arrow_time_unit(*unit)?;
            if src_arrow_unit != *arrow_unit {
                vortex_bail!(
                    "Cannot convert Duration({unit}) to Arrow Duration({arrow_unit:?}): unit mismatch"
                );
            }
            Ok(())
        }
        (temporal, target) => vortex_bail!(
            "Cannot convert {} to Arrow type {target}",
            match temporal {
                TemporalMetadata::Date(unit) => format!("Date({unit})"),
                TemporalMetadata::Time(unit) => format!("Time({unit})"),
                TemporalMetadata::Timestamp(unit, tz) => format!("Timestamp({unit}, tz={tz:?})"),
                TemporalMetadata::Duration(unit) => format!("Duration({unit})"),
                TemporalMetadata::Interval => "Interval".to_string(),
            }
        ),
    }
}
