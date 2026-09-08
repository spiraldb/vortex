// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::TemporalArray;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::PType;
use vortex_array::extension::datetime::TimeUnit;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use crate::timestamp;
pub struct TemporalParts {
    pub days: ArrayRef,
    pub seconds: ArrayRef,
    pub subseconds: ArrayRef,
}

/// Compress a `TemporalArray` into day, second, and subseconds components.
///
/// Splitting the components by granularity creates more small values, which enables better
/// cascading compression.
pub fn split_temporal(array: TemporalArray, ctx: &mut ExecutionCtx) -> VortexResult<TemporalParts> {
    let temporal_values = array
        .temporal_values()
        .clone()
        .execute::<PrimitiveArray>(ctx)?;

    // After this operation, timestamps will be a PrimitiveArray<i64>
    let timestamps = temporal_values
        .clone()
        .into_array()
        .cast(DType::Primitive(
            PType::I64,
            temporal_values.dtype().nullability(),
        ))?
        .execute::<PrimitiveArray>(ctx)?;

    let (days, seconds, subseconds) = match array.temporal_metadata().time_unit() {
        TimeUnit::Nanoseconds => split_slice::<1_000_000_000>(timestamps.as_slice::<i64>()),
        TimeUnit::Microseconds => split_slice::<1_000_000>(timestamps.as_slice::<i64>()),
        TimeUnit::Milliseconds => split_slice::<1_000>(timestamps.as_slice::<i64>()),
        TimeUnit::Seconds => split_slice::<1>(timestamps.as_slice::<i64>()),
        TimeUnit::Days => vortex_bail!("Cannot handle day-level data"),
    };

    Ok(TemporalParts {
        days: PrimitiveArray::new(days.freeze(), temporal_values.validity()?).into_array(),
        seconds: seconds.into_array(),
        subseconds: subseconds.into_array(),
    })
}

fn split_slice<const DIVISOR: i64>(
    timestamps: &[i64],
) -> (BufferMut<i64>, BufferMut<i64>, BufferMut<i64>) {
    let mut days = BufferMut::zeroed(timestamps.len());
    let mut seconds = BufferMut::zeroed(timestamps.len());
    let mut subseconds = BufferMut::zeroed(timestamps.len());
    let (days_out, seconds_out, subseconds_out) = (
        days.as_mut_slice(),
        seconds.as_mut_slice(),
        subseconds.as_mut_slice(),
    );
    for (i, &ts) in timestamps.iter().enumerate() {
        let ts_parts = timestamp::split_with_divisor::<DIVISOR>(ts);
        days_out[i] = ts_parts.days;
        seconds_out[i] = ts_parts.seconds;
        subseconds_out[i] = ts_parts.subseconds;
    }
    (days, seconds, subseconds)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::TemporalArray;
    use vortex_array::extension::datetime::TimeUnit;
    use vortex_array::validity::Validity;
    use vortex_buffer::buffer;
    use vortex_error::VortexExpect;

    use crate::TemporalParts;
    use crate::split_temporal;

    #[rstest]
    #[case(Validity::NonNullable)]
    #[case(Validity::AllValid)]
    #[case(Validity::AllInvalid)]
    #[case(Validity::from_iter([true, false, true]))]
    fn test_split_temporal(#[case] validity: Validity) {
        let mut ctx = array_session().create_execution_ctx();
        let milliseconds = PrimitiveArray::new(
            buffer![
                86_400i64,            // element with only day component
                86_400i64 + 1000,     // element with day + second components
                86_400i64 + 1000 + 1, // element with day + second + sub-second components
            ],
            validity.clone(),
        )
        .into_array();
        let temporal_array =
            TemporalArray::new_timestamp(milliseconds, TimeUnit::Milliseconds, Some("UTC".into()));
        let TemporalParts {
            days,
            seconds,
            subseconds,
        } = split_temporal(temporal_array, &mut ctx).unwrap();

        let days_prim = days.execute::<PrimitiveArray>(&mut ctx).unwrap();
        assert!(
            days_prim
                .validity()
                .vortex_expect("days validity should be derivable")
                .mask_eq(&validity, days_prim.len(), &mut ctx)
                .unwrap()
        );
        let seconds_prim = seconds.execute::<PrimitiveArray>(&mut ctx).unwrap();
        assert!(matches!(
            seconds_prim
                .validity()
                .vortex_expect("seconds validity should be derivable"),
            Validity::NonNullable
        ));
        let subseconds_prim = subseconds.execute::<PrimitiveArray>(&mut ctx).unwrap();
        assert!(matches!(
            subseconds_prim
                .validity()
                .vortex_expect("subseconds validity should be derivable"),
            Validity::NonNullable
        ));
    }
}
