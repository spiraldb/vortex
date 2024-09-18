use vortex::array::{PrimitiveArray, TemporalArray};
use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::compute::{slice, take, ArrayCompute, SliceFn, TakeFn};
use vortex::validity::ArrayValidity;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_datetime_dtype::{TemporalMetadata, TimeUnit};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult, VortexUnwrap as _};
use vortex_scalar::Scalar;

use crate::DateTimePartsArray;

impl ArrayCompute for DateTimePartsArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl TakeFn for DateTimePartsArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Ok(Self::try_new(
            self.dtype().clone(),
            take(self.days(), indices)?,
            take(self.seconds(), indices)?,
            take(self.subsecond(), indices)?,
        )?
        .into_array())
    }
}

impl SliceFn for DateTimePartsArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            self.dtype().clone(),
            slice(self.days(), start, stop)?,
            slice(self.seconds(), start, stop)?,
            slice(self.subsecond(), start, stop)?,
        )?
        .into_array())
    }
}

impl ScalarAtFn for DateTimePartsArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let DType::Extension(ext, nullability) = self.dtype().clone() else {
            vortex_bail!(
                "DateTimePartsArray must have extension dtype, found {}",
                self.dtype()
            );
        };

        let TemporalMetadata::Timestamp(time_unit, _) = TemporalMetadata::try_from(&ext)? else {
            vortex_bail!("Metadata must be Timestamp, found {}", ext.id());
        };

        let divisor = match time_unit {
            TimeUnit::Ns => 1_000_000_000,
            TimeUnit::Us => 1_000_000,
            TimeUnit::Ms => 1_000,
            TimeUnit::S => 1,
            TimeUnit::D => vortex_bail!("Invalid time unit D"),
        };

        let days: i64 = scalar_at(&self.days(), index)?.try_into()?;
        let seconds: i64 = scalar_at(&self.seconds(), index)?.try_into()?;
        let subseconds: i64 = scalar_at(&self.subsecond(), index)?.try_into()?;

        let scalar = days * 86_400 * divisor + seconds * divisor + subseconds;

        Ok(Scalar::primitive(scalar, nullability))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).vortex_unwrap()
    }
}

/// Decode an [Array] into a [TemporalArray].
///
/// Enforces that the passed array is actually a [DateTimePartsArray] with proper metadata.
pub fn decode_to_temporal(array: &DateTimePartsArray) -> VortexResult<TemporalArray> {
    let DType::Extension(ext, _) = array.dtype().clone() else {
        vortex_bail!(ComputeError: "expected dtype to be DType::Extension variant")
    };

    let Ok(temporal_metadata) = TemporalMetadata::try_from(&ext) else {
        vortex_bail!(ComputeError: "must decode TemporalMetadata from extension metadata");
    };

    let divisor = match temporal_metadata.time_unit() {
        TimeUnit::Ns => 1_000_000_000,
        TimeUnit::Us => 1_000_000,
        TimeUnit::Ms => 1_000,
        TimeUnit::S => 1,
        TimeUnit::D => vortex_bail!(InvalidArgument: "cannot decode into TimeUnit::D"),
    };

    let days_buf = array.days().into_primitive()?;
    let seconds_buf = array.seconds().into_primitive()?;
    let subsecond_buf = array.subsecond().into_primitive()?;

    let values = days_buf
        .maybe_null_slice::<i64>()
        .iter()
        .zip(seconds_buf.maybe_null_slice::<i64>().iter())
        .zip(subsecond_buf.maybe_null_slice::<i64>().iter())
        .map(|((d, s), ss)| d * 86_400 * divisor + s * divisor + ss)
        .collect::<Vec<_>>();

    Ok(TemporalArray::new_timestamp(
        PrimitiveArray::from_vec(values, array.logical_validity().into_validity()).into_array(),
        temporal_metadata.time_unit(),
        temporal_metadata.time_zone().map(ToString::to_string),
    ))
}

#[cfg(test)]
mod test {
    use vortex::array::{PrimitiveArray, TemporalArray};
    use vortex::{IntoArray, IntoArrayVariant};
    use vortex_datetime_dtype::TimeUnit;
    use vortex_dtype::{DType, Nullability};

    use crate::compute::decode_to_temporal;
    use crate::{compress_temporal, DateTimePartsArray};

    #[test]
    fn test_roundtrip_datetimeparts() {
        let raw_values = vec![
            86_400i64,            // element with only day component
            86_400i64 + 1000,     // element with day + second components
            86_400i64 + 1000 + 1, // element with day + second + sub-second components
        ];

        let raw_millis = PrimitiveArray::from(raw_values.clone()).into_array();

        let temporal_array =
            TemporalArray::new_timestamp(raw_millis, TimeUnit::Ms, Some("UTC".to_string()));

        let (days, seconds, subseconds) = compress_temporal(temporal_array.clone()).unwrap();

        let date_times = DateTimePartsArray::try_new(
            DType::Extension(temporal_array.ext_dtype().clone(), Nullability::NonNullable),
            days,
            seconds,
            subseconds,
        )
        .unwrap();

        let primitive_values = decode_to_temporal(&date_times)
            .unwrap()
            .temporal_values()
            .into_primitive()
            .unwrap();

        assert_eq!(
            primitive_values.maybe_null_slice::<i64>(),
            raw_values.as_slice()
        );
    }
}
