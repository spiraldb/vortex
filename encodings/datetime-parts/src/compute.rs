use vortex::array::datetime::{try_parse_time_unit, LocalDateTimeArray, TimeUnit};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::validity::ArrayValidity;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};
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
            take(&self.days(), indices)?,
            take(&self.seconds(), indices)?,
            take(&self.subsecond(), indices)?,
        )?
        .into_array())
    }
}

impl SliceFn for DateTimePartsArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            self.dtype().clone(),
            slice(&self.days(), start, stop)?,
            slice(&self.seconds(), start, stop)?,
            slice(&self.subsecond(), start, stop)?,
        )?
        .into_array())
    }
}

impl ScalarAtFn for DateTimePartsArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let DType::Extension(ext, nullability) = self.dtype().clone() else {
            panic!("DateTimePartsArray must have extension dtype");
        };

        match ext.id().as_ref() {
            LocalDateTimeArray::ID => {
                let time_unit = try_parse_time_unit(&ext)?;
                let divisor = match time_unit {
                    TimeUnit::Ns => 1_000_000_000,
                    TimeUnit::Us => 1_000_000,
                    TimeUnit::Ms => 1_000,
                    TimeUnit::S => 1,
                };

                let days: i64 = scalar_at(&self.days(), index)?.try_into()?;
                let seconds: i64 = scalar_at(&self.seconds(), index)?.try_into()?;
                let subseconds: i64 = scalar_at(&self.subsecond(), index)?.try_into()?;

                let scalar = days * 86_400 * divisor + seconds * divisor + subseconds;

                Ok(Scalar::primitive(scalar, nullability))
            }
            _ => {
                vortex_bail!(MismatchedTypes: LocalDateTimeArray::ID.to_string(), ext.id().as_ref().to_string())
            }
        }
    }
}

/// Decode an [Array] to a [LocalDateTimeArray].
///
/// Enforces that the passed array is actually a [DateTimePartsArray] with proper metadata.
pub fn decode_to_localdatetime(array: &Array) -> VortexResult<LocalDateTimeArray> {
    // Ensure we can process it
    let array = DateTimePartsArray::try_from(array)?;

    let DType::Extension(ext, _) = array.dtype().clone() else {
        vortex_bail!(ComputeError: "expected dtype to be DType::Extension variant")
    };

    if ext.id().as_ref() != LocalDateTimeArray::ID {
        vortex_bail!(ComputeError: "DateTimeParts extension type must be vortex.localdatetime")
    }

    let time_unit = try_parse_time_unit(&ext)?;
    let divisor = match time_unit {
        TimeUnit::Ns => 1_000_000_000,
        TimeUnit::Us => 1_000_000,
        TimeUnit::Ms => 1_000,
        TimeUnit::S => 1,
    };

    let days_buf = array
        .days()
        .flatten()?
        .into_array()
        .as_primitive()
        .scalar_buffer::<i64>();
    let seconds_buf = array
        .seconds()
        .flatten()?
        .into_array()
        .as_primitive()
        .scalar_buffer::<i64>();
    let subsecond_buf = array
        .subsecond()
        .flatten()?
        .into_array()
        .as_primitive()
        .scalar_buffer::<i64>();

    // TODO(aduffy): replace with vectorized implementation?
    let values = days_buf
        .iter()
        .zip(seconds_buf.iter())
        .zip(subsecond_buf.iter())
        .map(|((d, s), ss)| d * 86_400 * divisor + s * divisor + ss)
        .collect::<Vec<_>>();

    LocalDateTimeArray::try_new(
        time_unit,
        PrimitiveArray::from_vec(values, array.logical_validity().into_validity()).into_array(),
    )
}

#[cfg(test)]
mod test {
    use vortex::array::datetime::{LocalDateTimeArray, TimeUnit};
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::validity::Validity;
    use vortex::IntoArray;
    use vortex_dtype::{DType, ExtDType, ExtID, Nullability};

    use crate::compute::decode_to_localdatetime;
    use crate::DateTimePartsArray;

    #[test]
    fn test_decode_to_localdatetime() {
        let nanos = TimeUnit::Ns;

        let days = PrimitiveArray::from_vec(vec![2i64, 3], Validity::NonNullable).into_array();
        let seconds = PrimitiveArray::from_vec(vec![2i64, 3], Validity::NonNullable).into_array();
        let subsecond = PrimitiveArray::from_vec(vec![2i64, 3], Validity::NonNullable).into_array();

        let date_times = DateTimePartsArray::try_new(
            DType::Extension(
                ExtDType::new(
                    ExtID::from(LocalDateTimeArray::ID),
                    Some(nanos.metadata().clone()),
                ),
                Nullability::NonNullable,
            ),
            days,
            seconds,
            subsecond,
        )
        .unwrap();

        let local = decode_to_localdatetime(&date_times.into_array()).unwrap();

        let elem0: i64 = scalar_at(&local.timestamps(), 0)
            .unwrap()
            .try_into()
            .unwrap();
        let elem1: i64 = scalar_at(&local.timestamps(), 1)
            .unwrap()
            .try_into()
            .unwrap();

        assert_eq!(
            elem0,
            vec![(2i64 * 86_400 * 1_000_000_000), 2i64 * 1_000_000_000, 2i64]
                .into_iter()
                .sum::<i64>(),
        );
        assert_eq!(
            elem1,
            vec![(3i64 * 86_400 * 1_000_000_000), 3i64 * 1_000_000_000, 3i64]
                .into_iter()
                .sum::<i64>(),
        );
    }
}
