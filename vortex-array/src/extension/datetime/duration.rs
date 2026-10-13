// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use jiff::SignedDuration;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_session::registry::CachedId;

use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::PType;
use crate::dtype::extension::ExtDType;
use crate::dtype::extension::ExtId;
use crate::dtype::extension::ExtVTable;
use crate::extension::datetime::TimeUnit;
use crate::scalar::ScalarValue;

/// Duration DType: a signed length of time, stored as an `i64` count of a [`TimeUnit`].
///
/// This is compatible with the Arrow `Duration` type. [`TimeUnit::Days`] is not supported.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Duration;

impl Duration {
    /// Creates a new Duration extension dtype with the given time unit and nullability.
    pub fn try_new(time_unit: TimeUnit, nullability: Nullability) -> VortexResult<ExtDType<Self>> {
        ExtDType::try_new(time_unit, DType::Primitive(PType::I64, nullability))
    }

    /// Creates a new Duration extension dtype with the given time unit and nullability.
    ///
    /// # Panics
    ///
    /// Panics if the `time_unit` is [`TimeUnit::Days`].
    pub fn new(time_unit: TimeUnit, nullability: Nullability) -> ExtDType<Self> {
        Self::try_new(time_unit, nullability).vortex_expect("failed to create duration dtype")
    }
}

impl ExtVTable for Duration {
    type Metadata = TimeUnit;

    type NativeValue<'a> = SignedDuration;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("vortex.duration");
        *ID
    }

    fn serialize_metadata(&self, metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(vec![u8::from(*metadata)])
    }

    fn deserialize_metadata(&self, metadata: &[u8]) -> VortexResult<Self::Metadata> {
        vortex_ensure_eq!(
            metadata.len(),
            1,
            "Duration metadata must be exactly one byte"
        );
        TimeUnit::try_from(metadata[0])
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        let unit = ext_dtype.metadata();
        vortex_ensure!(
            *unit != TimeUnit::Days,
            "Duration type does not support time unit {unit}"
        );
        vortex_ensure!(
            matches!(ext_dtype.storage_dtype(), DType::Primitive(PType::I64, _)),
            "Duration storage dtype must be i64"
        );
        Ok(())
    }

    fn unpack_native<'a>(
        ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        let value = storage_value.as_primitive().cast::<i64>()?;
        Ok(match ext_dtype.metadata() {
            TimeUnit::Nanoseconds => SignedDuration::from_nanos(value),
            TimeUnit::Microseconds => SignedDuration::from_micros(value),
            TimeUnit::Milliseconds => SignedDuration::from_millis(value),
            TimeUnit::Seconds => SignedDuration::from_secs(value),
            d @ TimeUnit::Days => vortex_bail!("Duration type does not support time unit {d}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_error::VortexResult;

    use crate::dtype::DType;
    use crate::dtype::Nullability::Nullable;
    use crate::dtype::PType;
    use crate::dtype::extension::ExtDType;
    use crate::dtype::extension::ExtVTable;
    use crate::extension::datetime::Duration;
    use crate::extension::datetime::TimeUnit;
    use crate::scalar::PValue;
    use crate::scalar::Scalar;
    use crate::scalar::ScalarValue;

    #[rstest]
    #[case(TimeUnit::Seconds, 3661, "PT1H1M1S")]
    #[case(TimeUnit::Milliseconds, -1_500, "-PT1.5S")]
    #[case(TimeUnit::Microseconds, 90_000_000, "PT1M30S")]
    #[case(TimeUnit::Nanoseconds, 1, "PT0.000000001S")]
    #[case(TimeUnit::Seconds, i64::MAX, "PT2562047788015215H30M7S")]
    fn display_duration_scalar(
        #[case] unit: TimeUnit,
        #[case] value: i64,
        #[case] expected: &str,
    ) -> VortexResult<()> {
        let dtype = DType::Extension(Duration::new(unit, Nullable).erased());
        let scalar = Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(value))))?;
        assert_eq!(format!("{}", scalar.as_extension()), expected);
        Ok(())
    }

    #[test]
    fn reject_days_unit() {
        assert!(Duration::try_new(TimeUnit::Days, Nullable).is_err());
    }

    #[test]
    fn reject_non_i64_storage() {
        let result = ExtDType::<Duration>::try_new(
            TimeUnit::Seconds,
            DType::Primitive(PType::I32, Nullable),
        );
        assert!(result.is_err());
    }

    #[rstest]
    #[case(TimeUnit::Nanoseconds)]
    #[case(TimeUnit::Microseconds)]
    #[case(TimeUnit::Milliseconds)]
    #[case(TimeUnit::Seconds)]
    fn metadata_roundtrip(#[case] unit: TimeUnit) -> VortexResult<()> {
        let bytes = Duration.serialize_metadata(&unit)?;
        assert_eq!(Duration.deserialize_metadata(&bytes)?, unit);
        Ok(())
    }

    #[rstest]
    #[case::empty(&[])]
    #[case::invalid_tag(&[0xFF])]
    #[case::trailing_bytes(&[0x00, 0x00])]
    fn reject_invalid_metadata(#[case] bytes: &[u8]) {
        assert!(Duration.deserialize_metadata(bytes).is_err());
    }
}
