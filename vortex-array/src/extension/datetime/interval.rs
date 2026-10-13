// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_session::registry::CachedId;

use crate::EmptyMetadata;
use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::PType;
use crate::dtype::StructFields;
use crate::dtype::extension::ExtDType;
use crate::dtype::extension::ExtId;
use crate::dtype::extension::ExtVTable;
use crate::scalar::Scalar;
use crate::scalar::ScalarValue;

/// Interval DType: a calendar interval of months, days, and nanoseconds.
///
/// The three parts are kept separate because the length of a month or a day depends on the date the
/// interval is applied to. This is compatible with the Arrow `Interval(MonthDayNano)` type, and the
/// Arrow `YearMonth` and `DayTime` intervals convert into it without loss.
///
/// The storage is a struct of non-nullable `months: i32`, `days: i32`, and `nanoseconds: i64`
/// fields. Intervals compare field by field in that order, as Arrow orders them, so `1 month` is
/// greater than `100 days` and `1 month` does not equal `30 days`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Interval;

impl Interval {
    /// The names of the storage struct fields, in order.
    pub const FIELD_NAMES: [&'static str; 3] = ["months", "days", "nanoseconds"];

    /// Creates a new Interval extension dtype with the given nullability.
    pub fn new(nullability: Nullability) -> ExtDType<Self> {
        ExtDType::try_new(EmptyMetadata, Self::storage_dtype(nullability))
            .vortex_expect("failed to create interval dtype")
    }

    /// The storage dtype of an Interval with the given nullability.
    pub fn storage_dtype(nullability: Nullability) -> DType {
        DType::Struct(
            StructFields::new(
                Self::FIELD_NAMES.into(),
                vec![
                    DType::Primitive(PType::I32, Nullability::NonNullable),
                    DType::Primitive(PType::I32, Nullability::NonNullable),
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                ],
            ),
            nullability,
        )
    }
}

/// Unpacked value of an [`Interval`] extension scalar.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct IntervalValue {
    /// Number of months.
    pub months: i32,
    /// Number of days.
    pub days: i32,
    /// Number of nanoseconds.
    pub nanoseconds: i64,
}

impl IntervalValue {
    /// Creates an interval from its months, days, and nanoseconds.
    pub const fn new(months: i32, days: i32, nanoseconds: i64) -> Self {
        Self {
            months,
            days,
            nanoseconds,
        }
    }

    /// Reads an interval from the storage value of an [`Interval`] scalar.
    pub fn try_from_storage(storage_value: &ScalarValue) -> VortexResult<Self> {
        let ScalarValue::Tuple(parts) = storage_value else {
            vortex_bail!("Interval storage value must be a struct, got {storage_value}");
        };
        let [months, days, nanoseconds] = parts.as_slice() else {
            vortex_bail!(
                "Interval storage value must have 3 fields, got {}",
                parts.len()
            );
        };
        let part = |value: &Option<ScalarValue>| {
            value
                .as_ref()
                .map(|value| *value.as_primitive())
                .ok_or_else(|| vortex_err!("Interval storage fields must not be null"))
        };
        Ok(Self {
            months: part(months)?.cast::<i32>()?,
            days: part(days)?.cast::<i32>()?,
            nanoseconds: part(nanoseconds)?.cast::<i64>()?,
        })
    }

    /// Creates an [`Interval`] scalar holding this value.
    pub fn into_scalar(self, nullability: Nullability) -> Scalar {
        Scalar::extension::<Interval>(
            EmptyMetadata,
            Scalar::struct_(
                Interval::storage_dtype(nullability),
                [
                    Scalar::from(self.months),
                    Scalar::from(self.days),
                    Scalar::from(self.nanoseconds),
                ],
            ),
        )
    }
}

impl fmt::Display for IntervalValue {
    /// Writes an ISO 8601 duration with a sign on each part, like the PostgreSQL `iso_8601`
    /// interval style, because the parts can have different signs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("P")?;
        if self.months != 0 {
            write!(f, "{}M", self.months)?;
        }
        if self.days != 0 {
            write!(f, "{}D", self.days)?;
        }
        if self.nanoseconds == 0 {
            if self.months == 0 && self.days == 0 {
                f.write_str("T0S")?;
            }
            return Ok(());
        }

        let sign = if self.nanoseconds < 0 { "-" } else { "" };
        let nanoseconds = self.nanoseconds.unsigned_abs();
        let seconds = nanoseconds / 1_000_000_000;
        let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
        let mut fraction = nanoseconds % 1_000_000_000;

        f.write_str("T")?;
        if hours != 0 {
            write!(f, "{sign}{hours}H")?;
        }
        if minutes != 0 {
            write!(f, "{sign}{minutes}M")?;
        }
        if seconds != 0 || fraction != 0 {
            write!(f, "{sign}{seconds}")?;
            if fraction != 0 {
                let mut width = 9;
                while fraction.is_multiple_of(10) {
                    fraction /= 10;
                    width -= 1;
                }
                write!(f, ".{fraction:0width$}")?;
            }
            f.write_str("S")?;
        }
        Ok(())
    }
}

impl ExtVTable for Interval {
    type Metadata = EmptyMetadata;

    type NativeValue<'a> = IntervalValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("vortex.interval");
        *ID
    }

    fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(vec![])
    }

    fn deserialize_metadata(&self, metadata: &[u8]) -> VortexResult<Self::Metadata> {
        vortex_ensure!(metadata.is_empty(), "Interval metadata must be empty");
        Ok(EmptyMetadata)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        let storage = ext_dtype.storage_dtype();
        let expected = Self::storage_dtype(storage.nullability());
        vortex_ensure!(
            storage == &expected,
            "Interval storage dtype must be {expected}, got {storage}"
        );
        Ok(())
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        IntervalValue::try_from_storage(storage_value)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_error::VortexResult;

    use crate::EmptyMetadata;
    use crate::dtype::DType;
    use crate::dtype::Nullability::NonNullable;
    use crate::dtype::Nullability::Nullable;
    use crate::dtype::PType;
    use crate::dtype::StructFields;
    use crate::dtype::extension::ExtDType;
    use crate::dtype::extension::ExtVTable;
    use crate::extension::datetime::AnyTemporal;
    use crate::extension::datetime::Interval;
    use crate::extension::datetime::IntervalValue;
    use crate::extension::datetime::TemporalMetadata;

    #[rstest]
    #[case(IntervalValue::new(0, 0, 0), "PT0S")]
    #[case(IntervalValue::new(14, 0, 0), "P14M")]
    #[case(IntervalValue::new(0, -3, 0), "P-3D")]
    #[case(IntervalValue::new(1, 2, 3_661_000_000_000), "P1M2DT1H1M1S")]
    #[case(IntervalValue::new(0, 0, -1_500_000_000), "PT-1.5S")]
    #[case(IntervalValue::new(1, 0, 1), "P1MT0.000000001S")]
    #[case(IntervalValue::new(-1, 15, -3_600_000_000_000), "P-1M15DT-1H")]
    fn display_interval(#[case] value: IntervalValue, #[case] expected: &str) {
        assert_eq!(value.to_string(), expected);
    }

    #[test]
    fn scalar_roundtrip() -> VortexResult<()> {
        let value = IntervalValue::new(-1, 15, i64::MAX);
        let scalar = value.into_scalar(Nullable);
        let storage = scalar.as_extension().to_storage_scalar();
        let storage_value = storage.value().expect("interval scalar is not null");
        assert_eq!(IntervalValue::try_from_storage(storage_value)?, value);
        assert_eq!(format!("{}", scalar.as_extension()), value.to_string());
        Ok(())
    }

    #[test]
    fn matches_any_temporal() {
        let dtype = Interval::new(Nullable).erased();
        let temporal = dtype.metadata::<AnyTemporal>();
        assert_eq!(temporal, TemporalMetadata::Interval);
        assert_eq!(temporal.time_unit(), None);
        assert!(temporal.to_jiff(0).is_err());
    }

    #[rstest]
    #[case::wrong_names(DType::Struct(
        StructFields::new(
            ["m", "d", "n"].into(),
            vec![
                DType::Primitive(PType::I32, NonNullable),
                DType::Primitive(PType::I32, NonNullable),
                DType::Primitive(PType::I64, NonNullable),
            ],
        ),
        Nullable,
    ))]
    #[case::nullable_field(DType::Struct(
        StructFields::new(
            Interval::FIELD_NAMES.into(),
            vec![
                DType::Primitive(PType::I32, Nullable),
                DType::Primitive(PType::I32, NonNullable),
                DType::Primitive(PType::I64, NonNullable),
            ],
        ),
        Nullable,
    ))]
    #[case::primitive(DType::Primitive(PType::I64, Nullable))]
    fn reject_wrong_storage(#[case] storage: DType) {
        assert!(ExtDType::<Interval>::try_new(EmptyMetadata, storage).is_err());
    }

    #[test]
    fn metadata_must_be_empty() -> VortexResult<()> {
        assert!(Interval.serialize_metadata(&EmptyMetadata)?.is_empty());
        assert!(Interval.deserialize_metadata(&[]).is_ok());
        assert!(Interval.deserialize_metadata(&[0]).is_err());
        Ok(())
    }
}
