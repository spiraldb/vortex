#![cfg(feature = "arrow")]

use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use vortex_dtype::ExtDType;
use vortex_error::{vortex_bail, VortexError, VortexResult};

use crate::temporal::{TemporalMetadata, DATE_ID, TIMESTAMP_ID, TIME_ID};
use crate::unit::TimeUnit;

/// Construct an extension type from the provided temporal Arrow type.
///
/// Supported types are Date32, Date64, Time32, Time64, Timestamp.
pub fn make_temporal_ext_dtype(data_type: &DataType) -> ExtDType {
    assert!(data_type.is_temporal(), "Must receive a temporal DataType");

    match data_type {
        DataType::Timestamp(time_unit, time_zone) => {
            let time_unit = TimeUnit::from(time_unit);
            let tz = time_zone.clone().map(|s| s.to_string());

            ExtDType::new(
                TIMESTAMP_ID.clone(),
                Some(TemporalMetadata::Timestamp(time_unit, tz).into()),
            )
        }
        DataType::Time32(time_unit) => {
            let time_unit = TimeUnit::from(time_unit);
            ExtDType::new(
                TIME_ID.clone(),
                Some(TemporalMetadata::Time(time_unit).into()),
            )
        }
        DataType::Time64(time_unit) => {
            let time_unit = TimeUnit::from(time_unit);
            ExtDType::new(
                TIME_ID.clone(),
                Some(TemporalMetadata::Time(time_unit).into()),
            )
        }
        DataType::Date32 => ExtDType::new(
            DATE_ID.clone(),
            Some(TemporalMetadata::Date(TimeUnit::D).into()),
        ),
        DataType::Date64 => ExtDType::new(
            DATE_ID.clone(),
            Some(TemporalMetadata::Date(TimeUnit::Ms).into()),
        ),
        _ => unimplemented!("{data_type} conversion"),
    }
}

/// Convert temporal ExtDType to a corresponding arrow DataType
///
/// panics if the ext_dtype is not a temporal dtype
pub fn make_arrow_temporal_dtype(ext_dtype: &ExtDType) -> DataType {
    match TemporalMetadata::try_from(ext_dtype)
        .unwrap_or_else(|err| panic!("make_arrow_temporal_dtype must be called with a temporal ExtDType: {err}"))
    {
        TemporalMetadata::Date(time_unit) => match time_unit {
            TimeUnit::D => DataType::Date32,
            TimeUnit::Ms => DataType::Date64,
            _ => panic!("Invalid TimeUnit {time_unit} for {}", ext_dtype.id()),
        },
        TemporalMetadata::Time(time_unit) => match time_unit {
            TimeUnit::S => DataType::Time32(ArrowTimeUnit::Second),
            TimeUnit::Ms => DataType::Time32(ArrowTimeUnit::Millisecond),
            TimeUnit::Us => DataType::Time64(ArrowTimeUnit::Microsecond),
            TimeUnit::Ns => DataType::Time64(ArrowTimeUnit::Nanosecond),
            _ => panic!("Invalid TimeUnit {time_unit} for {}", ext_dtype.id()),
        },
        TemporalMetadata::Timestamp(time_unit, tz) => match time_unit {
            TimeUnit::Ns => DataType::Timestamp(ArrowTimeUnit::Nanosecond, tz.map(|t| t.into())),
            TimeUnit::Us => DataType::Timestamp(ArrowTimeUnit::Microsecond, tz.map(|t| t.into())),
            TimeUnit::Ms => DataType::Timestamp(ArrowTimeUnit::Millisecond, tz.map(|t| t.into())),
            TimeUnit::S => DataType::Timestamp(ArrowTimeUnit::Second, tz.map(|t| t.into())),
            _ => panic!("Invalid TimeUnit {time_unit} for {}", ext_dtype.id()),
        },
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(value: &ArrowTimeUnit) -> Self {
        (*value).into()
    }
}

impl From<ArrowTimeUnit> for TimeUnit {
    fn from(value: ArrowTimeUnit) -> Self {
        match value {
            ArrowTimeUnit::Second => Self::S,
            ArrowTimeUnit::Millisecond => Self::Ms,
            ArrowTimeUnit::Microsecond => Self::Us,
            ArrowTimeUnit::Nanosecond => Self::Ns,
        }
    }
}

impl TryFrom<TimeUnit> for ArrowTimeUnit {
    type Error = VortexError;

    fn try_from(value: TimeUnit) -> VortexResult<Self> {
        Ok(match value {
            TimeUnit::S => Self::Second,
            TimeUnit::Ms => Self::Millisecond,
            TimeUnit::Us => Self::Microsecond,
            TimeUnit::Ns => Self::Nanosecond,
            _ => vortex_bail!("Cannot convert {value} to Arrow TimeUnit"),
        })
    }
}
