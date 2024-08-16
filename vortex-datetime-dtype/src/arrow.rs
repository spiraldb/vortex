#![cfg(feature = "arrow")]

use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use vortex_dtype::ExtDType;

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
        _ => unimplemented!("we should fix this"),
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(value: &ArrowTimeUnit) -> Self {
        match value {
            ArrowTimeUnit::Second => Self::S,
            ArrowTimeUnit::Millisecond => Self::Ms,
            ArrowTimeUnit::Microsecond => Self::Us,
            ArrowTimeUnit::Nanosecond => Self::Ns,
        }
    }
}

impl From<TimeUnit> for ArrowTimeUnit {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::S => Self::Second,
            TimeUnit::Ms => Self::Millisecond,
            TimeUnit::Us => Self::Microsecond,
            TimeUnit::Ns => Self::Nanosecond,
            _ => panic!("cannot convert {value} to Arrow TimeUnit"),
        }
    }
}
