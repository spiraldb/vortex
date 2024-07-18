use std::fmt::{Display, Formatter};

use arrow_schema::DataType;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
pub use temporal::TemporalArray;
use vortex_dtype::ExtDType;

use crate::array::datetime::temporal::{TemporalMetadata, DATE_ID, TIMESTAMP_ID, TIME_ID};

pub mod temporal;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    IntoPrimitive,
    TryFromPrimitive,
)]
#[repr(u8)]
pub enum TimeUnit {
    Ns,
    Us,
    Ms,
    S,
    D,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ns => write!(f, "ns"),
            Self::Us => write!(f, "µs"),
            Self::Ms => write!(f, "ms"),
            Self::S => write!(f, "s"),
            Self::D => write!(f, "days"),
        }
    }
}

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
