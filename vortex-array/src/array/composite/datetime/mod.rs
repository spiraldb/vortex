use crate::array::composite::typed::{composite_impl, CompositeMetadata, TypedCompositeArray};
use crate::array::composite::CompositeID;
/// Arrow Datetime Types
/// time32/64 - time of day
///   => LocalTime
/// date32 - days since unix epoch
/// date64 - millis since unix epoch
///   => LocalDate
/// timestamp(unit, tz)
///   => Instant iff tz == UTC
///   => ZonedDateTime(Instant, tz)
/// timestamp(unit)
///   => LocalDateTime (tz is "unknown", not "UTC")
/// duration
///   => Duration
use crate::array::Array;
use crate::composite_dtypes::{TimeUnit, TimeUnitSerializer};
use crate::dtype::Metadata;
use crate::error::VortexResult;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub trait CompositeDType {
    const ID: CompositeID;
}

pub trait CompositeArrayImpl {
    fn id(&self) -> Arc<String>;
    fn metadata(&self) -> &Metadata;
    fn underlying(&self) -> &dyn Array;
}

#[derive(Debug, Clone)]
pub struct LocalDateTime {
    time_unit: TimeUnit,
}

impl LocalDateTime {
    pub fn new(time_unit: TimeUnit) -> Self {
        Self { time_unit }
    }
}

impl Display for LocalDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.time_unit)
    }
}

impl CompositeMetadata for LocalDateTime {
    const ID: CompositeID = CompositeID("vortex.localdatetime");

    fn deserialize(metadata: &[u8]) -> VortexResult<Self> {
        Ok(Self::new(TimeUnitSerializer::deserialize(metadata)))
    }
}

composite_impl!("vortex.localdatetime", LocalDateTime);

pub type LocalDateTimeArray = TypedCompositeArray<LocalDateTime>;
