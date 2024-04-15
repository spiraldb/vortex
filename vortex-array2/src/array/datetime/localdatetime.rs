use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use serde::{Deserialize, Serialize};
use vortex::ptype::PType;
use vortex_error::VortexResult;

use crate::array::datetime::TimeUnit;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::cast;
use crate::impl_composite;
use crate::validity::ArrayValidity;

impl_composite!("vortex.localdatetime", LocalDateTime);

pub type LocalDateTimeArray<'a> = TypedCompositeArray<'a, LocalDateTime>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalDateTime {
    time_unit: TimeUnit,
}

impl ArrayCompute for LocalDateTimeArray<'_> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }
}

impl AsArrowArray for LocalDateTimeArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // A LocalDateTime maps to an Arrow Timestamp array with no timezone.
        let timestamps = cast(&self.underlying(), PType::I64.into())?.flatten_primitive()?;
        let validity = timestamps.logical_validity().to_null_buffer()?;
        let buffer = timestamps.scalar_buffer::<i64>();

        Ok(match self.underlying_metadata().time_unit {
            TimeUnit::Ns => Arc::new(TimestampNanosecondArray::new(buffer, validity)),
            TimeUnit::Us => Arc::new(TimestampMicrosecondArray::new(buffer, validity)),
            TimeUnit::Ms => Arc::new(TimestampMillisecondArray::new(buffer, validity)),
            TimeUnit::S => Arc::new(TimestampSecondArray::new(buffer, validity)),
        })
    }
}
