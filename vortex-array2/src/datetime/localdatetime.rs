use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use vortex::ptype::PType;
use vortex_error::VortexResult;
use vortex_schema::CompositeID;

use crate::array::composite::{composite_impl, TypedCompositeArray};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::cast;
use crate::datetime::TimeUnit;
use crate::validity::ArrayValidity;
use crate::{ArrayMetadata, TryDeserializeArrayMetadata, TrySerializeArrayMetadata};

#[derive(Debug, Clone)]
pub struct LocalDateTime {
    time_unit: TimeUnit,
}

composite_impl!("vortex.localdatetime", LocalDateTime);

impl LocalDateTime {
    pub fn new(time_unit: TimeUnit) -> Self {
        Self { time_unit }
    }

    #[inline]
    pub fn time_unit(&self) -> TimeUnit {
        self.time_unit
    }
}

impl TrySerializeArrayMetadata for LocalDateTime {
    fn try_serialize_metadata(&self) -> VortexResult<Arc<[u8]>> {
        todo!()
    }
}

impl<'m> TryDeserializeArrayMetadata<'m> for LocalDateTime {
    fn try_deserialize_metadata(_metadata: Option<&'m [u8]>) -> VortexResult<Self> {
        todo!()
    }
}

impl ArrayMetadata for LocalDateTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

impl Display for LocalDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.time_unit)
    }
}
//
// impl BytesSerde for LocalDateTime {
//     fn serialize(&self) -> Vec<u8> {
//         self.time_unit.serialize()
//     }
//
//     fn deserialize(metadata: &[u8]) -> VortexResult<Self> {
//         TimeUnit::deserialize(metadata).map(Self::new)
//     }
// }

pub type LocalDateTimeArray<'a> = TypedCompositeArray<'a, LocalDateTime>;

impl ArrayCompute for LocalDateTimeArray<'_> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }
}

impl AsArrowArray for LocalDateTimeArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // A LocalDateTime maps to an Arrow Timestamp array with no timezone.
        let timestamps = cast(self.underlying(), PType::I64.into())?.flatten_primitive()?;
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
