use crate::array::typed::TypedArray;
use crate::array::Array;
use crate::composite_dtypes::{TimeUnit, TimeUnitSerializer};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::cast;
use crate::compute::flatten::{flatten_bool, flatten_primitive, flatten_struct};
use crate::compute::scalar_at::scalar_at;
use crate::dtype::DType;
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;
use crate::stats::Stat;
use arrow_array::{
    ArrayRef as ArrowArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_buffer::NullBuffer;
use std::sync::Arc;

impl AsArrowArray for TypedArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // Decide based on the DType if we know how to do this or not...
        match self.dtype() {
            DType::Composite(id, _dtype, metadata) => match id.as_str() {
                "zoneddatetime" => hacky_zoneddatetime_as_arrow(self.untyped_array(), metadata),
                &_ => Err(VortexError::InvalidArgument(
                    format!("Cannot convert composite DType {} to arrow", id).into(),
                )),
            },
            _ => Err(VortexError::InvalidArgument(
                format!("Cannot convert {} into Arrow array", self.dtype().clone()).into(),
            )),
        }
    }
}

fn hacky_zoneddatetime_as_arrow(array: &dyn Array, metadata: &[u8]) -> VortexResult<ArrowArrayRef> {
    // A ZonedDateTime is currently just a primitive that ignores the timezone...
    let array = flatten_primitive(cast(array, &PType::I64.into())?.as_ref())?;

    let values = array.scalar_buffer::<i64>();
    let validity = array
        .validity()
        .map(flatten_bool)
        .transpose()?
        .map(|b| NullBuffer::new(b.buffer().clone()));

    let time_unit = TimeUnitSerializer::deserialize(metadata);
    Ok(match time_unit {
        TimeUnit::Ns => Arc::new(TimestampNanosecondArray::new(values, validity)),
        TimeUnit::Us => Arc::new(TimestampMicrosecondArray::new(values, validity)),
        TimeUnit::Ms => Arc::new(TimestampMillisecondArray::new(values, validity)),
        TimeUnit::S => Arc::new(TimestampSecondArray::new(values, validity)),
    })
}

// FIXME(ngates): this is what ZonedDateTime should look like, but it's not implemented yet.
#[allow(dead_code)]
fn zoneddatetime_as_arrow(array: &dyn Array, metadata: &[u8]) -> VortexResult<ArrowArrayRef> {
    // A ZonedDateTime is a composite of {instant, timezone}.
    // TODO(ngates): make this actually a composite of Instant, instead of directly a primitive.
    let array = flatten_struct(array)?;
    assert_eq!(array.names()[0].as_str(), "instant");
    assert_eq!(array.names()[1].as_str(), "timezone");

    // Map the instant into an i64 primitive
    let instant = array.fields().first().unwrap();
    let instant = flatten_primitive(cast(instant.as_ref(), &PType::I64.into())?.as_ref())?;

    // Extract the values and validity buffer
    let values = instant.scalar_buffer::<i64>();
    let validity = instant
        .validity()
        .map(flatten_bool)
        .transpose()?
        .map(|b| NullBuffer::new(b.buffer().clone()));

    // Unwrap the constant timezone
    let timezone = array.fields().get(1).unwrap();
    if !timezone
        .stats()
        .get_or_compute_as::<bool>(&Stat::IsConstant)
        .unwrap_or(false)
    {
        return Err(VortexError::InvalidArgument(
            "Timezone must be constant to convert into Arrow".into(),
        ));
    }
    let _timezone = scalar_at(timezone.as_ref(), 0)?;

    // Extract the instant unit
    let time_unit = TimeUnitSerializer::deserialize(metadata);

    Ok(match time_unit {
        TimeUnit::Ns => Arc::new(TimestampNanosecondArray::new(values, validity)),
        TimeUnit::Us => Arc::new(TimestampMicrosecondArray::new(values, validity)),
        TimeUnit::Ms => Arc::new(TimestampMillisecondArray::new(values, validity)),
        TimeUnit::S => Arc::new(TimestampSecondArray::new(values, validity)),
    })
}
