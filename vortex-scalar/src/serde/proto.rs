#![cfg(feature = "proto")]

use vortex_buffer::{Buffer, BufferString};
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexError};

use crate::proto::scalar::scalar::Value;
use crate::pvalue::PValue;
use crate::{proto::scalar as pb, Scalar, ScalarValue};

impl TryFrom<&pb::Scalar> for Scalar {
    type Error = VortexError;

    fn try_from(value: &pb::Scalar) -> Result<Self, Self::Error> {
        let dtype = DType::try_from(
            value
                .dtype
                .as_ref()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Scalar missing dtype"))?,
        )?;

        let value = value
            .value
            .as_ref()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Scalar missing value"))?;

        let value = match value {
            Value::Bool(b) => ScalarValue::Bool(*b),
            Value::Uint32(v) => ScalarValue::Primitive(PValue::U32(*v)),
            Value::Uint64(v) => ScalarValue::Primitive(PValue::U64(*v)),
            Value::Sint32(v) => ScalarValue::Primitive(PValue::I32(*v)),
            Value::Sint64(v) => ScalarValue::Primitive(PValue::I64(*v)),
            Value::Float(v) => ScalarValue::Primitive(PValue::F32(*v)),
            Value::Double(v) => ScalarValue::Primitive(PValue::F64(*v)),
            Value::Bytes(v) => ScalarValue::Buffer(Buffer::from(v.clone())),
            Value::String(v) => ScalarValue::BufferString(BufferString::from(v.clone())),
        };

        Ok(Self { dtype, value })
    }
}
