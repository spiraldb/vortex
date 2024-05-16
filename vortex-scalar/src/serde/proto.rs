#![cfg(feature = "proto")]

use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value};
use vortex_buffer::BufferString;
use vortex_dtype::{DType, StructDType};
use vortex_error::{vortex_bail, vortex_err, VortexError};

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

        let scalar_value = value
            .value
            .as_ref()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Scalar missing value"))?;

        let pb_value = scalar_value
            .value
            .as_ref()
            .ok_or_else(|| vortex_err!(InvalidSerde: "ScalarValue missing value"))?;

        let value = try_from_value(&dtype, pb_value)?;

        Ok(Self { dtype, value })
    }
}

fn try_from_value(dtype: &DType, value: &Value) -> Result<ScalarValue, VortexError> {
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| vortex_err!(InvalidSerde: "Value missing kind"))?;

    Ok(match kind {
        Kind::NullValue(_) => ScalarValue::Null,
        Kind::BoolValue(v) => ScalarValue::Bool(*v),
        Kind::NumberValue(v) => ScalarValue::Primitive(PValue::F64(*v)),
        Kind::StringValue(v) => ScalarValue::BufferString(BufferString::from(v.clone())),
        Kind::ListValue(v) => {
            if let DType::List(elem_dtype, _) = dtype {
                try_from_list_value(elem_dtype, v)?
            } else {
                vortex_bail!(InvalidSerde: "Expected a list dtype, found {:?}", dtype);
            }
        }
        Kind::StructValue(v) => {
            if let DType::Struct(sdt, _) = dtype {
                try_from_struct_value(sdt, v)?
            } else {
                vortex_bail!(InvalidSerde: "Expected a struct dtype, found {:?}", dtype);
            }
        }
    })
}

fn try_from_list_value(elem_dtype: &DType, value: &ListValue) -> Result<ScalarValue, VortexError> {
    let mut values = vec![];

    for elem in value.values.iter() {
        values.push(try_from_value(elem_dtype, elem)?);
    }

    Ok(ScalarValue::List(values.into()))
}

fn try_from_struct_value(dtype: &StructDType, value: &Struct) -> Result<ScalarValue, VortexError> {
    let mut values = vec![];

    for (field, field_dt) in dtype.names().iter().zip(dtype.dtypes().iter()) {
        if let Some((_, v)) =
            // Add field values in order defined by the struct dtype.
            value
                .fields
                .iter()
                .find(|(f, _)| field.as_ref() == f.as_str())
        {
            values.push(try_from_value(field_dt, v)?);
        } else if field_dt.is_nullable() {
            values.push(ScalarValue::Null);
        } else {
            vortex_bail!(InvalidSerde: "Non-nullable struct field {} not found", field);
        }
    }

    Ok(ScalarValue::List(values.into()))
}
