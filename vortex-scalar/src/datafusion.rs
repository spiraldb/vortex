#![cfg(feature = "datafusion")]
use datafusion_common::ScalarValue;
use vortex_dtype::{DType, PType};

use crate::{PValue, Scalar};

impl From<Scalar> for ScalarValue {
    fn from(value: Scalar) -> Self {
        match value.dtype {
            DType::Null => ScalarValue::Null,
            DType::Bool(_) => ScalarValue::Boolean(value.value.as_bool().expect("should be bool")),
            DType::Primitive(ptype, _) => {
                let pvalue = value.value.as_pvalue().expect("should be pvalue");
                match pvalue {
                    None => match ptype {
                        PType::U8 => ScalarValue::UInt8(None),
                        PType::U16 => ScalarValue::UInt16(None),
                        PType::U32 => ScalarValue::UInt32(None),
                        PType::U64 => ScalarValue::UInt64(None),
                        PType::I8 => ScalarValue::Int8(None),
                        PType::I16 => ScalarValue::Int16(None),
                        PType::I32 => ScalarValue::Int32(None),
                        PType::I64 => ScalarValue::Int64(None),
                        PType::F16 => ScalarValue::Float16(None),
                        PType::F32 => ScalarValue::Float32(None),
                        PType::F64 => ScalarValue::Float64(None),
                    },
                    Some(pvalue) => match pvalue {
                        PValue::U8(v) => ScalarValue::UInt8(Some(v)),
                        PValue::U16(v) => ScalarValue::UInt16(Some(v)),
                        PValue::U32(v) => ScalarValue::UInt32(Some(v)),
                        PValue::U64(v) => ScalarValue::UInt64(Some(v)),
                        PValue::I8(v) => ScalarValue::Int8(Some(v)),
                        PValue::I16(v) => ScalarValue::Int16(Some(v)),
                        PValue::I32(v) => ScalarValue::Int32(Some(v)),
                        PValue::I64(v) => ScalarValue::Int64(Some(v)),
                        PValue::F16(v) => ScalarValue::Float16(Some(v)),
                        PValue::F32(v) => ScalarValue::Float32(Some(v)),
                        PValue::F64(v) => ScalarValue::Float64(Some(v)),
                    },
                }
            }
            DType::Utf8(_) => ScalarValue::Utf8(
                value
                    .value
                    .as_buffer_string()
                    .expect("should be buffer string")
                    .map(|b| b.as_str().to_string()),
            ),
            DType::Binary(_) => ScalarValue::Binary(
                value
                    .value
                    .as_buffer()
                    .expect("should be buffer")
                    .map(|b| b.as_slice().to_vec()),
            ),
            DType::Struct(..) => {
                todo!("struct scalar conversion")
            }
            DType::List(..) => {
                todo!("list scalar conversion")
            }
            DType::Extension(..) => {
                todo!("extension scalar conversion")
            }
        }
    }
}
