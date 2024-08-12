#![cfg(feature = "datafusion")]
use datafusion_common::ScalarValue;
use vortex_dtype::{DType, PType};

use crate::{PValue, Scalar};

impl From<Scalar> for ScalarValue {
    fn from(value: Scalar) -> Self {
        match value.dtype {
            DType::Null => ScalarValue::Null,
            DType::Bool(_) => ScalarValue::Boolean(value.value.as_bool().unwrap_or_else(|err| {
                panic!("Expected a bool scalar: {}", err)
            })),
            DType::Primitive(ptype, _) => {
                let pvalue = value.value.as_pvalue().unwrap_or_else(|err| {
                    panic!("Expected a pvalue scalar: {}", err)
                });
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
                    .unwrap_or_else(|err| {
                        panic!("Expected a buffer string: {}", err)
                    })
                    .map(|b| b.as_str().to_string()),
            ),
            DType::Binary(_) => ScalarValue::Binary(
                value
                    .value
                    .as_buffer()
                    .unwrap_or_else(|err| {
                        panic!("Expected a buffer: {}", err)
                    })
                    .map(|b| b.into_vec().unwrap_or_else(|buf| buf.as_slice().to_vec())),
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

impl From<ScalarValue> for Scalar {
    fn from(value: ScalarValue) -> Scalar {
        match value {
            ScalarValue::Null => Some(Scalar::null(DType::Null)),
            ScalarValue::Boolean(b) => b.map(Scalar::from),
            ScalarValue::Float16(f) => f.map(Scalar::from),
            ScalarValue::Float32(f) => f.map(Scalar::from),
            ScalarValue::Float64(f) => f.map(Scalar::from),
            ScalarValue::Int8(i) => i.map(Scalar::from),
            ScalarValue::Int16(i) => i.map(Scalar::from),
            ScalarValue::Int32(i) => i.map(Scalar::from),
            ScalarValue::Int64(i) => i.map(Scalar::from),
            ScalarValue::UInt8(i) => i.map(Scalar::from),
            ScalarValue::UInt16(i) => i.map(Scalar::from),
            ScalarValue::UInt32(i) => i.map(Scalar::from),
            ScalarValue::UInt64(i) => i.map(Scalar::from),
            ScalarValue::Utf8(s) => s.as_ref().map(|s| Scalar::from(s.as_str())),
            ScalarValue::Utf8View(s) => s.as_ref().map(|s| Scalar::from(s.as_str())),
            ScalarValue::LargeUtf8(s) => s.as_ref().map(|s| Scalar::from(s.as_str())),
            ScalarValue::Binary(b) => b.as_ref().map(|b| Scalar::from(b.clone())),
            ScalarValue::BinaryView(b) => b.as_ref().map(|b| Scalar::from(b.clone())),
            ScalarValue::LargeBinary(b) => b.as_ref().map(|b| Scalar::from(b.clone())),
            ScalarValue::FixedSizeBinary(_, b) => b.map(|b| Scalar::from(b.clone())),
            _ => unimplemented!(),
        }
        .unwrap_or(Scalar::null(DType::Null))
    }
}
