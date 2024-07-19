#![cfg(feature = "arrow")]

use std::sync::Arc;

use arrow_array::*;
use vortex_dtype::{DType, PType};

use crate::{PValue, Scalar};

impl From<Scalar> for Arc<dyn Datum> {
    fn from(value: Scalar) -> Arc<dyn Datum> {
        match value.dtype {
            DType::Null => Arc::new(NullArray::new(1)),
            DType::Bool(_) => match value.value.as_bool().expect("should be bool") {
                Some(b) => Arc::new(BooleanArray::new_scalar(b)),
                None => Arc::new(BooleanArray::new_null(1)),
            },
            DType::Primitive(ptype, _) => {
                let pvalue = value.value.as_pvalue().expect("should be pvalue");
                match pvalue {
                    None => match ptype {
                        PType::U8 => Arc::new(UInt8Array::new_null(1)),
                        PType::U16 => Arc::new(UInt16Array::new_null(1)),
                        PType::U32 => Arc::new(UInt32Array::new_null(1)),
                        PType::U64 => Arc::new(UInt64Array::new_null(1)),
                        PType::I8 => Arc::new(Int8Array::new_null(1)),
                        PType::I16 => Arc::new(Int16Array::new_null(1)),
                        PType::I32 => Arc::new(Int32Array::new_null(1)),
                        PType::I64 => Arc::new(Int64Array::new_null(1)),
                        PType::F16 => Arc::new(Float16Array::new_null(1)),
                        PType::F32 => Arc::new(Float32Array::new_null(1)),
                        PType::F64 => Arc::new(Float64Array::new_null(1)),
                    },
                    Some(pvalue) => match pvalue {
                        PValue::U8(v) => Arc::new(UInt8Array::new_scalar(v)),
                        PValue::U16(v) => Arc::new(UInt16Array::new_scalar(v)),
                        PValue::U32(v) => Arc::new(UInt32Array::new_scalar(v)),
                        PValue::U64(v) => Arc::new(UInt64Array::new_scalar(v)),
                        PValue::I8(v) => Arc::new(Int8Array::new_scalar(v)),
                        PValue::I16(v) => Arc::new(Int16Array::new_scalar(v)),
                        PValue::I32(v) => Arc::new(Int32Array::new_scalar(v)),
                        PValue::I64(v) => Arc::new(Int64Array::new_scalar(v)),
                        PValue::F16(v) => Arc::new(Float16Array::new_scalar(v)),
                        PValue::F32(v) => Arc::new(Float32Array::new_scalar(v)),
                        PValue::F64(v) => Arc::new(Float64Array::new_scalar(v)),
                    },
                }
            }
            DType::Utf8(_) => {
                match value
                    .value
                    .as_buffer_string()
                    .expect("should be buffer string")
                {
                    Some(s) => Arc::new(StringArray::new_scalar(s.as_str())),
                    None => Arc::new(StringArray::new_null(1)),
                }
            }
            DType::Binary(_) => {
                match value
                    .value
                    .as_buffer_string()
                    .expect("should be buffer string")
                {
                    Some(s) => Arc::new(BinaryArray::new_scalar(s.as_bytes())),
                    None => Arc::new(BinaryArray::new_null(1)),
                }
            }
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
