use std::panic::RefUnwindSafe;

use arrow2::datatypes::DataType;
use arrow2::datatypes::PrimitiveType as ArrowPrimitiveType;

use crate::error::{EncError, EncResult};
use crate::types::{DType, IntWidth};

pub trait PrimitiveType:
    Send + Sync + Sized + RefUnwindSafe + std::fmt::Debug + std::fmt::Display + PartialEq + Default
{
    const PTYPE: PType;
    type Bytes: AsRef<[u8]>
        + std::ops::Index<usize, Output = u8>
        + std::ops::IndexMut<usize, Output = u8>
        + for<'a> TryFrom<&'a [u8]>
        + std::fmt::Debug
        + Default;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F16,
    F32,
    F64,
}

impl PType {
    pub fn is_unsigned_int(self) -> bool {
        matches!(self, PType::U8 | PType::U16 | PType::U32 | PType::U64)
    }

    pub fn is_signed_int(self) -> bool {
        matches!(self, PType::I8 | PType::I16 | PType::I32 | PType::I64)
    }

    pub fn is_int(self) -> bool {
        self.is_unsigned_int() || self.is_signed_int()
    }

    pub fn is_float(self) -> bool {
        matches!(self, PType::F16 | PType::F32 | PType::F64)
    }
}

impl TryFrom<&DType> for PType {
    type Error = EncError;

    fn try_from(value: &DType) -> EncResult<Self> {
        match value {
            DType::Int(w) => match w {
                IntWidth::Unknown => Ok(PType::I64),
                IntWidth::_8 => Ok(PType::I8),
                IntWidth::_16 => Ok(PType::I16),
                IntWidth::_32 => Ok(PType::I32),
                IntWidth::_64 => Ok(PType::I64),
            },
            DType::UInt(w) => match w {
                IntWidth::Unknown => Ok(PType::U64),
                IntWidth::_8 => Ok(PType::U8),
                IntWidth::_16 => Ok(PType::U16),
                IntWidth::_32 => Ok(PType::U32),
                IntWidth::_64 => Ok(PType::U64),
            },
            _ => Err(EncError::InvalidDType(value.clone())),
        }
    }
}

impl TryFrom<&DataType> for PType {
    type Error = EncError;

    fn try_from(value: &DataType) -> EncResult<Self> {
        match value {
            DataType::Int8 => Ok(PType::I8),
            DataType::Int16 => Ok(PType::I16),
            DataType::Int32 => Ok(PType::I32),
            DataType::Int64 => Ok(PType::I64),
            DataType::UInt8 => Ok(PType::U8),
            DataType::UInt16 => Ok(PType::U16),
            DataType::UInt32 => Ok(PType::U32),
            DataType::UInt64 => Ok(PType::U64),
            // DataType::Float16 => Ok(PType::F16),
            DataType::Float32 => Ok(PType::F32),
            DataType::Float64 => Ok(PType::F64),
            _ => Err(EncError::InvalidArrowDataType(value.clone())),
        }
    }
}

impl TryFrom<ArrowPrimitiveType> for PType {
    type Error = EncError;

    fn try_from(value: ArrowPrimitiveType) -> EncResult<Self> {
        match value {
            ArrowPrimitiveType::Int8 => Ok(PType::I8),
            ArrowPrimitiveType::Int16 => Ok(PType::I16),
            ArrowPrimitiveType::Int32 => Ok(PType::I32),
            ArrowPrimitiveType::Int64 => Ok(PType::I64),
            ArrowPrimitiveType::UInt8 => Ok(PType::U8),
            ArrowPrimitiveType::UInt16 => Ok(PType::U16),
            ArrowPrimitiveType::UInt32 => Ok(PType::U32),
            ArrowPrimitiveType::UInt64 => Ok(PType::U64),
            // ArrowPrimitiveType::Float16 => Ok(PType::F16),
            ArrowPrimitiveType::Float32 => Ok(PType::F32),
            ArrowPrimitiveType::Float64 => Ok(PType::F64),
            _ => Err(EncError::InvalidArrowDataType(value.into())),
        }
    }
}
