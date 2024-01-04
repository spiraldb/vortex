use std::panic::RefUnwindSafe;

use arrow2::datatypes::DataType;
use arrow2::datatypes::PrimitiveType as ArrowPrimitiveType;
use arrow2::types::NativeType;
use bytemuck::Pod;

use crate::types::{DType, IntWidth};

pub trait PrimitiveType:
    super::private::Sealed
    + Pod
    + Send
    + Sync
    + Sized
    + RefUnwindSafe
    + std::fmt::Debug
    + std::fmt::Display
    + PartialEq
    + Default
{
    const PTYPE: PType;
    type ArrowType: NativeType;
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

macro_rules! ptype {
    ($type:ty, $ptype:expr) => {
        impl PrimitiveType for $type {
            const PTYPE: PType = $ptype;
            type ArrowType = Self;
            type Bytes = [u8; std::mem::size_of::<Self>()];
        }
    };
}

ptype!(u8, PType::U8);
ptype!(u16, PType::U16);
ptype!(u32, PType::U32);
ptype!(u64, PType::U64);
ptype!(i8, PType::I8);
ptype!(i16, PType::I16);
ptype!(i32, PType::I32);
ptype!(i64, PType::I64);
// f16 is not a builtin types thus implemented in f16.rs
ptype!(f32, PType::F32);
ptype!(f64, PType::F64);

impl TryFrom<&DType> for PType {
    type Error = ();

    fn try_from(value: &DType) -> Result<Self, Self::Error> {
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
            _ => Err(()),
        }
    }
}

impl TryFrom<&arrow2::datatypes::DataType> for PType {
    type Error = ();

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Int8 => Ok(PType::I8),
            DataType::Int16 => Ok(PType::I16),
            DataType::Int32 => Ok(PType::I32),
            DataType::Int64 => Ok(PType::I64),
            DataType::UInt8 => Ok(PType::U8),
            DataType::UInt16 => Ok(PType::U16),
            DataType::UInt32 => Ok(PType::U32),
            DataType::UInt64 => Ok(PType::U64),
            DataType::Float16 => Ok(PType::F16),
            DataType::Float32 => Ok(PType::F32),
            DataType::Float64 => Ok(PType::F64),
            _ => Err(()),
        }
    }
}

impl TryFrom<ArrowPrimitiveType> for PType {
    type Error = ();

    fn try_from(value: ArrowPrimitiveType) -> Result<Self, Self::Error> {
        match value {
            ArrowPrimitiveType::Int8 => Ok(PType::I8),
            ArrowPrimitiveType::Int16 => Ok(PType::I16),
            ArrowPrimitiveType::Int32 => Ok(PType::I32),
            ArrowPrimitiveType::Int64 => Ok(PType::I64),
            ArrowPrimitiveType::UInt8 => Ok(PType::U8),
            ArrowPrimitiveType::UInt16 => Ok(PType::U16),
            ArrowPrimitiveType::UInt32 => Ok(PType::U32),
            ArrowPrimitiveType::UInt64 => Ok(PType::U64),
            ArrowPrimitiveType::Float16 => Ok(PType::F16),
            ArrowPrimitiveType::Float32 => Ok(PType::F32),
            ArrowPrimitiveType::Float64 => Ok(PType::F64),
            _ => Err(()),
        }
    }
}
