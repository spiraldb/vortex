use std::panic::RefUnwindSafe;

use arrow2::datatypes::DataType;
use arrow2::datatypes::PrimitiveType as ArrowPrimitiveType;
use half::f16;

use crate::error::{EncError, EncResult};
use crate::types::{DType, IntWidth};

#[derive(Debug, Clone, PartialEq)]
pub enum PValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F16(f16),
    F32(f32),
    F64(f64),
}

impl PValue {
    pub fn ptype(&self) -> PType {
        match self {
            PValue::U8(_) => PType::U8,
            PValue::U16(_) => PType::U16,
            PValue::U32(_) => PType::U32,
            PValue::U64(_) => PType::U64,
            PValue::I8(_) => PType::I8,
            PValue::I16(_) => PType::I16,
            PValue::I32(_) => PType::I32,
            PValue::I64(_) => PType::I64,
            PValue::F16(_) => PType::F16,
            PValue::F32(_) => PType::F32,
            PValue::F64(_) => PType::F64,
        }
    }
}

macro_rules! match_each_pvalue {
    ($self:expr, | $_:tt $pvalue:ident | $($body:tt)*) => ({
        macro_rules! __with_pvalue__ {( $_ $pvalue:ident ) => ( $($body)* )}
        match $self {
            PValue::U8(v) => __with_pvalue__! { v },
            PValue::U16(v) => __with_pvalue__! { v },
            PValue::U32(v) => __with_pvalue__! { v },
            PValue::U64(v) => __with_pvalue__! { v },
            PValue::I8(v) => __with_pvalue__! { v },
            PValue::I16(v) => __with_pvalue__! { v },
            PValue::I32(v) => __with_pvalue__! { v },
            PValue::I64(v) => __with_pvalue__! { v },
            PValue::F16(v) => __with_pvalue__! { v },
            PValue::F32(v) => __with_pvalue__! { v },
            PValue::F64(v) => __with_pvalue__! { v },
        }
    })
}

macro_rules! match_each_pvalue_integer {
    ($self:expr, | $_:tt $pvalue:ident | $($body:tt)*) => ({
        macro_rules! __with_pvalue__ {( $_ $pvalue:ident ) => ( $($body)* )}
        match $self {
            PValue::U8(v) => __with_pvalue__! { v },
            PValue::U16(v) => __with_pvalue__! { v },
            PValue::U32(v) => __with_pvalue__! { v },
            PValue::U64(v) => __with_pvalue__! { v },
            PValue::I8(v) => __with_pvalue__! { v },
            PValue::I16(v) => __with_pvalue__! { v },
            PValue::I32(v) => __with_pvalue__! { v },
            PValue::I64(v) => __with_pvalue__! { v },
            _ => Err(EncError::InvalidDType($self.ptype().into())),
        }
    })
}

pub(crate) use match_each_pvalue;
pub(crate) use match_each_pvalue_integer;

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

    fn pvalue(self) -> PValue;
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
    ($type:ty, $ptype:tt) => {
        impl PrimitiveType for $type {
            const PTYPE: PType = PType::$ptype;
            type Bytes = [u8; std::mem::size_of::<Self>()];

            fn pvalue(self) -> PValue {
                PValue::$ptype(self)
            }
        }

        impl TryFrom<PValue> for $type {
            type Error = EncError;

            fn try_from(value: PValue) -> EncResult<Self> {
                match value {
                    PValue::$ptype(v) => Ok(v),
                    _ => Err(EncError::InvalidDType(value.ptype().into())),
                }
            }
        }
    };
}

ptype!(u8, U8);
ptype!(u16, U16);
ptype!(u32, U32);
ptype!(u64, U64);
ptype!(i8, I8);
ptype!(i16, I16);
ptype!(i32, I32);
ptype!(i64, I64);
// f16 is not a builtin types thus implemented in f16.rs
ptype!(f16, F16);
ptype!(f32, F32);
ptype!(f64, F64);

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
            // DataType::Float16 => Ok(PType::F16),
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
            // ArrowPrimitiveType::Float16 => Ok(PType::F16),
            ArrowPrimitiveType::Float32 => Ok(PType::F32),
            ArrowPrimitiveType::Float64 => Ok(PType::F64),
            _ => Err(()),
        }
    }
}
