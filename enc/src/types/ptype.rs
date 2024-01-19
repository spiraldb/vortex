use std::fmt::{Debug, Display};

use arrow::datatypes::{ArrowNativeType, DataType};
use half::f16;

use crate::error::{EncError, EncResult};
use crate::types::{DType, IntWidth};

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

pub trait NativePType:
    Send + Sync + Sized + Debug + Display + PartialEq + Default + ArrowNativeType
{
    const PTYPE: PType;
}

macro_rules! native_ptype {
    ($T:ty, $ptype:tt) => {
        impl NativePType for $T {
            const PTYPE: PType = PType::$ptype;
        }
    };
}

native_ptype!(u8, U8);
native_ptype!(u16, U16);
native_ptype!(u32, U32);
native_ptype!(u64, U64);
native_ptype!(i8, I8);
native_ptype!(i16, I16);
native_ptype!(i32, I32);
native_ptype!(i64, I64);
native_ptype!(f16, F16);
native_ptype!(f32, F32);
native_ptype!(f64, F64);

macro_rules! match_each_native_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        match $self {
            PType::I8 => __with__! { i8 },
            PType::I16 => __with__! { i16 },
            PType::I32 => __with__! { i32 },
            PType::I64 => __with__! { i64 },
            PType::U8 => __with__! { u8 },
            PType::U16 => __with__! { u16 },
            PType::U32 => __with__! { u32 },
            PType::U64 => __with__! { u64 },
            PType::F16 => __with__! { f16 },
            PType::F32 => __with__! { f32 },
            PType::F64 => __with__! { f64 },
        }
    })
}
pub(crate) use match_each_native_ptype;

macro_rules! match_each_integral_float_ptype {
    ($self:expr, | $_1:tt $enc:ident | { $($body:tt)* }, | $_2:tt $enc_f:ident | { $($body_f:tt)* }) => {{
        macro_rules! __with__i {
            ( $_1 $enc:ident ) => (
                { $($body)* }
            );
        }
        macro_rules! __with__f {
            ( $_2 $enc_f:ident ) => (
                { $($body_f)* }
            );
        }
        match $self {
            PType::I8 => __with__i! { i8 },
            PType::I16 => __with__i! { i16 },
            PType::I32 => __with__i! { i32 },
            PType::I64 => __with__i! { i64 },
            PType::U8 => __with__i! { u8 },
            PType::U16 => __with__i! { u16 },
            PType::U32 => __with__i! { u32 },
            PType::U64 => __with__i! { u64 },
            PType::F16 => __with__f! { f16 },
            PType::F32 => __with__f! { f32 },
            PType::F64 => __with__f! { f64 },
        }
    }};
}

pub(crate) use match_each_integral_float_ptype;

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

    pub fn byte_width(&self) -> usize {
        match_each_native_ptype!(self, |$T| std::mem::size_of::<$T>())
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
