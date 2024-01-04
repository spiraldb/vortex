use arrow2::types::NativeType;

use bytemuck::Pod;
use std::panic::RefUnwindSafe;

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
