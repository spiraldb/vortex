use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::ptype::PType;

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum PTypeTag {
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

impl From<PType> for PTypeTag {
    fn from(value: PType) -> Self {
        match value {
            PType::U8 => PTypeTag::U8,
            PType::U16 => PTypeTag::U16,
            PType::U32 => PTypeTag::U32,
            PType::U64 => PTypeTag::U64,
            PType::I8 => PTypeTag::I8,
            PType::I16 => PTypeTag::I16,
            PType::I32 => PTypeTag::I32,
            PType::I64 => PTypeTag::I64,
            PType::F16 => PTypeTag::F16,
            PType::F32 => PTypeTag::F32,
            PType::F64 => PTypeTag::F64,
        }
    }
}

impl From<PTypeTag> for PType {
    fn from(value: PTypeTag) -> Self {
        match value {
            PTypeTag::U8 => PType::U8,
            PTypeTag::U16 => PType::U16,
            PTypeTag::U32 => PType::U32,
            PTypeTag::U64 => PType::U64,
            PTypeTag::I8 => PType::I8,
            PTypeTag::I16 => PType::I16,
            PTypeTag::I32 => PType::I32,
            PTypeTag::I64 => PType::I64,
            PTypeTag::F16 => PType::F16,
            PTypeTag::F32 => PType::F32,
            PTypeTag::F64 => PType::F64,
        }
    }
}
