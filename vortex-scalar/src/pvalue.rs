use num_traits::NumCast;
use vortex_dtype::half::f16;
use vortex_dtype::PType;
use vortex_error::vortex_err;
use vortex_error::VortexError;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
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

macro_rules! int_pvalue {
    ($T:ty, $PT:tt) => {
        impl TryFrom<PValue> for $T {
            type Error = VortexError;

            fn try_from(value: PValue) -> Result<Self, Self::Error> {
                match value {
                    PValue::U8(v) => <$T as NumCast>::from(v),
                    PValue::U16(v) => <$T as NumCast>::from(v),
                    PValue::U32(v) => <$T as NumCast>::from(v),
                    PValue::U64(v) => <$T as NumCast>::from(v),
                    PValue::I8(v) => <$T as NumCast>::from(v),
                    PValue::I16(v) => <$T as NumCast>::from(v),
                    PValue::I32(v) => <$T as NumCast>::from(v),
                    PValue::I64(v) => <$T as NumCast>::from(v),
                    _ => None,
                }
                .ok_or_else(|| {
                    vortex_err!("Cannot read primitive value {:?} as {}", value, PType::$PT)
                })
            }
        }
    };
}

int_pvalue!(u8, U8);
int_pvalue!(u16, U16);
int_pvalue!(u32, U32);
int_pvalue!(u64, U64);
int_pvalue!(i8, I8);
int_pvalue!(i16, I16);
int_pvalue!(i32, I32);
int_pvalue!(i64, I64);

macro_rules! float_pvalue {
    ($T:ty, $PT:tt) => {
        impl TryFrom<PValue> for $T {
            type Error = VortexError;

            fn try_from(value: PValue) -> Result<Self, Self::Error> {
                match value {
                    PValue::F16(f) => <$T as NumCast>::from(f),
                    PValue::F32(f) => <$T as NumCast>::from(f),
                    PValue::F64(f) => <$T as NumCast>::from(f),
                    _ => None,
                }
                .ok_or_else(|| {
                    vortex_err!("Cannot read primitive value {:?} as {}", value, PType::$PT)
                })
            }
        }
    };
}

float_pvalue!(f32, F32);
float_pvalue!(f64, F64);

impl TryFrom<PValue> for f16 {
    type Error = VortexError;

    fn try_from(value: PValue) -> Result<Self, Self::Error> {
        // We serialize f16 as u16.
        match value {
            PValue::U16(u) => Some(f16::from_bits(u)),
            PValue::F32(f) => <f16 as NumCast>::from(f),
            PValue::F64(f) => <f16 as NumCast>::from(f),
            _ => None,
        }
        .ok_or_else(|| vortex_err!("Cannot read primitive value {:?} as {}", value, PType::F16))
    }
}

macro_rules! impl_pvalue {
    ($T:ty, $PT:tt) => {
        impl From<$T> for PValue {
            fn from(value: $T) -> Self {
                PValue::$PT(value)
            }
        }
    };
}

impl_pvalue!(u8, U8);
impl_pvalue!(u16, U16);
impl_pvalue!(u32, U32);
impl_pvalue!(u64, U64);
impl_pvalue!(i8, I8);
impl_pvalue!(i16, I16);
impl_pvalue!(i32, I32);
impl_pvalue!(i64, I64);
impl_pvalue!(f16, F16);
impl_pvalue!(f32, F32);
impl_pvalue!(f64, F64);
