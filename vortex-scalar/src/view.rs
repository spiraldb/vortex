#![allow(dead_code)]
use flexbuffers::FlexBufferType;
use num_traits::FromPrimitive;
use paste::paste;
use vortex_buffer::Buffer;
use vortex_dtype::half::f16;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError};

pub struct ScalarViewBuffer(Buffer);

impl ScalarViewBuffer {
    pub fn into_scalar_view(self, dtype: DType) -> ScalarView {
        ScalarView {
            dtype,
            buffer: self,
        }
    }

    pub(crate) fn flexbuffer(&self) -> flexbuffers::Reader<&[u8]> {
        flexbuffers::Reader::get_root(self.0.as_ref())
            .expect("Should have been validated on construction")
    }
}

impl TryFrom<Buffer> for ScalarViewBuffer {
    type Error = VortexError;

    fn try_from(value: Buffer) -> Result<Self, Self::Error> {
        // Ensure that the buffer is a valid flexbuffer
        let _ = flexbuffers::Reader::get_root(value.as_ref())?;
        Ok(Self(value))
    }
}

pub struct ScalarView {
    dtype: DType,
    buffer: ScalarViewBuffer,
}

impl ScalarView {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }
}

macro_rules! scalar_view_try_from {
    ($T:ty) => {
        paste! {
            impl TryFrom<ScalarView> for Option<$T> {
                type Error = VortexError;

                fn try_from(value: ScalarView) -> Result<Self, Self::Error> {
                    if !value.dtype().is_nullable() {
                        vortex_bail!(
                            "Cannot convert non-nullable scalar {} into Option<T>",
                            value.dtype()
                        );
                    }
                    let fb = value.buffer.flexbuffer();
                    if fb.flexbuffer_type() == FlexBufferType::Null {
                        return Ok(None);
                    }
                    return Ok(Some(fb.[<as_ $T>]().into()));
                }
            }

            impl TryFrom<ScalarView> for $T {
                type Error = VortexError;

                fn try_from(value: ScalarView) -> Result<Self, Self::Error> {
                    if value.dtype().is_nullable() {
                        vortex_bail!(
                            "Can only convert nullable scalar {} into Option<T>",
                            value.dtype()
                        );
                    }
                    Ok(value.buffer.flexbuffer().[<as_ $T>]().into())
                }
            }
        }
    };
}

scalar_view_try_from!(bool);
scalar_view_try_from!(u8);
scalar_view_try_from!(u16);
scalar_view_try_from!(u32);
scalar_view_try_from!(u64);
scalar_view_try_from!(i8);
scalar_view_try_from!(i16);
scalar_view_try_from!(i32);
scalar_view_try_from!(i64);
scalar_view_try_from!(f32);
scalar_view_try_from!(f64);

impl TryFrom<ScalarView> for Option<f16> {
    type Error = VortexError;

    fn try_from(value: ScalarView) -> Result<Self, Self::Error> {
        Option::<f32>::try_from(value).map(|f| f.map(f16::from_f32))
    }
}

impl TryFrom<ScalarView> for f16 {
    type Error = VortexError;

    fn try_from(value: ScalarView) -> Result<Self, Self::Error> {
        f32::try_from(value).map(f16::from_f32)
    }
}

impl TryFrom<ScalarView> for Option<usize> {
    type Error = VortexError;

    fn try_from(value: ScalarView) -> Result<Self, Self::Error> {
        Option::<u64>::try_from(value).map(|f| f.and_then(usize::from_u64))
    }
}

impl TryFrom<ScalarView> for usize {
    type Error = VortexError;

    fn try_from(value: ScalarView) -> Result<Self, Self::Error> {
        u64::try_from(value).and_then(|u| usize::from_u64(u).ok_or(vortex_err!("usize overflow")))
    }
}
