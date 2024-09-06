use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{slice, ArrayCompute, SliceFn};
use vortex::{Array, IntoArray};
use vortex_dtype::PType;
use vortex_error::{vortex_bail, vortex_err, VortexResult, VortexUnwrap};
use vortex_scalar::{PrimitiveScalar, Scalar};
use zigzag::ZigZag as ExternalZigZag;

use crate::ZigZagArray;

impl ArrayCompute for ZigZagArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

impl ScalarAtFn for ZigZagArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let scalar = scalar_at_unchecked(&self.encoded(), index);
        if scalar.is_null() {
            return Ok(scalar);
        }

        let pscalar = PrimitiveScalar::try_from(&scalar)?;
        match pscalar.ptype() {
            PType::U8 => Ok(i8::decode(pscalar.typed_value::<u8>().ok_or_else(|| {
                vortex_err!(
                    "Cannot decode provided scalar: expected u8, got ptype {}",
                    pscalar.ptype()
                )
            })?)
            .into()),
            PType::U16 => Ok(i16::decode(pscalar.typed_value::<u16>().ok_or_else(|| {
                vortex_err!(
                    "Cannot decode provided scalar: expected u16, got ptype {}",
                    pscalar.ptype()
                )
            })?)
            .into()),
            PType::U32 => Ok(i32::decode(pscalar.typed_value::<u32>().ok_or_else(|| {
                vortex_err!(
                    "Cannot decode provided scalar: expected u32, got ptype {}",
                    pscalar.ptype()
                )
            })?)
            .into()),
            PType::U64 => Ok(i64::decode(pscalar.typed_value::<u64>().ok_or_else(|| {
                vortex_err!(
                    "Cannot decode provided scalar: expected u64, got ptype {}",
                    pscalar.ptype()
                )
            })?)
            .into()),
            _ => vortex_bail!(
                "ZigZag can only decode unsigned integers, got {}",
                pscalar.ptype()
            ),
        }
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).vortex_unwrap()
    }
}

impl SliceFn for ZigZagArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(slice(&self.encoded(), start, stop)?)?.into_array())
    }
}
