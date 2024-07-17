use arrow_buffer::NullBuffer;
use vortex_dtype::match_each_integer_ptype;
use vortex_dtype::DType;
use vortex_dtype::NativePType;
use vortex_error::VortexResult;

use crate::array::varbin::builder::VarBinBuilder;
use crate::array::varbin::VarBinArray;
use crate::compute::TakeFn;
use crate::validity::Validity;
use crate::Array;
use crate::IntoArray;
use crate::{ArrayDType, IntoArrayVariant};

impl TakeFn for VarBinArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        // TODO(ngates): support i64 indices.
        assert!(
            indices.len() < i32::MAX as usize,
            "indices.len() must be less than i32::MAX"
        );

        let offsets = self.offsets().into_primitive()?;
        let data = self.bytes().into_primitive()?;
        let indices = indices.clone().into_primitive()?;
        match_each_integer_ptype!(offsets.ptype(), |$O| {
            match_each_integer_ptype!(indices.ptype(), |$I| {
                Ok(take(
                    self.dtype().clone(),
                    offsets.maybe_null_slice::<$O>(),
                    data.maybe_null_slice::<u8>(),
                    indices.maybe_null_slice::<$I>(),
                    self.validity(),
                )?.into_array())
            })
        })
    }
}

fn take<I: NativePType, O: NativePType>(
    dtype: DType,
    offsets: &[O],
    data: &[u8],
    indices: &[I],
    validity: Validity,
) -> VortexResult<VarBinArray> {
    let logical_validity = validity.to_logical(offsets.len() - 1);
    if let Some(v) = logical_validity.to_null_buffer()? {
        return Ok(take_nullable(dtype, offsets, data, indices, v));
    }

    let mut builder = VarBinBuilder::<O>::with_capacity(indices.len());
    for &idx in indices {
        let idx = idx.to_usize().unwrap();
        let start = offsets[idx].to_usize().unwrap();
        let stop = offsets[idx + 1].to_usize().unwrap();
        builder.push(Some(&data[start..stop]));
    }
    Ok(builder.finish(dtype))
}

fn take_nullable<I: NativePType, O: NativePType>(
    dtype: DType,
    offsets: &[O],
    data: &[u8],
    indices: &[I],
    null_buffer: NullBuffer,
) -> VarBinArray {
    let mut builder = VarBinBuilder::<I>::with_capacity(indices.len());
    for &idx in indices {
        let idx = idx.to_usize().unwrap();
        if null_buffer.is_valid(idx) {
            let start = offsets[idx].to_usize().unwrap();
            let stop = offsets[idx + 1].to_usize().unwrap();
            builder.push(Some(&data[start..stop]));
        } else {
            builder.push(None);
        }
    }
    builder.finish(dtype)
}
