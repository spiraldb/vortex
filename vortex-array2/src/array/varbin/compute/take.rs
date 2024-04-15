use arrow_buffer::NullBuffer;
use vortex::match_each_integer_ptype;
use vortex::ptype::NativePType;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::varbin::builder::VarBinBuilder;
use crate::array::varbin::{OwnedVarBinArray, VarBinArray};
use crate::compute::take::TakeFn;
use crate::validity::Validity;
use crate::IntoArray;
use crate::{Array, OwnedArray};

impl TakeFn for VarBinArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        // TODO(ngates): support i64 indices.
        assert!(
            indices.len() < i32::MAX as usize,
            "indices.len() must be less than i32::MAX"
        );

        let offsets = self.offsets().flatten_primitive()?;
        let data = self.bytes().flatten_primitive()?;
        let indices = indices.clone().flatten_primitive()?;
        match_each_integer_ptype!(offsets.ptype(), |$O| {
            match_each_integer_ptype!(indices.ptype(), |$I| {
                Ok(take(
                    self.dtype().clone(),
                    offsets.typed_data::<$O>(),
                    data.typed_data::<u8>(),
                    indices.typed_data::<$I>(),
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
) -> VortexResult<OwnedVarBinArray> {
    let logical_validity = validity.to_logical(offsets.len() - 1);
    if let Some(v) = logical_validity.to_null_buffer()? {
        return Ok(take_nullable(dtype, offsets, data, indices, v));
    }

    let mut builder = VarBinBuilder::<I>::with_capacity(indices.len());
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
) -> OwnedVarBinArray {
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
