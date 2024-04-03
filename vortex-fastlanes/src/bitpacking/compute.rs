use fastlanez::TryBitPack;
use itertools::Itertools;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compute::cast::cast;
use vortex::compute::flatten::{flatten_primitive, FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::ScalarAtFn;
use vortex::compute::take::TakeFn;
use vortex::compute::ArrayCompute;
use vortex::match_each_integer_ptype;
use vortex::ptype::NativePType;
use vortex::scalar::Scalar;
use vortex_error::{vortex_err, VortexResult};

use crate::bitpacking::compress::{unpack, unpack_single};
use crate::downcast::DowncastFastlanes;
use crate::{match_integers_by_width, unpack_single_primitive, BitPackedArray};

impl ArrayCompute for BitPackedArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for BitPackedArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        unpack(self).map(FlattenedArray::Primitive)
    }
}

impl ScalarAtFn for BitPackedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if index >= self.len() {
            return Err(vortex_err!(OutOfBounds:index, 0, self.len()));
        }
        if self.bit_width() == 0 {
            let ptype = self.dtype().try_into()?;
            match_each_integer_ptype!(&ptype, |$P| {
                return Ok(Scalar::from(0 as $P));
            })
        }
        unpack_single(self, index)
    }
}

impl TakeFn for BitPackedArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let prim_indices = flatten_primitive(indices)?;

        // Group indices into 1024-element chunks and relativise them to the beginning of each chunk
        let relative_indices: Vec<(usize, Vec<u16>)> = match_each_integer_ptype!(prim_indices.ptype(), |$P| {
            prim_indices
                .typed_data::<$P>()
                .iter()
                .sorted()
                .group_by(|idx| (**idx / 1024) as usize)
                .into_iter()
                .map(|(k, g)| (k, g.map(|idx| (*idx % 1024) as u16).collect()))
                .collect()
        });

        let ptype = self.dtype().try_into()?;
        let taken = match_integers_by_width!(ptype, |$P| {
            PrimitiveArray::from(take_primitive::<$P>(self, &relative_indices, prim_indices.len())?)
        });
        // TODO(wmanning): this should be a reinterpret_cast
        cast(&taken, self.dtype())
    }
}

fn take_primitive<T: NativePType + TryBitPack>(
    array: &BitPackedArray,
    relative_indices: &[(usize, Vec<u16>)],
    size_hint: usize,
) -> VortexResult<Vec<T>> {
    let mut output = Vec::with_capacity(size_hint);
    for (chunk, offsets) in relative_indices {
        let sliced = array.slice(chunk * 1024, (chunk + 1) * 1024).unwrap();
        let sliced = sliced.as_bitpacked();
        let packed = flatten_primitive(sliced.encoded()).unwrap();
        let packed = packed.typed_data::<u8>();

        // TODO(wmanning): if offsets.len() over threshold, unpack the whole thing
        for index in offsets {
            output.push(unsafe {
                unpack_single_primitive::<T>(packed, sliced.bit_width(), *index as usize).unwrap();
            });
        }
    }
    Ok(output)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
    use vortex::array::Array;
    use vortex::compress::{CompressConfig, CompressCtx};
    use vortex::compute::take::take;
    use vortex::encoding::EncodingRef;

    use crate::BitPackedEncoding;

    #[test]
    fn take_indices() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let indices = PrimitiveArray::from(vec![0, 125, 2047, 2049, 2151, 2790]);
        let unpacked = PrimitiveArray::from((0..4096).map(|i| (i % 63) as u8).collect::<Vec<_>>());
        let bitpacked = ctx.compress(&unpacked, None).unwrap();
        let result = take(&bitpacked, &indices).unwrap();
        assert_eq!(result.encoding().id(), PrimitiveEncoding::ID);
        let res_bytes = result.as_primitive().typed_data::<u8>();
        assert_eq!(res_bytes, &[0, 62, 31, 33, 9, 18]);
    }
}
