use itertools::Itertools;

use fastlanez::TryBitPack;
use vortex::array::{Array, ArrayRef};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::ArrayCompute;
use vortex::compute::cast::cast;
use vortex::compute::flatten::{flatten_primitive, FlattenedArray, FlattenFn};
use vortex::compute::scalar_at::ScalarAtFn;
use vortex::compute::take::TakeFn;
use vortex::match_each_integer_ptype;
use vortex::ptype::NativePType;
use vortex::scalar::Scalar;
use vortex_error::{vortex_err, VortexResult};

use crate::{BitPackedArray, match_integers_by_width, unpack_single_primitive};
use crate::bitpacking::compress::{unpack, unpack_single};

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
        // TODO(wmanning): check patches/validity!
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
                .group_by(|idx| (**idx / 1024) as usize)
                .into_iter()
                .map(|(k, g)| (k, g.map(|idx| (*idx % 1024) as u16).collect()))
                .collect()
        });

        let ptype = self.dtype().try_into()?;
        let taken = match_integers_by_width!(ptype, |$P| {
            PrimitiveArray::from(take_primitive::<$P>(self, &relative_indices, prim_indices.len())?)
        });
        // TODO(wmanning): handle patches & validity
        // TODO(wmanning): this should be a reinterpret_cast
        cast(&taken, self.dtype())
    }
}

fn take_primitive<T: NativePType + TryBitPack>(
    array: &BitPackedArray,
    relative_indices: &[(usize, Vec<u16>)],
    size_hint: usize,
) -> VortexResult<Vec<T>> {
    let bit_width = array.bit_width();
    let packed = flatten_primitive(array.encoded())?;
    let packed = packed.typed_data::<u8>();

    // assuming the buffer is already allocated (which will happen at most once)
    // then unpacking all 1024 elements takes ~8.8x as long as unpacking a single element
    // see https://github.com/fulcrum-so/vortex/pull/190#issue-2223752833
    // however, the gap should be smaller with larger registers (e.g., AVX-512) vs the 128 bit
    // ones on M2 Macbook Air.
    let bulk_threshold = 8;

    let mut output = Vec::with_capacity(size_hint);
    let mut buffer: Vec<T> = Vec::new();
    for (chunk, offsets) in relative_indices {
        let packed_chunk = &packed[chunk * 128 * bit_width..][..128 * bit_width];
        if offsets.len() > bulk_threshold {
            buffer.clear();
            TryBitPack::try_unpack_into(packed_chunk, bit_width, &mut buffer)
                .map_err(|_| vortex_err!("Unsupported bit width {}", bit_width))?;
            for index in offsets {
                output.push(buffer[*index as usize]);
            }
        } else {
            for index in offsets {
                output.push(unsafe {
                    unpack_single_primitive::<T>(packed_chunk, bit_width, *index as usize)?
                });
            }
        }
    }
    Ok(output)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::Array;
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
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
