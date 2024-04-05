use itertools::Itertools;

use fastlanez::TryBitPack;
use vortex::array::{Array, ArrayRef};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::ArrayCompute;
use vortex::compute::flatten::{flatten_primitive, FlattenedArray, FlattenFn};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::{take, TakeFn};
use vortex::match_each_integer_ptype;
use vortex::ptype::NativePType;
use vortex::scalar::Scalar;
use vortex::validity::OwnedValidity;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

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
            return Err(vortex_err!(OutOfBounds: index, 0, self.len()));
        }
        if let Some(patches) = self.patches() {
            // NB: All non-null values are considered patches
            if self.bit_width == 0 || patches.is_valid(index) {
                return scalar_at(patches, index)?.cast(self.dtype());
            }
        }
        unpack_single(self, index)?.cast(self.dtype())
    }
}

impl TakeFn for BitPackedArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let indices = flatten_primitive(indices)?;
        let ptype = self.dtype().try_into()?;
        let taken_validity = self.validity().map(|v| v.take(&indices)).transpose()?;
        let taken = match_integers_by_width!(ptype, |$T| {
            PrimitiveArray::from_nullable(take_primitive::<$T>(self, &indices)?, taken_validity)
        });
        Ok(taken.reinterpret_cast(ptype).into_array())
    }
}

fn take_primitive<T: NativePType + TryBitPack>(
    array: &BitPackedArray,
    indices: &PrimitiveArray,
) -> VortexResult<Vec<T>> {
    // Group indices into 1024-element chunks and relativise them to the beginning of each chunk
    let relative_indices: Vec<(usize, Vec<u16>)> = match_each_integer_ptype!(indices.ptype(), |$P| {
        indices
            .typed_data::<$P>()
            .iter()
            .group_by(|idx| (**idx / 1024) as usize)
            .into_iter()
            .map(|(k, g)| (k, g.map(|idx| (*idx % 1024) as u16).collect()))
            .collect()
    });

    let bit_width = array.bit_width();
    let packed = flatten_primitive(array.encoded())?;
    let packed = packed.typed_data::<u8>();

    // assuming the buffer is already allocated (which will happen at most once)
    // then unpacking all 1024 elements takes ~8.8x as long as unpacking a single element
    // see https://github.com/fulcrum-so/vortex/pull/190#issue-2223752833
    // however, the gap should be smaller with larger registers (e.g., AVX-512) vs the 128 bit
    // ones on M2 Macbook Air.
    let bulk_threshold = 8;

    let mut output = Vec::with_capacity(indices.len());
    let mut buffer: Vec<T> = Vec::new();
    for (chunk, offsets) in relative_indices {
        let packed_chunk = &packed[chunk * 128 * bit_width..][..128 * bit_width];
        if offsets.len() > bulk_threshold {
            buffer.clear();
            TryBitPack::try_unpack_into(packed_chunk, bit_width, &mut buffer)
                .map_err(|_| vortex_err!("Unsupported bit width {}", bit_width))?;
            for index in offsets {
                output.push(buffer[index as usize]);
            }
        } else {
            for index in offsets {
                output.push(unsafe {
                    unpack_single_primitive::<T>(packed_chunk, bit_width, index as usize)?
                });
            }
        }
    }

    if let Some(patches) = array.patches() {
        let taken_patches = patches
            .maybe_sparse()
            .map(|patches| take(patches, indices))
            .transpose()?
            .ok_or(vortex_err!("Only sparse patches are currently supported!"))?;
        let taken_patches = taken_patches
            .maybe_sparse()
            .ok_or(vortex_err!("Only sparse patches are currently supported!"))?;

        let output_patches = flatten_primitive(taken_patches.values())?;
        taken_patches
            .resolved_indices()
            .iter()
            .zip_eq(output_patches.typed_data::<T>())
            .for_each(|(idx, val)| {
                output[*idx] = *val;
            });
    }
    Ok(output)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;
    use rand::{Rng, thread_rng};
    use rand::distributions::Uniform;

    use vortex::array::Array;
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
    use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
    use vortex::compute::scalar_at::scalar_at;
    use vortex::compute::take::take;
    use vortex::encoding::EncodingRef;
    use vortex::scalar::Scalar;

    use crate::BitPackedEncoding;
    use crate::downcast::DowncastFastlanes;

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

    #[test]
    fn take_random_indices() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let num_patches: usize = 128;
        let values = (0..u16::MAX as u32 + num_patches as u32).collect::<Vec<_>>();
        let uncompressed = PrimitiveArray::from(values.clone());
        let packed = BitPackedEncoding {}
            .compress(&uncompressed, None, ctx)
            .unwrap();
        let packed = packed.as_bitpacked();
        assert!(packed.patches().is_some());

        let patches = packed.patches().unwrap().as_sparse();
        assert_eq!(
            patches.resolved_indices(),
            ((values.len() + 1 - num_patches)..values.len()).collect_vec()
        );

        let rng = thread_rng();
        let range = Uniform::new(0, values.len());
        let random_indices: PrimitiveArray = rng
            .sample_iter(range)
            .take(10_000)
            .map(|i| i as u32)
            .collect_vec()
            .into();
        let taken = take(packed, &random_indices).unwrap();

        // sanity check
        random_indices
            .typed_data::<u32>()
            .iter()
            .enumerate()
            .for_each(|(ti, i)| {
                assert_eq!(
                    scalar_at(packed, *i as usize).unwrap(),
                    Scalar::from(values[*i as usize])
                );
                assert_eq!(
                    scalar_at(&taken, ti).unwrap(),
                    Scalar::from(values[*i as usize])
                );
            });
    }

    #[test]
    fn test_scalar_at() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let values = (0u32..257).collect_vec();
        let uncompressed = PrimitiveArray::from(values.clone()).into_array();
        let packed = BitPackedEncoding
            .compress(&uncompressed, None, ctx)
            .unwrap();
        let packed = packed.as_bitpacked();
        assert!(packed.patches().is_some());

        let patches = packed.patches().unwrap().as_sparse();
        assert_eq!(patches.resolved_indices(), vec![256]);

        values.iter().enumerate().for_each(|(i, v)| {
            assert_eq!(scalar_at(packed, i).unwrap(), Scalar::from(*v));
        });
    }
}
