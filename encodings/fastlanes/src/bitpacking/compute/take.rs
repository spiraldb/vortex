use std::collections::HashMap;

use fastlanes::BitPacking;
use itertools::Itertools;
use num_traits::PrimInt;
use vortex::array::PrimitiveArray;
use vortex::compute::{search_sorted, take, SearchSortedSide, TakeFn};
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_dtype::{
    match_each_integer_ptype, match_each_unsigned_integer_ptype, NativePType, PType,
};
use vortex_error::{vortex_err, VortexResult};

use crate::{unpack_single_primitive, BitPackedArray};

impl TakeFn for BitPackedArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let ptype: PType = self.dtype().try_into()?;
        let validity = self.validity();
        let taken_validity = validity.take(indices)?;

        let indices = indices.clone().into_primitive()?;
        let taken = match_each_unsigned_integer_ptype!(ptype, |$T| {
            PrimitiveArray::from_vec(take_primitive::<$T>(self, &indices)?, taken_validity)
        });
        Ok(taken.reinterpret_cast(ptype).into_array())
    }
}

fn take_primitive<T: NativePType + BitPacking>(
    array: &BitPackedArray,
    indices: &PrimitiveArray,
) -> VortexResult<Vec<T>> {
    // Group indices into 1024-element chunks and relativise them to the beginning of each chunk
    let relative_indices: Vec<(usize, Vec<u16>)> = match_each_integer_ptype!(indices.ptype(), |$P| {
        indices
            .maybe_null_slice::<$P>()
            .iter()
            .map(|i| *i as usize + array.offset())
            .chunk_by(|idx| idx / 1024)
            .into_iter()
            .map(|(k, g)| (k, g.map(|idx| (idx % 1024) as u16).collect()))
            .collect()
    });

    let bit_width = array.bit_width();

    let packed = array.packed_slice::<T>();

    let patches = array._patches();

    // if we have a small number of relatively large batches, we gain by slicing and then patching inside the loop
    // if we have a large number of relatively small batches, the overhead isn't worth it, and we're better off with a bulk patch
    // roughly, if we have an average of less than 64 elements per batch, we prefer bulk patching

    // FIXME(DK): restore this
    // let prefer_bulk_patch = relative_indices.len() * 64 > indices.len();

    // assuming the buffer is already allocated (which will happen at most once)
    // then unpacking all 1024 elements takes ~8.8x as long as unpacking a single element
    // see https://github.com/fulcrum-so/vortex/pull/190#issue-2223752833
    // however, the gap should be smaller with larger registers (e.g., AVX-512) vs the 128 bit
    // ones on M2 Macbook Air.
    let unpack_chunk_threshold = 8;

    let mut output = Vec::with_capacity(indices.len());
    let mut unpacked = [T::zero(); 1024];
    for (chunk, offsets) in relative_indices {
        let chunk_size = 128 * bit_width / size_of::<T>();
        let packed_chunk = &packed[chunk * chunk_size..][..chunk_size];
        if offsets.len() > unpack_chunk_threshold {
            unsafe {
                BitPacking::unchecked_unpack(bit_width, packed_chunk, &mut unpacked);
            }
            for index in &offsets {
                output.push(unpacked[*index as usize]);
            }
        } else {
            for index in &offsets {
                output.push(unsafe {
                    unpack_single_primitive::<T>(packed_chunk, bit_width, *index as usize)
                });
            }
        }

        // if !prefer_bulk_patch {
        //     if let Some(ref patches) = patches {
        //         // NOTE: we need to subtract the array offset before slicing into the patches.
        //         // This is because BitPackedArray is rounded to block boundaries, but patches
        //         // is sliced exactly.
        //         let patches_start = if chunk == 0 {
        //             0
        //         } else {
        //             (chunk * 1024) - array.offset() - self.packed_len()
        //         };
        //         let patches_end = min(
        //             (chunk + 1) * 1024 - array.offset() - self.packed_len(),
        //             patches.len(),
        //         );
        //         let patches_slice = slice(patches.as_ref(), patches_start, patches_end)?;
        //         let patches_slice = SparseArray::try_from(patches_slice)?;
        //         let offsets = PrimitiveArray::from(offsets);
        //         do_patch_for_take_primitive(&patches_slice, &offsets, &mut output)?;
        //     }
        // }
    }

    // if prefer_bulk_patch {
    if let Some((patch_indices, patch_values, _)) = patches {
        do_patch_for_take_primitive(
            patch_indices,
            patch_values,
            indices.clone().into_array(),
            &mut output,
        )?;
    }
    // }

    Ok(output)
}

fn do_patch_for_take_primitive<T: NativePType>(
    patch_indices: Array,
    patch_values: Array,
    indices: Array,
    output: &mut [T],
) -> VortexResult<()> {
    // println!(
    //     "do_patch {:?}",
    //     (
    //         patch_indices.as_primitive().maybe_null_slice::<u64>(),
    //         patch_values.as_primitive().maybe_null_slice::<u32>(),
    //         indices.as_primitive().maybe_null_slice::<u32>()
    //     )
    // );
    let (indices_of_kept_patches, indices_of_kept_indices) =
        kept_indices(patch_indices.clone(), indices)?;
    let kept_patch_indices =
        take(patch_indices, indices_of_kept_patches.clone())?.into_primitive()?;
    let kept_patch_values = take(patch_values, indices_of_kept_patches)?.into_primitive()?;

    let _kept_patch_indices = kept_patch_indices.maybe_null_slice::<u64>();
    let kept_patch_values = kept_patch_values.maybe_null_slice::<T>();
    for (index, value) in indices_of_kept_indices.iter().zip(kept_patch_values) {
        output[*index] = *value;
    }

    Ok(())
}

fn kept_indices(values: Array, filter_values: Array) -> VortexResult<(Array, Vec<usize>)> {
    let filter_values = filter_values.into_primitive()?;
    if filter_values.len() > 128 {
        let values = values.into_primitive()?;
        match_each_integer_ptype!(filter_values.ptype(), |$P| {
            kept_indices_by_map::<$P>(values, filter_values)
        })
    } else {
        match_each_integer_ptype!(filter_values.ptype(), |$P| {
            kept_indices_by_search_sorted::<$P>(values, filter_values)
        })
    }
}

fn kept_indices_by_map<T: PrimInt + NativePType>(
    values: PrimitiveArray,
    filter_values: PrimitiveArray,
) -> VortexResult<(Array, Vec<usize>)> {
    let values_to_index: HashMap<u64, u64> = values
        .maybe_null_slice::<u64>()
        .iter()
        .enumerate()
        .map(|(index, r)| (*r, index as u64))
        .collect();
    let mut kept_values_indices = Vec::new();
    let mut kept_filter_values_indices = Vec::new();
    for (filter_values_index, filter_value) in
        filter_values.maybe_null_slice::<T>().iter().enumerate()
    {
        let filter_value_u64 = filter_value
            .to_u64()
            .ok_or(vortex_err!("could not convert {} to u64", filter_value))?;
        match values_to_index.get(&filter_value_u64) {
            None => {}
            Some(values_index) => {
                kept_values_indices.push(*values_index);
                kept_filter_values_indices.push(filter_values_index);
            }
        }
    }
    let maybe_invalid_kept_indices =
        PrimitiveArray::from_vec::<u64>(kept_values_indices, Validity::NonNullable);
    Ok((
        PrimitiveArray::new(
            maybe_invalid_kept_indices.buffer().clone(),
            maybe_invalid_kept_indices.ptype(),
            values
                .validity()
                .take(&maybe_invalid_kept_indices.into_array())?,
        )
        .into_array(),
        kept_filter_values_indices,
    ))
}

fn kept_indices_by_search_sorted<T: PrimInt + NativePType>(
    values: Array,
    filter_values: PrimitiveArray,
) -> VortexResult<(Array, Vec<usize>)> {
    let mut kept_values_indices_vec = Vec::new();
    let mut kept_indices_indices_vec = Vec::new();
    for (filter_values_index, filter_value) in
        filter_values.maybe_null_slice::<T>().iter().enumerate()
    {
        let filter_value_u64 = filter_value
            .to_u64()
            .ok_or(vortex_err!("could not convert {} to u64", filter_value))?;
        let results = search_sorted(&values, filter_value_u64, SearchSortedSide::Left)?;
        match results.to_found() {
            None => {}
            Some(values_index) => {
                kept_values_indices_vec.push(values_index as u64);
                kept_indices_indices_vec.push(filter_values_index);
            }
        }
    }

    let maybe_invalid_kept_indices =
        PrimitiveArray::from_vec::<u64>(kept_values_indices_vec, Validity::NonNullable);

    let values = values.into_primitive()?;
    Ok((
        PrimitiveArray::new(
            maybe_invalid_kept_indices.buffer().clone(),
            maybe_invalid_kept_indices.ptype(),
            values
                .validity()
                .take(&maybe_invalid_kept_indices.into_array())?,
        )
        .into_array(),
        kept_indices_indices_vec,
    ))
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use rand::distributions::Uniform;
    use rand::{thread_rng, Rng};
    use vortex::array::PrimitiveArray;
    use vortex::compute::unary::scalar_at;
    use vortex::compute::{slice, take};
    use vortex::{IntoArray, IntoArrayVariant};

    use crate::BitPackedArray;

    #[test]
    fn take_indices() {
        let indices = PrimitiveArray::from(vec![0, 125, 2047, 2049, 2151, 2790]).into_array();

        // Create a u8 array modulo 63.
        let unpacked = PrimitiveArray::from((0..4096).map(|i| (i % 63) as u8).collect::<Vec<_>>());
        let bitpacked = BitPackedArray::encode(unpacked.as_ref(), 6).unwrap();

        let primitive_result = take(bitpacked.as_ref(), &indices)
            .unwrap()
            .into_primitive()
            .unwrap();
        let res_bytes = primitive_result.maybe_null_slice::<u8>();
        assert_eq!(res_bytes, &[0, 62, 31, 33, 9, 18]);
    }

    #[test]
    fn take_sliced_indices() {
        let indices = PrimitiveArray::from(vec![1919, 1921]).into_array();

        // Create a u8 array modulo 63.
        let unpacked = PrimitiveArray::from((0..4096).map(|i| (i % 63) as u8).collect::<Vec<_>>());
        let bitpacked = BitPackedArray::encode(unpacked.as_ref(), 6).unwrap();
        let sliced = slice(bitpacked.as_ref(), 128, 2050).unwrap();

        let primitive_result = take(&sliced, &indices).unwrap().into_primitive().unwrap();
        let res_bytes = primitive_result.maybe_null_slice::<u8>();
        assert_eq!(res_bytes, &[31, 33]);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // This test is too slow on miri
    fn take_random_indices() {
        let num_patches = 128;
        let values = (0..u16::MAX as u32 + num_patches as u32).collect::<Vec<_>>();
        let uncompressed = PrimitiveArray::from(values.clone());
        let packed = BitPackedArray::encode(uncompressed.as_ref(), 16).unwrap();
        assert!(packed._patches().is_some());

        let patches_indices = packed._patches().unwrap().0;
        assert_eq!(
            patches_indices.as_primitive().maybe_null_slice::<u64>(),
            ((values.len() as u64 + 1 - num_patches)..(values.len() as u64)).collect_vec()
        );

        let rng = thread_rng();
        let range = Uniform::new(0, values.len());
        let random_indices: PrimitiveArray = rng
            .sample_iter(range)
            .take(10_000)
            .map(|i| i as u32)
            .collect_vec()
            .into();
        let taken = take(packed.as_ref(), random_indices.as_ref()).unwrap();

        // sanity check
        random_indices
            .maybe_null_slice::<u32>()
            .iter()
            .enumerate()
            .for_each(|(ti, i)| {
                assert_eq!(
                    u32::try_from(scalar_at(packed.as_ref(), *i as usize).unwrap().as_ref())
                        .unwrap(),
                    values[*i as usize]
                );
                assert_eq!(
                    u32::try_from(scalar_at(&taken, ti).unwrap().as_ref()).unwrap(),
                    values[*i as usize]
                );
            });
    }

    #[test]
    fn test_scalar_at() {
        let values = (0u32..257).collect_vec();
        let uncompressed = PrimitiveArray::from(values.clone()).into_array();
        let packed = BitPackedArray::encode(&uncompressed, 8).unwrap();
        assert!(packed._patches().is_some());

        let patches_indices = packed._patches().unwrap().0;
        assert_eq!(
            patches_indices.as_primitive().maybe_null_slice::<u64>(),
            vec![256]
        );

        values.iter().enumerate().for_each(|(i, v)| {
            assert_eq!(
                u32::try_from(scalar_at(packed.as_ref(), i).unwrap().as_ref()).unwrap(),
                *v
            );
        });
    }
}
