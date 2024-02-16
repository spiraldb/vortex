use log::debug;

use codecz::ffor;
use codecz::ffor::{FforEncoded, SupportsFFoR};
use enc::array::primitive::PrimitiveArray;
use enc::array::{Array, ArrayRef};
use enc::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use enc::match_each_integer_ptype;
use enc::ptype::NativePType;
use enc::scalar::{ListScalarVec, Scalar};
use enc::stats::Stat;
use enc_patched::PatchedArray;

use crate::ffor::{FFORArray, FFoREncoding};

impl EncodingCompression for FFoREncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.as_any().downcast_ref::<PrimitiveArray>() else {
            debug!("Skipping FFoR: not primitive");
            return None;
        };

        // Only supports ints
        if !parray.ptype().is_int() {
            debug!("Skipping FFoR: only supports integer types");
            return None;
        }

        debug!("Compressing with FFoR");
        Some(&(ffor_compressor as Compressor))
    }
}

// FFOR and other bitpacking algorithms are essentially the "terminal"
// lightweight encodings for integers, as the output is essentially an array
// of opaque bytes. At that point, the only available schemes are general-purpose
// compression algorithms, which we would apply at the file level instead (if at all)
fn ffor_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let like_ffor = like.and_then(|like_array| like_array.as_any().downcast_ref::<FFORArray>());
    let parray = array.as_any().downcast_ref::<PrimitiveArray>().unwrap();

    let (ffor, idxs, values) = like_ffor
        .map(|ffor_like| ffor_encode_like(parray, ffor_like))
        .unwrap_or_else(|| ffor_encode_parts(parray));
    let compressed_idx = idxs.map(|idx| ctx.next_level().compress(idx.as_ref(), None));
    let compressed_values = values.map(|val| ctx.next_level().compress(val.as_ref(), None));

    if let Some((cidx, cvalues)) = compressed_idx.zip(compressed_values) {
        PatchedArray::new(ffor.boxed(), cidx, cvalues).boxed()
    } else {
        ffor.boxed()
    }
}

pub fn ffor_encode(parray: &PrimitiveArray) -> ArrayRef {
    let (ffor, idx, values) = ffor_encode_parts(parray);
    if let Some((pidx, pvalues)) = idx.zip(values) {
        PatchedArray::new(ffor.boxed(), pidx.boxed(), pvalues.boxed()).boxed()
    } else {
        ffor.boxed()
    }
}

pub fn ffor_encode_parts(
    parray: &PrimitiveArray,
) -> (FFORArray, Option<PrimitiveArray>, Option<PrimitiveArray>) {
    let min_val_scalar = parray.stats().get_or_compute(&Stat::Min).unwrap();
    let max_val_scalar = parray.stats().get_or_compute(&Stat::Max).unwrap();
    let bit_widths = parray
        .stats()
        .get_or_compute_as::<ListScalarVec<u64>>(&Stat::BitWidthFreq)
        .unwrap()
        .0;
    match_each_integer_ptype!(parray.ptype(), |$T| {
        let min_val: $T = min_val_scalar.as_ref().try_into().unwrap();
        let max_val: $T = max_val_scalar.as_ref().try_into().unwrap();
        let num_bits = codecz::ffor::find_best_bit_width::<$T>(bit_widths.as_slice(), min_val, max_val).unwrap();
        ffor_encode_primitive(parray.buffer().typed_data::<$T>(), parray.validity(), num_bits, min_val)
    })
}

fn ffor_encode_like(
    parray: &PrimitiveArray,
    sample: &FFORArray,
) -> (FFORArray, Option<PrimitiveArray>, Option<PrimitiveArray>) {
    let min_val_scalar = parray.stats().get_or_compute(&Stat::Min).unwrap();
    match_each_integer_ptype!(parray.ptype(), |$T| {
        let min_val: $T = min_val_scalar.as_ref().try_into().unwrap();
        ffor_encode_primitive(parray.buffer().typed_data::<$T>(), parray.validity(), sample.num_bits(), min_val)
    })
}

fn ffor_encode_primitive<T: SupportsFFoR + NativePType>(
    values: &[T],
    validity: Option<&ArrayRef>,
    num_bits: u8,
    min_val: T,
) -> (FFORArray, Option<PrimitiveArray>, Option<PrimitiveArray>)
where
    Box<dyn Scalar>: From<T>,
{
    // TODO: actually handle CodecErrors instead of blindly unwrapping
    let FforEncoded {
        buf,
        num_exceptions,
    } = ffor::encode::<T>(values, num_bits, min_val).unwrap();
    let bytes_array = PrimitiveArray::from_vec_in(buf);

    let ffor_array = FFORArray::try_from_parts(
        bytes_array.boxed(),
        validity.cloned(),
        min_val,
        num_bits,
        values.len(),
    )
    .unwrap();

    if num_exceptions == 0 {
        return (ffor_array, None, None);
    }

    let (patch_values, patch_indices) =
        ffor::collect_exceptions(values, num_bits, min_val, num_exceptions).unwrap();
    let patch_indices = codecz::utils::into_u32_vec(&patch_indices, num_exceptions);

    (
        ffor_array,
        Some(PrimitiveArray::from_vec_in(patch_indices)),
        Some(PrimitiveArray::from_vec_in(patch_values)),
    )
}
