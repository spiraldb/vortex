use crate::ffor::{FFORArray, FFoREncoding};
use codecz::ffor::{FforEncoded, SupportsFFoR};
use enc::array::primitive::PrimitiveArray;
use enc::array::{Array, ArrayRef, Encoding};
use enc::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};

use enc::match_each_integer_ptype;
use enc::ptype::NativePType;
use enc::scalar::{ListScalarVec, Scalar};
use enc::stats::Stat;
use enc_patched::PatchedArray;
use log::info;

impl EncodingCompression for FFoREncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            info!("Skipping FFoR: disabled");
            return None;
        }

        // Only support primitive arrays
        let Some(parray) = array.as_any().downcast_ref::<PrimitiveArray>() else {
            info!("Skipping FFoR: not primitive");
            return None;
        };

        // Only supports ints
        if !parray.ptype().is_int() {
            info!("Skipping FFoR: only supports integer types");
            return None;
        }

        info!("Compressing with FFoR");
        Some(&(ffor_compressor as Compressor))
    }
}

fn ffor_compressor(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
    ffor_encode(array.as_any().downcast_ref::<PrimitiveArray>().unwrap())
}

pub fn ffor_encode(parray: &PrimitiveArray) -> ArrayRef {
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
        ffor_encode_primitive(parray.buffer().typed_data::<$T>(), num_bits, min_val)
    })
}

fn ffor_encode_primitive<T: SupportsFFoR + NativePType>(
    values: &[T],
    num_bits: u8,
    min_val: T,
) -> ArrayRef
where
    Box<dyn Scalar>: From<T>,
{
    // TODO: actually handle CodecErrors instead of blindly unwrapping
    let FforEncoded {
        buf,
        num_exceptions,
    } = codecz::ffor::encode::<T>(values, num_bits, min_val).unwrap();
    let bytes_array = PrimitiveArray::from_vec_in(buf);

    let ffor_array =
        FFORArray::try_from_parts(bytes_array.boxed(), min_val, num_bits, values.len())
            .unwrap()
            .boxed();

    if num_exceptions == 0 {
        return ffor_array;
    }

    let (patch_values, patch_indices) =
        codecz::ffor::collect_exceptions(values, num_bits, min_val, num_exceptions).unwrap();
    let patch_indices = codecz::utils::into_u32_vec(&patch_indices, num_exceptions);

    PatchedArray::try_new(
        ffor_array,
        PrimitiveArray::from_vec_in(patch_indices).boxed(),
        PrimitiveArray::from_vec_in(patch_values).boxed(),
    )
    .unwrap()
    .boxed()
}
