use log::info;

use codecz::alp::{ALPExponents, SupportsALP};
use enc::array::primitive::PrimitiveArray;
use enc::array::{Array, ArrayRef, Encoding};
use enc::compress::{
    ArrayCompression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};
use enc::ptype::{NativePType, PType};
use enc_patched::PatchedArray;

use crate::alp::{ALPArray, ALPEncoding};
use crate::helpers;

impl ArrayCompression for ALPArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        // Recursively compress the inner encoded array.
        ALPArray::try_new(ctx.compress(self.encoded()), self.exponents())
            .unwrap()
            .boxed()
    }
}

impl EncodingCompression for ALPEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            info!("Skipping ALP: disabled");
            return None;
        }

        // Only support primitive arrays
        let Some(parray) = array.as_any().downcast_ref::<PrimitiveArray>() else {
            info!("Skipping ALP: not primitive");
            return None;
        };

        // Only supports f32 and f64
        if !matches!(parray.ptype(), PType::F32 | PType::F64) {
            info!("Skipping ALP: unsupported ptype");
            return None;
        }

        info!("Compressing with ALP");
        Some(&(alp_compressor as Compressor))
    }
}

fn alp_compressor(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
    alp_encode(array.as_any().downcast_ref::<PrimitiveArray>().unwrap())
}

pub fn alp_encode(parray: &PrimitiveArray) -> ArrayRef {
    match parray.ptype() {
        PType::F32 => alp_encode_primitive(parray.buffer().typed_data::<f32>()),
        PType::F64 => alp_encode_primitive(parray.buffer().typed_data::<f64>()),
        _ => panic!("Unsupported ptype"),
    }
}

fn alp_encode_primitive<T: SupportsALP + NativePType>(values: &[T]) -> ArrayRef
where
    T::EncInt: NativePType,
{
    // TODO: actually handle CodecErrors instead of blindly unwrapping
    let encoded = codecz::alp::encode(values).unwrap();
    let values_array = PrimitiveArray::from_vec_in(encoded.values);
    let alp_array = ALPArray::try_new(values_array.boxed(), encoded.exponents)
        .unwrap()
        .boxed();

    if encoded.num_exceptions == 0 {
        return alp_array;
    }

    let patch_indices = helpers::into_u32_vec(&encoded.exceptions_idx, encoded.num_exceptions);
    let patch_values = helpers::gather_patches(values, patch_indices.as_slice());
    PatchedArray::try_new(
        alp_array,
        PrimitiveArray::from_vec_in(patch_indices).boxed(),
        PrimitiveArray::from_vec_in(patch_values).boxed(),
    )
    .unwrap()
    .boxed()
}

pub fn alp_decode(parray: &PrimitiveArray, exp: ALPExponents) -> PrimitiveArray {
    match parray.ptype() {
        PType::I32 => PrimitiveArray::from_vec_in(
            codecz::alp::decode::<f32>(parray.buffer().typed_data::<i32>(), exp).unwrap(),
        ),
        PType::I64 => PrimitiveArray::from_vec_in(
            codecz::alp::decode::<f64>(parray.buffer().typed_data::<i64>(), exp).unwrap(),
        ),
        _ => panic!("Unsupported ptype"),
    }
}
