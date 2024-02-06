use crate::alp::{ALPArray, ALPEncoding};
use crate::helpers;
use enc::array::primitive::PrimitiveArray;
use enc::array::{Array, ArrayKind, ArrayRef, Encoding};
use enc::compress::{
    ArrayCompression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};
use enc::ptype::{NativePType, PType};

use codecz::alp::{ALPExponents, SupportsALP};
use enc_patched::PatchedArray;

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
            return None;
        }

        // Only support primitive arrays
        let Some(parray) = array.as_any().downcast_ref::<PrimitiveArray>() else {
            return None;
        };

        // Only supports f32 and f64
        if !matches!(parray.ptype(), PType::F32 | PType::F64) {
            return None;
        }

        Some(&(alp_compressor as Compressor))
    }
}

fn alp_compressor(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
    let (encoded, exp) = match ArrayKind::from(array) {
        ArrayKind::Primitive(p) => alp_encode(p),
        _ => panic!("Compress more arrays"),
    };
    ALPArray::try_new(encoded, exp).unwrap().boxed()
}

pub fn alp_encode(parray: &PrimitiveArray) -> (ArrayRef, ALPExponents) {
    match parray.ptype() {
        PType::F32 => alp_encode_primitive(parray.buffer().typed_data::<f32>()),
        PType::F64 => alp_encode_primitive(parray.buffer().typed_data::<f64>()),
        _ => panic!("Unsupported ptype"),
    }
}

fn alp_encode_primitive<T: SupportsALP + NativePType>(values: &[T]) -> (ArrayRef, ALPExponents)
where
    T::EncInt: NativePType,
{
    // TODO: actually handle CodecErrors instead of blindly unwrapping
    let encoded = codecz::alp::encode(values).unwrap();
    let values_array = PrimitiveArray::from_vec_in(encoded.values);
    if encoded.num_exceptions == 0 {
        (values_array.boxed(), encoded.exponents)
    } else {
        let patch_indices = helpers::into_u32_vec(&encoded.exceptions_idx, encoded.num_exceptions);
        let patch_values = helpers::gather_patches(values, patch_indices.as_slice());
        let patched = PatchedArray::try_new(
            values_array.boxed(),
            PrimitiveArray::from_vec_in(patch_indices).boxed(),
            PrimitiveArray::from_vec_in(patch_values).boxed(),
        );
        (patched.unwrap().boxed(), encoded.exponents)
    }
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
