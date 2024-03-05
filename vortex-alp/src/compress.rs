use log::debug;

use crate::alp::ALPFloat;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::error::{VortexError, VortexResult};
use vortex::ptype::PType;

use crate::array::{ALPArray, ALPEncoding};
use crate::downcast::DowncastALP;

impl EncodingCompression for ALPEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping ALP: not primitive");
            return None;
        };

        // Only supports f32 and f64
        if !matches!(parray.ptype(), PType::F32 | PType::F64) {
            debug!("Skipping ALP: only supports f32 and f64");
            return None;
        }

        Some(&(alp_compressor as Compressor))
    }
}

fn alp_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let like_alp = like.map(|like_array| like_array.as_alp());

    let mut parray = array.as_primitive().clone();
    if parray.validity().is_some() {
        parray = compute::

    }

    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => {
            ALPFloat::encode_to_array(parray.typed_data::<f32>(), like_alp.map(|a| a.exponents()))
        }
        PType::F64 => {
            ALPFloat::encode_to_array(parray.typed_data::<f64>(), like_alp.map(|a| a.exponents()))
        }
        _ => panic!("Unsupported ptype"),
    };

    let compressed_encoded = ctx
        .next_level()
        .compress(encoded.as_ref(), like_alp.map(|a| a.encoded()));

    let compressed_patches = patches.map(|p| {
        ctx.next_level()
            .compress(p.as_ref(), like_alp.and_then(|a| a.patches()))
    });

    ALPArray::new(compressed_encoded, exponents, compressed_patches).boxed()
}

pub fn alp_encode(parray: &PrimitiveArray) -> VortexResult<ALPArray> {
    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => ALPFloat::encode_to_array(parray.typed_data::<f32>(), None),
        PType::F64 => ALPFloat::encode_to_array(parray.typed_data::<f64>(), None),
        _ => return Err(VortexError::InvalidPType(parray.ptype().clone())),
    };
    Ok(ALPArray::new(encoded, exponents, patches))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::alp::Exponents;

    #[test]
    fn test_compress() {
        let array = PrimitiveArray::from_vec(vec![1.234f32; 1025]);
        let encoded = alp_encode(&array).unwrap();
        println!("Encoded {:?}", encoded);
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().typed_data::<i32>(),
            vec![1234; 1025]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });
    }

    #[test]
    fn test_nullable_compress() {
        let array = PrimitiveArray::from_iter(vec![None, Some(1.234f32), None]);
        let encoded = alp_encode(&array).unwrap();
        println!("Encoded {:?}", encoded);
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().typed_data::<i32>(),
            vec![0, 1234, 1234]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });
    }
}
