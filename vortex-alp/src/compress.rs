use itertools::Itertools;

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::CloneOptionalArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::flatten::flatten_primitive;
use vortex::compute::patch::PatchFn;
use vortex::error::{VortexError, VortexResult};
use vortex::ptype::{NativePType, PType};

use crate::alp::ALPFloat;
use crate::array::{ALPArray, ALPEncoding};
use crate::downcast::DowncastALP;
use crate::{match_each_alp_float_ptype, Exponents};

impl EncodingCompression for ALPEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = array.maybe_primitive()?;

        // Only supports f32 and f64
        if !matches!(parray.ptype(), PType::F32 | PType::F64) {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let like_alp = like.map(|like_array| like_array.as_alp());

        // TODO(ngates): fill forward nulls
        let parray = array.as_primitive();

        let (exponents, encoded, patches) = match parray.ptype() {
            PType::F32 => {
                encode_to_array(parray.typed_data::<f32>(), like_alp.map(|l| l.exponents()))
            }
            PType::F64 => {
                encode_to_array(parray.typed_data::<f64>(), like_alp.map(|l| l.exponents()))
            }
            _ => panic!("Unsupported ptype"),
        };

        let compressed_encoded = ctx
            .named("packed")
            .excluding(&ALPEncoding::ID)
            .compress(encoded.as_ref(), like_alp.map(|a| a.encoded()))?;

        let compressed_patches = patches
            .map(|p| {
                ctx.auxiliary("patches")
                    .excluding(&ALPEncoding::ID)
                    .compress(p.as_ref(), like_alp.and_then(|a| a.patches()))
            })
            .transpose()?;

        Ok(ALPArray::new(compressed_encoded, exponents, compressed_patches).boxed())
    }
}

fn encode_to_array<T>(
    values: &[T],
    exponents: Option<&Exponents>,
) -> (Exponents, ArrayRef, Option<ArrayRef>)
where
    T: ALPFloat + NativePType,
    T::ALPInt: NativePType,
{
    let (exponents, values, exc_pos, exc) = T::encode(values, exponents);
    let len = values.len();
    (
        exponents,
        PrimitiveArray::from(values).boxed(),
        (!exc.is_empty()).then(|| {
            SparseArray::new(
                PrimitiveArray::from(exc_pos).boxed(),
                PrimitiveArray::from(exc).boxed(),
                len,
            )
            .boxed()
        }),
    )
}

pub(crate) fn alp_encode(parray: &PrimitiveArray) -> VortexResult<ALPArray> {
    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => encode_to_array(parray.typed_data::<f32>(), None),
        PType::F64 => encode_to_array(parray.typed_data::<f64>(), None),
        _ => return Err(VortexError::InvalidPType(*parray.ptype())),
    };
    Ok(ALPArray::new(encoded, exponents, patches))
}

pub fn decompress(array: &ALPArray) -> VortexResult<PrimitiveArray> {
    let encoded = flatten_primitive(array.encoded())?;
    let decoded = match_each_alp_float_ptype!(*encoded.ptype(), |$T| {
        PrimitiveArray::from_nullable(
            decompress_primitive::<$T>(encoded.typed_data(), array.exponents()),
            encoded.validity().clone_optional(),
        )
    })?;
    if let Some(patches) = array.patches() {
        // TODO(#121): right now, applying patches forces an extraneous copy of the array data
        let patched = decoded.patch(patches)?;
        let patched_encoding_id = patched.encoding().id().clone();
        patched
            .into_any()
            .downcast()
            .map_err(|_| VortexError::InvalidEncoding(patched_encoding_id))
            .map(|ptr| *ptr)
    } else {
        Ok(decoded)
    }
}

fn decompress_primitive<T: NativePType + ALPFloat>(
    values: &[T::ALPInt],
    exponents: &Exponents,
) -> Vec<T> {
    values
        .iter()
        .map(|&v| T::decode_single(v, exponents))
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress() {
        let array = PrimitiveArray::from(vec![1.234f32; 1025]);
        let encoded = alp_encode(&array).unwrap();
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().typed_data::<i32>(),
            vec![1234; 1025]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });

        let decoded = decompress(&encoded).unwrap();
        assert_eq!(array.typed_data::<f32>(), decoded.typed_data::<f32>());
    }

    #[test]
    fn test_nullable_compress() {
        let array = PrimitiveArray::from_iter(vec![None, Some(1.234f32), None]);
        let encoded = alp_encode(&array).unwrap();
        println!("Encoded {:?}", encoded);
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().typed_data::<i32>(),
            vec![0, 1234, 0]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });
    }
}
