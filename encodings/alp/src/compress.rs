use vortex::array::{PrimitiveArray, Sparse, SparseArray};
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray, IntoArrayVariant};
use vortex_dtype::{NativePType, PType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::alp::ALPFloat;
use crate::array::ALPArray;
use crate::Exponents;

#[macro_export]
macro_rules! match_each_alp_float_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use vortex_dtype::PType;
        let ptype = $self;
        match ptype {
            PType::F32 => __with__! { f32 },
            PType::F64 => __with__! { f64 },
            _ => panic!("ALP can only encode f32 and f64"),
        }
    })
}

pub fn alp_encode_components<T>(
    values: &PrimitiveArray,
    exponents: Option<Exponents>,
) -> (Exponents, Array, Option<Array>)
where
    T: ALPFloat + NativePType,
    T::ALPInt: NativePType,
{
    let (exponents, encoded, exc_pos, exc) = T::encode(values.maybe_null_slice::<T>(), exponents);
    let len = encoded.len();
    (
        exponents,
        PrimitiveArray::from_vec(encoded, values.validity()).into_array(),
        (!exc.is_empty()).then(|| {
            SparseArray::try_new(
                PrimitiveArray::from(exc_pos).into_array(),
                PrimitiveArray::from_vec(exc, Validity::AllValid).into_array(),
                len,
                Scalar::null(values.dtype().as_nullable()),
            )
            .unwrap()
            .into_array()
        }),
    )
}

pub fn alp_encode(parray: &PrimitiveArray) -> VortexResult<ALPArray> {
    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => alp_encode_components::<f32>(parray, None),
        PType::F64 => alp_encode_components::<f64>(parray, None),
        _ => vortex_bail!("ALP can only encode f32 and f64"),
    };
    ALPArray::try_new(encoded, exponents, patches)
}

pub fn decompress(array: ALPArray) -> VortexResult<PrimitiveArray> {
    let encoded = array.encoded().into_primitive()?;
    let validity = encoded.validity();

    let decoded = match_each_alp_float_ptype!(array.dtype().try_into()?, |$T| {
        PrimitiveArray::from_vec(
            decompress_primitive::<$T>(encoded.into_maybe_null_slice(), array.exponents()),
            validity,
        )
    });

    if let Some(patches) = array.patches() {
        patch_decoded(decoded, &patches)
    } else {
        Ok(decoded)
    }
}

fn patch_decoded(array: PrimitiveArray, patches: &Array) -> VortexResult<PrimitiveArray> {
    match patches.encoding().id() {
        Sparse::ID => {
            match_each_alp_float_ptype!(array.ptype(), |$T| {
                let typed_patches = SparseArray::try_from(patches).unwrap();
                array.patch(
                    &typed_patches.resolved_indices(),
                    typed_patches.values().into_primitive()?.maybe_null_slice::<$T>())
            })
        }
        _ => vortex_bail!(
            "Can't patch ALP array with {}; only {} is supported",
            patches,
            Sparse::ID
        ),
    }
}

fn decompress_primitive<T: NativePType + ALPFloat>(
    values: Vec<T::ALPInt>,
    exponents: Exponents,
) -> Vec<T> {
    values
        .into_iter()
        .map(|v| T::decode_single(v, exponents))
        .collect()
}

#[cfg(test)]
mod tests {
    use core::f64;

    use vortex::compute::unary::scalar_at;
    use vortex::AsArray;

    use super::*;

    #[test]
    fn test_compress() {
        let array = PrimitiveArray::from(vec![1.234f32; 1025]);
        let encoded = alp_encode(&array).unwrap();
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().maybe_null_slice::<i32>(),
            vec![1234; 1025]
        );
        assert_eq!(encoded.exponents(), Exponents { e: 4, f: 1 });

        let decoded = decompress(encoded).unwrap();
        assert_eq!(
            array.maybe_null_slice::<f32>(),
            decoded.maybe_null_slice::<f32>()
        );
    }

    #[test]
    fn test_nullable_compress() {
        let array = PrimitiveArray::from_nullable_vec(vec![None, Some(1.234f32), None]);
        let encoded = alp_encode(&array).unwrap();
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().maybe_null_slice::<i32>(),
            vec![0, 1234, 0]
        );
        assert_eq!(encoded.exponents(), Exponents { e: 4, f: 1 });

        let decoded = decompress(encoded).unwrap();
        let expected = vec![0f32, 1.234f32, 0f32];
        assert_eq!(decoded.maybe_null_slice::<f32>(), expected.as_slice());
    }

    #[test]
    #[allow(clippy::approx_constant)] // ALP doesn't like E
    fn test_patched_compress() {
        let values = vec![1.234f64, 2.718, std::f64::consts::PI, 4.0];
        let array = PrimitiveArray::from(values.clone());
        let encoded = alp_encode(&array).unwrap();
        assert!(encoded.patches().is_some());
        assert_eq!(
            encoded.encoded().as_primitive().maybe_null_slice::<i64>(),
            vec![1234i64, 2718, 2718, 4000] // fill forward
        );
        assert_eq!(encoded.exponents(), Exponents { e: 3, f: 0 });

        let decoded = decompress(encoded).unwrap();
        assert_eq!(values, decoded.maybe_null_slice::<f64>());
    }

    #[test]
    #[allow(clippy::approx_constant)] // ALP doesn't like E
    fn test_nullable_patched_scalar_at() {
        let values = vec![
            Some(1.234f64),
            Some(2.718),
            Some(std::f64::consts::PI),
            Some(4.0),
            None,
        ];
        let array = PrimitiveArray::from_nullable_vec(values);
        let encoded = alp_encode(&array).unwrap();
        assert!(encoded.patches().is_some());

        assert_eq!(encoded.exponents(), Exponents { e: 3, f: 0 });

        for idx in 0..3 {
            let s = scalar_at(encoded.as_array_ref(), idx).unwrap();
            assert!(s.is_valid());
        }

        let s = scalar_at(encoded.as_array_ref(), 4).unwrap();
        assert!(s.is_null());

        let _decoded = decompress(encoded).unwrap();
    }

    #[test]
    fn roundtrips_close_fractional() {
        let original = PrimitiveArray::from(vec![195.26274f32, 195.27837, -48.815685]);
        let alp_arr = alp_encode(&original).unwrap();
        let decompressed = alp_arr.into_primitive().unwrap();
        assert_eq!(
            original.maybe_null_slice::<f32>(),
            decompressed.maybe_null_slice::<f32>()
        );
    }
}
