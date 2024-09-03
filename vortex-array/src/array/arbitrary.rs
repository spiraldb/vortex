use arbitrary::{Arbitrary, Result, Unstructured};
use vortex_dtype::{DType, NativePType, Nullability};

use super::{BoolArray, PrimitiveArray};
use crate::array::{VarBinArray, VarBinViewArray};
use crate::validity::Validity;
use crate::{Array, IntoArray as _};

impl<'a> Arbitrary<'a> for Array {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        random_array(u)
    }
}

fn random_array(u: &mut Unstructured) -> Result<Array> {
    match u.int_in_range(0..=12)? {
        0 => random_primitive::<u8>(u),
        1 => random_primitive::<u16>(u),
        2 => random_primitive::<u32>(u),
        3 => random_primitive::<u64>(u),
        4 => random_primitive::<i8>(u),
        5 => random_primitive::<i16>(u),
        6 => random_primitive::<i32>(u),
        7 => random_primitive::<i64>(u),
        8 => random_primitive::<f32>(u),
        9 => random_primitive::<f64>(u),
        10 => random_bool(u),
        11 => random_string(u),
        12 => random_bytes(u),
        _ => unreachable!(),
    }
}

fn random_string(u: &mut Unstructured) -> Result<Array> {
    let v = Vec::<Option<String>>::arbitrary(u)?;
    let arr = match u.int_in_range(0..=1)? {
        0 => VarBinArray::from_iter(v, DType::Utf8(Nullability::Nullable)).into_array(),
        1 => VarBinViewArray::from_iter_nullable_str(v).into_array(),
        _ => unreachable!(),
    };

    Ok(arr)
}

fn random_bytes(u: &mut Unstructured) -> Result<Array> {
    let v = Vec::<Option<Vec<u8>>>::arbitrary(u)?;
    let arr = match u.int_in_range(0..=1)? {
        0 => VarBinArray::from_iter(v, DType::Binary(Nullability::Nullable)).into_array(),
        1 => VarBinViewArray::from_iter_nullable_bin(v).into_array(),
        _ => unreachable!(),
    };

    Ok(arr)
}

fn random_primitive<'a, T: Arbitrary<'a> + NativePType>(u: &mut Unstructured<'a>) -> Result<Array> {
    let v = Vec::<T>::arbitrary(u)?;
    let validity = random_validity(u, v.len())?;
    Ok(PrimitiveArray::from_vec(v, validity).into_array())
}

fn random_bool(u: &mut Unstructured) -> Result<Array> {
    let v = Vec::<bool>::arbitrary(u)?;
    let validity = random_validity(u, v.len())?;

    Ok(BoolArray::from_vec(v, validity).into_array())
}

fn random_validity(u: &mut Unstructured, len: usize) -> Result<Validity> {
    let v = match u.int_in_range(0..=3)? {
        0 => Validity::AllValid,
        1 => Validity::AllInvalid,
        2 => Validity::NonNullable,
        3 => {
            let bools = (0..len)
                .map(|_| bool::arbitrary(u))
                .collect::<Result<Vec<_>>>()?;
            Validity::from(bools)
        }
        _ => unreachable!(),
    };

    Ok(v)
}
