use arbitrary::{Arbitrary, Result, Unstructured};
use vortex_dtype::{DType, FieldName, NativePType, Nullability};
use vortex_error::VortexUnwrap;

use super::{BoolArray, ChunkedArray, PrimitiveArray, StructArray};
use crate::array::{VarBinArray, VarBinViewArray};
use crate::validity::Validity;
use crate::{Array, ArrayDType, IntoArray as _};

impl<'a> Arbitrary<'a> for Array {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let nullability = if u.arbitrary()? {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        };
        random_array(u, None, nullability)
    }
}

fn random_array(
    u: &mut Unstructured,
    len: Option<usize>,
    nullability: Nullability,
) -> Result<Array> {
    let array_kind = u.int_in_range(0..=13)?;
    let name_count = u.int_in_range(1..=10)?;
    let names: Vec<FieldName> = arbitrary_vec_of_len(u, Some(name_count))?;
    let mut chunks = (0..u.int_in_range(1..=11)?)
        .map(|_| match array_kind {
            0 => random_primitive::<u8>(u, len, nullability),
            1 => random_primitive::<u16>(u, len, nullability),
            2 => random_primitive::<u32>(u, len, nullability),
            3 => random_primitive::<u64>(u, len, nullability),
            4 => random_primitive::<i8>(u, len, nullability),
            5 => random_primitive::<i16>(u, len, nullability),
            6 => random_primitive::<i32>(u, len, nullability),
            7 => random_primitive::<i64>(u, len, nullability),
            8 => random_primitive::<f32>(u, len, nullability),
            9 => random_primitive::<f64>(u, len, nullability),
            10 => random_bool(u, len, nullability),
            11 => random_string(u, len, nullability),
            12 => random_bytes(u, len, nullability),
            13 => random_struct(u, len, names.clone(), nullability),
            _ => unreachable!(),
        })
        .collect::<Result<Vec<_>>>()?;
    if chunks.len() == 1 {
        Ok(chunks.remove(0))
    } else {
        let dtype = chunks[0].dtype().clone();
        Ok(ChunkedArray::try_new(chunks, dtype)
            .vortex_unwrap()
            .into_array())
    }
}

fn random_string(
    u: &mut Unstructured,
    len: Option<usize>,
    nullability: Nullability,
) -> Result<Array> {
    match nullability {
        Nullability::NonNullable => {
            let v = arbitrary_vec_of_len::<String>(u, len)?;
            Ok(match u.int_in_range(0..=1)? {
                0 => VarBinArray::from_vec(v, DType::Utf8(Nullability::NonNullable)).into_array(),
                1 => VarBinViewArray::from_iter_str(v).into_array(),
                _ => unreachable!(),
            })
        }
        Nullability::Nullable => {
            let v = arbitrary_vec_of_len::<Option<String>>(u, len)?;
            Ok(match u.int_in_range(0..=1)? {
                0 => VarBinArray::from_iter(v, DType::Utf8(Nullability::Nullable)).into_array(),
                1 => VarBinViewArray::from_iter_nullable_str(v).into_array(),
                _ => unreachable!(),
            })
        }
    }
}

fn random_bytes(
    u: &mut Unstructured,
    len: Option<usize>,
    nullability: Nullability,
) -> Result<Array> {
    match nullability {
        Nullability::NonNullable => {
            let v = arbitrary_vec_of_len::<Vec<u8>>(u, len)?;
            Ok(match u.int_in_range(0..=1)? {
                0 => VarBinArray::from_vec(v, DType::Binary(Nullability::NonNullable)).into_array(),
                1 => VarBinViewArray::from_iter_bin(v).into_array(),
                _ => unreachable!(),
            })
        }
        Nullability::Nullable => {
            let v = arbitrary_vec_of_len::<Option<Vec<u8>>>(u, len)?;
            Ok(match u.int_in_range(0..=1)? {
                0 => VarBinArray::from_iter(v, DType::Binary(Nullability::Nullable)).into_array(),
                1 => VarBinViewArray::from_iter_nullable_bin(v).into_array(),
                _ => unreachable!(),
            })
        }
    }
}

fn random_primitive<'a, T: Arbitrary<'a> + NativePType>(
    u: &mut Unstructured<'a>,
    len: Option<usize>,
    nullability: Nullability,
) -> Result<Array> {
    let v = arbitrary_vec_of_len::<T>(u, len)?;
    let validity = random_validity(u, v.len(), nullability)?;
    Ok(PrimitiveArray::from_vec(v, validity).into_array())
}

fn random_bool(
    u: &mut Unstructured,
    len: Option<usize>,
    nullability: Nullability,
) -> Result<Array> {
    let v = arbitrary_vec_of_len(u, len)?;
    let validity = random_validity(u, v.len(), nullability)?;
    Ok(BoolArray::from_vec(v, validity).into_array())
}

fn random_validity(u: &mut Unstructured, len: usize, nullability: Nullability) -> Result<Validity> {
    match nullability {
        Nullability::NonNullable => Ok(Validity::NonNullable),
        Nullability::Nullable => Ok(match u.int_in_range(0..=2)? {
            0 => Validity::AllValid,
            1 => Validity::AllInvalid,
            2 => Validity::from(arbitrary_vec_of_len(u, Some(len))?),
            _ => unreachable!(),
        }),
    }
}

fn random_struct(
    u: &mut Unstructured,
    len: Option<usize>,
    names: Vec<FieldName>,
    nullability: Nullability,
) -> Result<Array> {
    let first_arr = random_array(u, len, nullability)?;
    let defined_len = len.unwrap_or(first_arr.len());
    let arrays = [Ok(first_arr)]
        .into_iter()
        .chain((1..names.len()).map(|_| random_array(u, Some(defined_len), nullability)))
        .collect::<Result<Vec<_>>>()?;
    Ok(StructArray::try_new(
        names.into(),
        arrays,
        defined_len,
        random_validity(u, defined_len, nullability)?,
    )
    .vortex_unwrap()
    .into_array())
}

fn arbitrary_vec_of_len<'a, T: Arbitrary<'a>>(
    u: &mut Unstructured<'a>,
    len: Option<usize>,
) -> Result<Vec<T>> {
    len.map(|l| (0..l).map(|_| T::arbitrary(u)).collect::<Result<Vec<_>>>())
        .unwrap_or_else(|| Vec::<T>::arbitrary(u))
}
