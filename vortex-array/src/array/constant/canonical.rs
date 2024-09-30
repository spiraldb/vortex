use std::iter;

use vortex_dtype::{match_each_native_ptype, DType, Nullability};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::ScalarValue;

use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::array::{BoolArray, NullArray};
use crate::validity::Validity;
use crate::{ArrayDType, Canonical, IntoCanonical};

impl IntoCanonical for ConstantArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let validity = match self.dtype().nullability() {
            Nullability::NonNullable => Validity::NonNullable,
            Nullability::Nullable => match self.scalar_value().is_null() {
                true => Validity::AllInvalid,
                false => Validity::AllValid,
            },
        };

        let dtype = self.dtype().clone();

        match self.scalar_value() {
            ScalarValue::Bool(b) => Ok(Canonical::Bool(BoolArray::from_vec(
                vec![*b; self.len()],
                validity,
            ))),
            ScalarValue::Primitive(pvalue) => {
                let ptype = if let DType::Primitive(ptype, _) = dtype {
                    ptype
                } else {
                    vortex_bail!(
                        "constant array with dtype {} but primitive value {}",
                        dtype,
                        pvalue
                    );
                };

                match_each_native_ptype!(ptype, |$P| {
                    Ok(Canonical::Primitive(PrimitiveArray::from_vec::<$P>(
                        vec![$P::try_from(*pvalue).unwrap_or_else(|_| $P::default()); self.len()],
                        validity,
                    )))
                })
            }
            ScalarValue::Buffer(value) => {
                let const_value = value.as_slice();

                Ok(Canonical::VarBin(VarBinArray::from_iter_nonnull(
                    iter::repeat(const_value).take(self.len()),
                    dtype,
                )))
            }
            ScalarValue::BufferString(value) => {
                let const_value = value.as_bytes();

                Ok(Canonical::VarBin(VarBinArray::from_iter_nonnull(
                    iter::repeat(const_value).take(self.len()),
                    dtype,
                )))
            }
            ScalarValue::List(_) => vortex_bail!("Unsupported scalar type {}", dtype),
            ScalarValue::Null => {
                if !dtype.is_nullable() {
                    vortex_bail!("dtype is non-nullable but value is null: {}", dtype)
                }

                match dtype {
                    DType::Null => Ok(Canonical::Null(NullArray::new(self.len()))),
                    DType::Bool(_) => Ok(Canonical::Bool(BoolArray::from_vec(
                        vec![true; self.len()],
                        validity,
                    ))),
                    DType::Primitive(ptype, _) => {
                        match_each_native_ptype!(ptype, |$P| {
                            Ok(Canonical::Primitive(PrimitiveArray::from_vec::<$P>(
                                vec![$P::default(); self.len()],
                                validity,
                            )))
                        })
                    }
                    DType::Utf8(_) => Ok(Canonical::VarBin(VarBinArray::from_iter(
                        iter::repeat::<Option<String>>(None).take(self.len()),
                        dtype,
                    ))),
                    DType::Binary(_) => Ok(Canonical::VarBin(VarBinArray::from_iter(
                        iter::repeat::<Option<Vec<u8>>>(None).take(self.len()),
                        dtype,
                    ))),
                    DType::Struct(..) => vortex_bail!("Unsupported scalar type {}", dtype),
                    DType::List(..) => vortex_bail!("Unsupported scalar type {}", dtype),
                    DType::Extension(..) => vortex_bail!("Unsupported scalar type {}", dtype),
                }
            }
        }
    }
}
