use std::iter;

use vortex_dtype::{match_each_native_ptype, Nullability, PType};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::{BinaryScalar, BoolScalar, Utf8Scalar};

use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::{BoolArray, VarBinViewArray};
use crate::validity::Validity;
use crate::{ArrayDType, Canonical, IntoCanonical};

impl IntoCanonical for ConstantArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let validity = match self.dtype().nullability() {
            Nullability::NonNullable => Validity::NonNullable,
            Nullability::Nullable => match self.scalar().is_null() {
                true => Validity::AllInvalid,
                false => Validity::AllValid,
            },
        };

        if let Ok(b) = BoolScalar::try_from(self.scalar()) {
            return Ok(Canonical::Bool(BoolArray::from_vec(
                vec![b.value().unwrap_or_default(); self.len()],
                validity,
            )));
        }

        if let Ok(s) = Utf8Scalar::try_from(self.scalar()) {
            let const_value = s
                .value()
                .ok_or_else(|| vortex_err!("Constant UTF-8 array has null value"))?;
            let string = const_value.as_str();

            return Ok(Canonical::VarBinView(VarBinViewArray::from_iter(
                iter::repeat(Some(string)).take(self.len()),
            )));
        }

        if let Ok(b) = BinaryScalar::try_from(self.scalar()) {
            let const_value = b
                .value()
                .ok_or_else(|| vortex_err!("Constant UTF-8 array has null value"))?;
            let bytes = const_value.as_slice();

            return Ok(Canonical::VarBinView(VarBinViewArray::from_iter(
                iter::repeat(Some(bytes)).take(self.len()),
            )));
        }

        if let Ok(ptype) = PType::try_from(self.scalar().dtype()) {
            return match_each_native_ptype!(ptype, |$P| {
                Ok(Canonical::Primitive(PrimitiveArray::from_vec::<$P>(
                    vec![$P::try_from(self.scalar()).unwrap_or_else(|_| $P::default()); self.len()],
                    validity,
                )))
            });
        }

        vortex_bail!("Unsupported scalar type {}", self.dtype())
    }
}
