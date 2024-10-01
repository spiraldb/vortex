use std::iter;

use vortex_dtype::{match_each_native_ptype, DType, Nullability, PType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::{BinaryScalar, BoolScalar, Utf8Scalar};

use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::array::BoolArray;
use crate::validity::Validity;
use crate::{ArrayDType, Canonical, IntoCanonical};

impl IntoCanonical for ConstantArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let scalar = &self.owned_scalar();

        let validity = match self.dtype().nullability() {
            Nullability::NonNullable => Validity::NonNullable,
            Nullability::Nullable => match scalar.is_null() {
                true => Validity::AllInvalid,
                false => Validity::AllValid,
            },
        };

        if let Ok(b) = BoolScalar::try_from(scalar) {
            return Ok(Canonical::Bool(BoolArray::from_vec(
                vec![b.value().unwrap_or_default(); self.len()],
                validity,
            )));
        }

        if let Ok(s) = Utf8Scalar::try_from(scalar) {
            let value = s.value();
            let const_value = value.as_ref().map(|v| v.as_bytes());

            return Ok(Canonical::VarBin(VarBinArray::from_iter(
                iter::repeat(const_value).take(self.len()),
                DType::Utf8(validity.nullability()),
            )));
        }

        if let Ok(b) = BinaryScalar::try_from(scalar) {
            let value = b.value();
            let const_value = value.as_ref().map(|v| v.as_slice());

            return Ok(Canonical::VarBin(VarBinArray::from_iter(
                iter::repeat(const_value).take(self.len()),
                DType::Binary(validity.nullability()),
            )));
        }

        if let Ok(ptype) = PType::try_from(scalar.dtype()) {
            return match_each_native_ptype!(ptype, |$P| {
                Ok(Canonical::Primitive(PrimitiveArray::from_vec::<$P>(
                    vec![$P::try_from(scalar).unwrap_or_else(|_| $P::default()); self.len()],
                    validity,
                )))
            });
        }

        vortex_bail!("Unsupported scalar type {}", self.dtype())
    }
}
