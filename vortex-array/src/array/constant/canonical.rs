use std::iter;

use arrow_buffer::BooleanBuffer;
use vortex_dtype::{match_each_native_ptype, Nullability, PType};
use vortex_error::{vortex_bail, VortexExpect, VortexResult};
use vortex_scalar::{BinaryScalar, BoolScalar, Utf8Scalar};

use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::{BoolArray, VarBinViewArray};
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
            return Ok(Canonical::Bool(BoolArray::try_new(
                if b.value().unwrap_or_default() {
                    BooleanBuffer::new_set(self.len())
                } else {
                    BooleanBuffer::new_unset(self.len())
                },
                validity,
            )?));
        }

        if let Ok(s) = Utf8Scalar::try_from(scalar) {
            let value = s.value();
            let const_value = value.as_ref().map(|v| v.as_str());

            let canonical = match validity.nullability() {
                Nullability::NonNullable => VarBinViewArray::from_iter_str(iter::repeat_n(
                    const_value
                        .vortex_expect("null Utf8Scalar value for non-nullable ConstantArray"),
                    self.len(),
                )),
                Nullability::Nullable => {
                    VarBinViewArray::from_iter_nullable_str(iter::repeat_n(const_value, self.len()))
                }
            };
            return Ok(Canonical::VarBinView(canonical));
        }

        if let Ok(b) = BinaryScalar::try_from(scalar) {
            let value = b.value();
            let const_value = value.as_ref().map(|v| v.as_slice());

            let canonical = match validity.nullability() {
                Nullability::NonNullable => VarBinViewArray::from_iter_bin(iter::repeat_n(
                    const_value.vortex_expect("null BinaryScalar for non-nullable ConstantArray"),
                    self.len(),
                )),
                Nullability::Nullable => {
                    VarBinViewArray::from_iter_nullable_bin(iter::repeat_n(const_value, self.len()))
                }
            };
            return Ok(Canonical::VarBinView(canonical));
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
