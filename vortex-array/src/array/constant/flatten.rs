use vortex_dtype::{match_each_native_ptype, Nullability, PType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::BoolScalar;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::validity::Validity;
use crate::{ArrayDType, ArrayTrait};
use crate::{ArrayFlatten, Flattened};

impl ArrayFlatten for ConstantArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        let validity = match self.dtype().nullability() {
            Nullability::NonNullable => Validity::NonNullable,
            Nullability::Nullable => match self.scalar().is_null() {
                true => Validity::AllInvalid,
                false => Validity::AllValid,
            },
        };

        if let Ok(b) = BoolScalar::try_from(self.scalar()) {
            return Ok(Flattened::Bool(BoolArray::from_vec(
                vec![b.value().unwrap_or_default(); self.len()],
                validity,
            )));
        }

        if let Ok(ptype) = PType::try_from(self.scalar().dtype()) {
            return match_each_native_ptype!(ptype, |$P| {
                Ok(Flattened::Primitive(PrimitiveArray::from_vec::<$P>(
                    vec![$P::try_from(self.scalar())?; self.len()],
                    validity,
                )))
            });
        }

        vortex_bail!("Unsupported scalar type {}", self.dtype())
    }
}
