use vortex::match_each_native_ptype;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;
use vortex_schema::Nullability;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::validity::Validity;
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

        Ok(match self.scalar() {
            Scalar::Bool(b) => Flattened::Bool(BoolArray::from_vec(
                vec![b.value().copied().unwrap_or_default(); self.len()],
                validity,
            )),
            Scalar::Primitive(p) => {
                match_each_native_ptype!(p.ptype(), |$P| {
                    Flattened::Primitive(PrimitiveArray::from_vec::<$P>(
                        vec![$P::try_from(self.scalar())?; self.len()],
                        validity,
                    ))
                })
            }
            _ => panic!("Unsupported scalar type {}", self.dtype()),
        })
    }
}
