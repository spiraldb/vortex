use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::UInt8Type;
use vortex_dtype::{match_each_native_ptype, DType, Nullability, PType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::{BoolScalar, Utf8Scalar};

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbinview::{BinaryView, VarBinViewArray};
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::{ArrayDType, ArrayData, Canonical, IntoArray, IntoCanonical};

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
            let const_value = s.value().unwrap();
            let bytes = const_value.as_bytes();

            let buffers = if bytes.len() <= BinaryView::MAX_INLINED_SIZE {
                Vec::new()
            } else {
                vec![PrimitiveArray::from_vec(bytes.to_vec(), validity.clone()).into_array()]
            };

            // Repeat the same view over and over again.
            let view = if bytes.len() <= BinaryView::MAX_INLINED_SIZE {
                BinaryView::new_inlined(bytes)
            } else {
                // Create a new view using the provided byte buffer
                BinaryView::new_view(bytes.len() as u32, bytes[0..4].try_into().unwrap(), 0, 0)
            };

            // Construct the Views array to be a repeating byte string of 16 bytes per entry.
            let mut views = PrimitiveBuilder::<UInt8Type>::new();
            (0..self.len())
                .for_each(|_| views.append_slice(view.as_u128().to_le_bytes().as_slice()));
            let views_array =
                ArrayData::from_arrow(&views.finish(), self.dtype().is_nullable()).into_array();

            return Ok(Canonical::VarBinView(
                VarBinViewArray::try_new(
                    views_array,
                    buffers,
                    DType::Utf8(validity.nullability()),
                    validity,
                )
                .unwrap(),
            ));
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
