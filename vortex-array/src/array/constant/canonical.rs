use arrow_array::builder::make_view;
use arrow_buffer::{BooleanBuffer, BufferBuilder, MutableBuffer};
use vortex_buffer::Buffer;
use vortex_dtype::{match_each_native_ptype, DType, Nullability, PType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::{BinaryScalar, BoolScalar, Utf8Scalar};

use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::{BinaryView, BoolArray, VarBinViewArray};
use crate::validity::Validity;
use crate::{ArrayDType, Canonical, IntoArray, IntoCanonical};

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
            let const_value = value.as_ref().map(|v| v.as_bytes());

            return canonical_byte_view(const_value, self.dtype(), self.len())
                .map(Canonical::VarBinView);
        }

        if let Ok(b) = BinaryScalar::try_from(scalar) {
            let value = b.value();
            let const_value = value.as_ref().map(|v| v.as_slice());

            return canonical_byte_view(const_value, self.dtype(), self.len())
                .map(Canonical::VarBinView);
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

fn canonical_byte_view(
    scalar_bytes: Option<&[u8]>,
    dtype: &DType,
    len: usize,
) -> VortexResult<VarBinViewArray> {
    match scalar_bytes {
        None => {
            let views = MutableBuffer::from(Vec::<u128>::with_capacity(1));

            VarBinViewArray::try_new(
                views.into(),
                Vec::new(),
                dtype.clone(),
                Validity::AllInvalid,
            )
        }
        Some(scalar_bytes) => {
            // Create a view to hold the scalar bytes.
            // If the scalar cannot be inlined, allocate a single buffer large enough to hold it.
            let view: u128 = make_view(scalar_bytes, 0, 0);
            let mut buffers = Vec::new();
            if scalar_bytes.len() >= BinaryView::MAX_INLINED_SIZE {
                buffers.push(
                    PrimitiveArray::new(
                        Buffer::from(scalar_bytes),
                        PType::U8,
                        Validity::NonNullable,
                    )
                    .into_array(),
                );
            }

            // Clone our constant view `len` times.
            // TODO(aduffy): switch this out for a ConstantArray once we
            //   add u128 PType, see https://github.com/spiraldb/vortex/issues/1110
            let mut views = BufferBuilder::<u128>::new(len);
            views.append_n(len, view);
            let views = views.finish().into();

            let validity = if dtype.nullability() == Nullability::NonNullable {
                Validity::NonNullable
            } else {
                Validity::AllValid
            };

            VarBinViewArray::try_new(views, buffers, dtype.clone(), validity)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::array::ConstantArray;
    use crate::compute::unary::scalar_at;
    use crate::IntoCanonical;

    #[test]
    fn test_canonicalize_const_str() {
        let const_array = ConstantArray::new("four".to_string(), 4);

        // Check all values correct.
        let canonical = const_array
            .into_canonical()
            .unwrap()
            .into_varbinview()
            .unwrap();

        assert_eq!(canonical.len(), 4);

        for i in 0..=3 {
            assert_eq!(scalar_at(&canonical, i).unwrap(), "four".into(),);
        }
    }
}
