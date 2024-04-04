use arrow_buffer::NullBufferBuilder;
use num_traits::PrimInt;
use vortex_schema::DType;

use crate::array::primitive::PrimitiveArray;
use crate::array::validity::Validity;
use crate::array::varbin::VarBinArray;
use crate::array::Array;
use crate::ptype::NativePType;

pub struct VarBinBuilder<O: NativePType + PrimInt> {
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: NullBufferBuilder,
}

impl<O: NativePType + PrimInt> VarBinBuilder<O> {
    pub fn with_capacity(len: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(O::zero());
        Self {
            offsets,
            data: Vec::new(),
            validity: NullBufferBuilder::new(len),
        }
    }

    pub fn push(&mut self, value: Option<&[u8]>) {
        match value {
            Some(v) => {
                self.offsets
                    .push(O::from(self.data.len() + v.len()).unwrap());
                self.data.extend_from_slice(v);
                self.validity.append_non_null();
            }
            None => {
                self.offsets.push(self.offsets[self.offsets.len() - 1]);
                self.validity.append_null();
            }
        }
    }

    pub fn finish(self, dtype: DType) -> VarBinArray {
        let offsets = PrimitiveArray::from(self.offsets);
        let data = PrimitiveArray::from(self.data);

        // TODO(ngates): create our own ValidityBuilder that doesn't need mut or clone on finish.
        let nulls = self.validity.finish_cloned();

        let validity = if dtype.is_nullable() {
            Some(
                nulls
                    .map(Validity::from)
                    .unwrap_or_else(|| Validity::Valid(offsets.len() - 1)),
            )
        } else {
            assert!(nulls.is_none(), "dtype and validity mismatch");
            None
        };

        VarBinArray::new(offsets.into_array(), data.into_array(), dtype, validity)
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::DType;
    use vortex_schema::Nullability::Nullable;

    use crate::array::varbin::builder::VarBinBuilder;
    use crate::array::Array;
    use crate::compute::scalar_at::scalar_at;
    use crate::scalar::Scalar;

    #[test]
    fn test_builder() {
        let mut builder = VarBinBuilder::<i32>::with_capacity(0);
        builder.push(Some(b"hello"));
        builder.push(None);
        builder.push(Some(b"world"));
        let array = builder.finish(DType::Utf8(Nullable));

        assert_eq!(array.len(), 3);
        assert_eq!(array.nullability(), Nullable);
        assert_eq!(scalar_at(&array, 0).unwrap(), Scalar::from("hello"));
        assert!(scalar_at(&array, 1).unwrap().is_null());
    }
}
