use std::mem;

use arrow_buffer::NullBufferBuilder;
use vortex_dtype::DType;
use vortex_dtype::NativePType;

use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::{OwnedVarBinArray, VarBinArray};
use crate::validity::Validity;
use crate::IntoArray;

pub struct VarBinBuilder<O: NativePType> {
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: NullBufferBuilder,
}

impl<O: NativePType> VarBinBuilder<O> {
    pub fn with_capacity(len: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(O::zero());
        Self {
            offsets,
            data: Vec::new(),
            validity: NullBufferBuilder::new(len),
        }
    }

    #[inline]
    pub fn push(&mut self, value: Option<&[u8]>) {
        match value {
            Some(v) => self.push_value(v),
            None => self.push_null(),
        }
    }

    #[inline]
    pub fn push_value(&mut self, value: &[u8]) {
        self.offsets
            .push(O::from(self.data.len() + value.len()).unwrap());
        self.data.extend_from_slice(value);
        self.validity.append_non_null();
    }

    #[inline]
    pub fn push_null(&mut self) {
        self.offsets.push(self.offsets[self.offsets.len() - 1]);
        self.validity.append_null();
    }

    pub fn finish(&mut self, dtype: DType) -> OwnedVarBinArray {
        let offsets = PrimitiveArray::from(mem::take(&mut self.offsets));
        let data = PrimitiveArray::from(mem::take(&mut self.data));

        let nulls = self.validity.finish();

        let validity = if dtype.is_nullable() {
            nulls.map(Validity::from).unwrap_or(Validity::AllValid)
        } else {
            assert!(nulls.is_none(), "dtype and validity mismatch");
            Validity::NonNullable
        };

        VarBinArray::try_new(offsets.into_array(), data.into_array(), dtype, validity).unwrap()
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::DType;
    use vortex_dtype::Nullability::Nullable;

    use crate::array::varbin::builder::VarBinBuilder;
    use crate::compute::scalar_at::scalar_at;
    use crate::scalar::Utf8Scalar;
    use crate::{ArrayDType, IntoArray};

    #[test]
    fn test_builder() {
        let mut builder = VarBinBuilder::<i32>::with_capacity(0);
        builder.push(Some(b"hello"));
        builder.push(None);
        builder.push(Some(b"world"));
        let array = builder.finish(DType::Utf8(Nullable)).into_array();

        assert_eq!(array.len(), 3);
        assert_eq!(array.dtype().nullability(), Nullable);
        assert_eq!(
            scalar_at(&array, 0).unwrap(),
            Utf8Scalar::nullable("hello".to_owned()).into()
        );
        assert!(scalar_at(&array, 1).unwrap().is_null());
    }
}
