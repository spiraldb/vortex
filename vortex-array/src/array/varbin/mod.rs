use std::fmt::Debug;

use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
pub use stats::compute_stats;
use vortex_buffer::Buffer;
use vortex_dtype::{match_each_native_ptype, DType, NativePType, Nullability};
use vortex_error::{
    vortex_bail, vortex_err, vortex_panic, VortexError, VortexExpect as _, VortexResult,
    VortexUnwrap as _,
};
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::builder::VarBinBuilder;
use crate::compute::slice;
use crate::compute::unary::scalar_at;
use crate::stats::StatsSet;
use crate::validity::{Validity, ValidityMetadata};
use crate::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, IntoArrayVariant};

mod accessor;
mod array;
pub mod builder;
mod compute;
mod flatten;
mod stats;
mod variants;

impl_encoding!("vortex.varbin", 4u16, VarBin);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarBinMetadata {
    validity: ValidityMetadata,
    offsets_dtype: DType,
    bytes_len: usize,
}

impl VarBinArray {
    pub fn try_new(
        offsets: Array,
        bytes: Array,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Self> {
        if !offsets.dtype().is_int() || offsets.dtype().is_nullable() {
            vortex_bail!(MismatchedTypes: "non nullable int", offsets.dtype());
        }
        if !matches!(bytes.dtype(), &DType::BYTES) {
            vortex_bail!(MismatchedTypes: "u8", bytes.dtype());
        }
        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            vortex_bail!(MismatchedTypes: "utf8 or binary", dtype);
        }
        if dtype.is_nullable() == (validity == Validity::NonNullable) {
            vortex_bail!("incorrect validity {:?}", validity);
        }

        let length = offsets.len() - 1;

        let metadata = VarBinMetadata {
            validity: validity.to_metadata(offsets.len() - 1)?,
            offsets_dtype: offsets.dtype().clone(),
            bytes_len: bytes.len(),
        };

        let mut children = Vec::with_capacity(3);
        children.push(offsets);
        children.push(bytes);
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(dtype, length, metadata, children.into(), StatsSet::new())
    }

    #[inline]
    pub fn offsets(&self) -> Array {
        self.as_ref()
            .child(0, &self.metadata().offsets_dtype, self.len() + 1)
            .vortex_expect("Missing offsets in VarBinArray")
    }

    pub fn first_offset<T: NativePType + for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
    ) -> VortexResult<T> {
        scalar_at(&self.offsets(), 0)?
            .cast(&DType::from(T::PTYPE))?
            .as_ref()
            .try_into()
    }

    #[inline]
    pub fn bytes(&self) -> Array {
        self.as_ref()
            .child(1, &DType::BYTES, self.metadata().bytes_len)
            .vortex_expect("Missing bytes in VarBinArray")
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.as_ref().child(2, &Validity::DTYPE, self.len()))
    }

    pub fn sliced_bytes(&self) -> VortexResult<Array> {
        let first_offset: usize = scalar_at(&self.offsets(), 0)?.as_ref().try_into()?;
        let last_offset: usize = scalar_at(&self.offsets(), self.offsets().len() - 1)?
            .as_ref()
            .try_into()?;
        slice(self.bytes(), first_offset, last_offset)
    }

    pub fn from_vec<T: AsRef<[u8]>>(vec: Vec<T>, dtype: DType) -> Self {
        let size: usize = vec.iter().map(|v| v.as_ref().len()).sum();
        if size < u32::MAX as usize {
            Self::from_vec_sized::<u32, T>(vec, dtype)
        } else {
            Self::from_vec_sized::<u64, T>(vec, dtype)
        }
    }

    fn from_vec_sized<K, T>(vec: Vec<T>, dtype: DType) -> Self
    where
        K: NativePType,
        T: AsRef<[u8]>,
    {
        let mut builder = VarBinBuilder::<K>::with_capacity(vec.len());
        for v in vec {
            builder.push_value(v.as_ref());
        }
        builder.finish(dtype)
    }

    #[allow(clippy::same_name_method)]
    pub fn from_iter<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
        dtype: DType,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinBuilder::<u32>::with_capacity(iter.size_hint().0);
        for v in iter {
            builder.push(v.as_ref().map(|o| o.as_ref()));
        }
        builder.finish(dtype)
    }

    pub fn offset_at(&self, index: usize) -> usize {
        PrimitiveArray::try_from(self.offsets())
            .ok()
            .map(|p| {
                match_each_native_ptype!(p.ptype(), |$P| {
                    p.maybe_null_slice::<$P>()[index].as_()
                })
            })
            .unwrap_or_else(|| {
                scalar_at(&self.offsets(), index)
                    .unwrap_or_else(|err| {
                        vortex_panic!(err, "Failed to get offset at index: {}", index)
                    })
                    .as_ref()
                    .try_into()
                    .vortex_expect("Failed to convert offset to usize")
            })
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Buffer> {
        let start = self.offset_at(index);
        let end = self.offset_at(index + 1);
        let sliced = slice(self.bytes(), start, end)?;
        Ok(sliced.into_primitive()?.buffer().clone())
    }
}

impl ArrayTrait for VarBinArray {}

impl From<Vec<&[u8]>> for VarBinArray {
    fn from(value: Vec<&[u8]>) -> Self {
        Self::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<Vec<u8>>> for VarBinArray {
    fn from(value: Vec<Vec<u8>>) -> Self {
        Self::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<String>> for VarBinArray {
    fn from(value: Vec<String>) -> Self {
        Self::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl From<Vec<&str>> for VarBinArray {
    fn from(value: Vec<&str>) -> Self {
        Self::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Binary(Nullability::Nullable))
    }
}

impl FromIterator<Option<Vec<u8>>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Binary(Nullability::Nullable))
    }
}

impl FromIterator<Option<String>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Utf8(Nullability::Nullable))
    }
}

impl<'a> FromIterator<Option<&'a str>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Utf8(Nullability::Nullable))
    }
}

pub fn varbin_scalar(value: Buffer, dtype: &DType) -> Scalar {
    if matches!(dtype, DType::Utf8(_)) {
        Scalar::try_utf8(value, dtype.nullability())
            .map_err(|err| vortex_err!("Failed to create scalar from utf8 buffer: {}", err))
            .vortex_unwrap()
    } else {
        Scalar::binary(value, dtype.nullability())
    }
}

#[cfg(test)]
mod test {
    use rstest::{fixture, rstest};
    use vortex_dtype::{DType, Nullability};

    use crate::array::primitive::PrimitiveArray;
    use crate::array::varbin::VarBinArray;
    use crate::compute::slice;
    use crate::compute::unary::scalar_at;
    use crate::validity::Validity;
    use crate::{Array, IntoArray};

    #[fixture]
    fn binary_array() -> Array {
        let values = PrimitiveArray::from(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from(vec![0, 11, 44]);

        VarBinArray::try_new(
            offsets.into_array(),
            values.into_array(),
            DType::Utf8(Nullability::NonNullable),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    }

    #[rstest]
    pub fn test_scalar_at(binary_array: Array) {
        assert_eq!(binary_array.len(), 2);
        assert_eq!(scalar_at(&binary_array, 0).unwrap(), "hello world".into());
        assert_eq!(
            scalar_at(&binary_array, 1).unwrap(),
            "hello world this is a long string".into()
        )
    }

    #[rstest]
    pub fn slice_array(binary_array: Array) {
        let binary_arr = slice(&binary_array, 1, 2).unwrap();
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            "hello world this is a long string".into()
        );
    }
}
