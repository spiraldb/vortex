use std::sync::Arc;

use arrow_array::builder::GenericByteViewBuilder;
use arrow_array::types::{BinaryViewType, ByteViewType, StringViewType};
use arrow_array::ArrayRef;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use crate::array::varbinview::BinaryView;
use crate::array::{VarBinArray, VarBinViewArray};
use crate::arrow::FromArrowArray;
use crate::validity::ArrayValidity;
use crate::{Array, ArrayDType, Canonical, IntoCanonical};

impl IntoCanonical for VarBinArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        fn into_byteview<T, F>(array: &VarBinArray, from_bytes_fn: F) -> ArrayRef
        where T: ByteViewType,
            F: Fn(&[u8]) -> &T::Native
        {
            let mut builder = GenericByteViewBuilder::<T>::with_capacity(array.len());
            builder.append_block(
                array
                    .bytes()
                    .into_buffer()
                    .expect("VarBinArray::bytes array must have buffer")
                    .into_arrow(),
            );

            for idx in 0..array.len() {
                if !array.is_valid(idx) {
                    builder.append_null();
                    continue;
                }
                let start = u32::try_from(array.offset_at(idx)).unwrap();
                let end = u32::try_from(array.offset_at(idx + 1)).unwrap();
                let len = end - start;
                if (len as usize) <= BinaryView::MAX_INLINED_SIZE {
                    // Get access to the value using the internal T type here.
                    let bytes = array.bytes_at(idx).unwrap();
                    let value = from_bytes_fn(bytes.as_slice());
                    builder.append_value(value);
                } else {
                    unsafe { builder.append_view_unchecked(0, start, end - start) };
                }
            }

            Arc::new(builder.finish())
        }

        let arrow_array = match self.dtype() {
            DType::Utf8(_) => into_byteview::<StringViewType, _>(&self, |b| unsafe {
                // SAFETY: VarBinViewArray values are checked at construction. If DType is Utf8,
                //  then all values must be valid UTF-8 bytes.
                std::str::from_utf8_unchecked(b)
            }),
            DType::Binary(_) => into_byteview::<BinaryViewType, _>(&self, |b| b),
            _ => vortex_bail!("invalid DType for VarBinViewArray")
        };
        let array = Array::from_arrow(arrow_array.clone(), arrow_array.is_nullable());
        let varbinview = VarBinViewArray::try_from(array)?;

        Ok(Canonical::VarBinView(varbinview))
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability};

    use crate::array::varbin::builder::VarBinBuilder;
    use crate::validity::ArrayValidity;
    use crate::IntoCanonical;

    #[test]
    fn test_canonical_varbin() {
        let mut varbin = VarBinBuilder::<i32>::with_capacity(10);
        varbin.push_null();
        varbin.push_null();
        // inlined value
        varbin.push_value("123456789012".as_bytes());
        // non-inlinable value
        varbin.push_value("1234567890123".as_bytes());
        let varbin = varbin.finish(DType::Utf8(Nullability::Nullable));

        let canonical = varbin.into_canonical().unwrap().into_varbinview().unwrap();

        assert!(!canonical.is_valid(0));
        assert!(!canonical.is_valid(1));

        // First value is inlined (12 bytes)
        assert!(canonical.view_at(2).is_inlined());
        assert_eq!(
            canonical.bytes_at(2).unwrap().as_slice(),
            "123456789012".as_bytes()
        );

        // Second value is not inlined (13 bytes)
        assert!(!canonical.view_at(3).is_inlined());
        assert_eq!(
            canonical.bytes_at(3).unwrap().as_slice(),
            "1234567890123".as_bytes()
        );
    }
}
