use std::sync::Arc;

use arrow_array::builder::GenericByteViewBuilder;
use arrow_array::types::{BinaryViewType, ByteViewType, StringViewType};
use arrow_array::ArrayRef;
use num_traits::AsPrimitive;
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::varbin::VarBinArray;
use crate::array::{BinaryView, PrimitiveArray, VarBinViewArray};
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::{Array, Canonical, IntoArrayVariant, IntoCanonical};

impl IntoCanonical for VarBinArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let (dtype, bytes, offsets, validity) = self.into_parts();
        let bytes = bytes.into_primitive()?;
        let offsets = offsets.into_primitive()?;

        // Constant validity check function.
        let validity_check_fn: &dyn Fn(usize) -> bool = match validity {
            Validity::NonNullable | Validity::AllValid => &|_idx: usize| true,
            Validity::AllInvalid => &|_idx: usize| false,
            Validity::Array(_) => &|idx: usize| validity.is_valid(idx),
        };

        let arrow_array = match dtype {
            DType::Utf8(_) => {
                byteview_from_varbin_parts(
                    StringViewType {},
                    bytes,
                    offsets,
                    validity_check_fn,
                    |b| unsafe {
                        // SAFETY: VarBinViewArray values are checked at construction. If DType is Utf8,
                        //  then all values must be valid UTF-8 bytes.
                        std::str::from_utf8_unchecked(b)
                    },
                )
            }
            DType::Binary(_) => byteview_from_varbin_parts(
                BinaryViewType {},
                bytes,
                offsets,
                validity_check_fn,
                |b| b,
            ),
            _ => vortex_bail!("invalid DType for VarBinViewArray"),
        };
        let array = Array::from_arrow(arrow_array.clone(), arrow_array.is_nullable());
        let varbinview = VarBinViewArray::try_from(array)?;

        Ok(Canonical::VarBinView(varbinview))
    }
}

fn byteview_from_varbin_parts<T, F, ValidFn>(
    _type: T,
    bytes: PrimitiveArray,
    offsets: PrimitiveArray,
    validity_check_fn: ValidFn,
    from_bytes_fn: F,
) -> ArrayRef
where
    T: ByteViewType,
    F: Fn(&[u8]) -> &T::Native,
    ValidFn: Fn(usize) -> bool,
{
    let array_len = offsets.len() - 1;
    let mut builder = GenericByteViewBuilder::<T>::with_capacity(array_len);

    // Directly append the buffer from the original VarBin to back the new VarBinView
    builder.append_block(bytes.clone().into_buffer().into_arrow());

    // Monomorphized `offset_at` accessor.
    // This is more efficient than going through the `offset_at` method when we are going
    // to touch the entire array.

    let offset_fn: &dyn Fn(usize) -> usize = match_each_integer_ptype!(offsets.ptype(), |$P| {
        let offsets_typed: &[$P] = offsets.maybe_null_slice::<$P>();
        &|idx: usize| -> usize { offsets_typed[idx].as_() }
    });

    let bytes_buffer = bytes.into_buffer();

    // Can we factor out the validity check?
    for idx in 0..array_len {
        // This check should be specialized away if the function is false.
        if !validity_check_fn(idx) {
            builder.append_null();
            continue;
        }
        let start = offset_fn(idx);
        let end = offset_fn(idx + 1);
        let len = end - start;

        // TODO(aduffy): fix this to overflow into multiple buffers in a slow-path.
        assert_eq!(
            start as u32 as usize, start,
            "VarBinView cannot have buffer >= 2GiB"
        );
        assert_eq!(
            end as u32 as usize, end,
            "VarBinView cannot have buffer >= 2GiB"
        );

        if len <= BinaryView::MAX_INLINED_SIZE {
            let bytes = bytes_buffer.slice(start..end);
            let value = from_bytes_fn(bytes.as_slice());
            builder.append_value(value);
        } else {
            unsafe { builder.append_view_unchecked(0, start as u32, len as u32) };
        }
    }

    Arc::new(builder.finish())
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
