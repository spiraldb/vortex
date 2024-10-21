use std::sync::Arc;

use arrow_array::builder::make_view;
use arrow_array::types::{BinaryViewType, ByteViewType, StringViewType};
use arrow_array::{ArrayRef, GenericByteViewArray};
use arrow_buffer::{BufferBuilder, NullBufferBuilder, ScalarBuffer};
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

        let arrow_array = match dtype {
            DType::Utf8(_) => byteview_from_varbin::<StringViewType>(bytes, offsets, validity),
            DType::Binary(_) => byteview_from_varbin::<BinaryViewType>(bytes, offsets, validity),
            _ => vortex_bail!("invalid DType for VarBinViewArray"),
        };
        let array = Array::from_arrow(arrow_array.clone(), arrow_array.is_nullable());
        let varbinview = VarBinViewArray::try_from(array)?;

        Ok(Canonical::VarBinView(varbinview))
    }
}

// Sentinel indicating that a value being passed to the `make_view` constructor is unused.
const UNUSED: u32 = u32::MAX;

fn byteview_from_varbin<T: ByteViewType>(
    bytes: PrimitiveArray,
    offsets: PrimitiveArray,
    validity: Validity,
) -> ArrayRef {
    let array_len = offsets.len() - 1;

    let mut views = BufferBuilder::<u128>::new(array_len);
    let mut nulls = NullBufferBuilder::new(array_len);

    // TODO(aduffy): handle arrays >= 2GiB by splitting into multiple blocks at string boundaries.
    let buffers = vec![bytes.clone().into_buffer().into_arrow()];
    assert!(
        buffers[0].len() <= i32::MAX as usize,
        "VarBinView cannot support arrays of length >2GiB"
    );

    // Monomorphized `offset_at` accessor.
    // This is more efficient than going through the `offset_at` method when we are going
    // to touch the entire array.
    let offset_fn: &dyn Fn(usize) -> usize = match_each_integer_ptype!(offsets.ptype(), |$P| {
        let offsets_typed: &[$P] = offsets.maybe_null_slice::<$P>();
        &|idx: usize| -> usize { offsets_typed[idx].as_() }
    });

    // This specializes validity lookups for the 3 different nullability patterns.
    // This is faster than matching on the validity each time.
    let validity_fn: &dyn Fn(usize) -> bool = match validity {
        // No nulls => use a constant true function
        Validity::NonNullable | Validity::AllValid => &|_idx: usize| true,
        // All nulls => use constant false
        Validity::AllInvalid => &|_idx: usize| false,
        // Mix of null and non-null, index into the validity map
        _ => &|idx: usize| validity.is_valid(idx),
    };

    let bytes_buffer = bytes.into_buffer();

    for idx in 0..array_len {
        let is_valid = validity_fn(idx);
        if !is_valid {
            nulls.append_null();
            views.append(0);
            continue;
        }

        // Non-null codepath
        nulls.append_non_null();

        // Find the index in the buffer.
        let start = offset_fn(idx);
        let end = offset_fn(idx + 1);
        let len = end - start;

        // Copy the first MAX(len, 12) bytes into the end of the view.
        let bytes = bytes_buffer.slice(start..end);
        let view: u128 = if len <= BinaryView::MAX_INLINED_SIZE {
            make_view(bytes.as_slice(), UNUSED, UNUSED)
        } else {
            let block_id = 0u32;
            make_view(bytes.as_slice(), block_id, start as u32)
        };

        views.append(view);
    }

    // SAFETY: we enforce in the Vortex type layer that Utf8 data is properly encoded.
    Arc::new(unsafe {
        GenericByteViewArray::<T>::new_unchecked(
            ScalarBuffer::from(views.finish()),
            buffers,
            nulls.finish(),
        )
    })
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
