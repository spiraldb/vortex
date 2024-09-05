use arrow_array::UInt8Array;
use arrow_buffer::{BooleanBufferBuilder, Buffer, MutableBuffer, ScalarBuffer};
use itertools::Itertools;
use vortex_dtype::{DType, PType, StructDType};
use vortex_error::{vortex_bail, vortex_err, ErrString, VortexResult};

use crate::array::chunked::ChunkedArray;
use crate::array::extension::ExtensionArray;
use crate::array::null::NullArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::{BinaryView, BoolArray, VarBinViewArray};
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::variants::StructArrayTrait;
use crate::{
    Array, ArrayDType, ArrayValidity, Canonical, IntoArray, IntoArrayVariant, IntoCanonical,
};

impl IntoCanonical for ChunkedArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let validity = if self.dtype().is_nullable() {
            self.logical_validity().into_validity()
        } else {
            Validity::NonNullable
        };
        try_canonicalize_chunks(self.chunks().collect(), validity, self.dtype())
    }
}

pub(crate) fn try_canonicalize_chunks(
    chunks: Vec<Array>,
    validity: Validity,
    dtype: &DType,
) -> VortexResult<Canonical> {
    if chunks.is_empty() {
        vortex_bail!(InvalidArgument: "chunks must be non-empty")
    }

    let mismatched = chunks
        .iter()
        .filter(|chunk| !chunk.dtype().eq(dtype))
        .collect::<Vec<_>>();
    if !mismatched.is_empty() {
        vortex_bail!(MismatchedTypes: dtype.clone(), ErrString::from(format!("{:?}", mismatched)))
    }

    match dtype {
        // Structs can have their internal field pointers swizzled to push the chunking down
        // one level internally without copying or decompressing any data.
        DType::Struct(struct_dtype, _) => {
            let struct_array = swizzle_struct_chunks(chunks.as_slice(), validity, struct_dtype)?;
            Ok(Canonical::Struct(struct_array))
        }

        // Extension arrays are containers that wraps an inner storage array with some metadata.
        // We delegate to the canonical format of the internal storage array for every chunk, and
        // push the chunking down into the inner storage array.
        //
        //  Input:
        //  ------
        //
        //                  ChunkedArray
        //                 /            \
        //                /              \
        //         ExtensionArray     ExtensionArray
        //             |                   |
        //          storage             storage
        //
        //
        //  Output:
        //  ------
        //
        //                 ExtensionArray
        //                      |
        //                 ChunkedArray
        //                /             \
        //          storage             storage
        //
        DType::Extension(ext_dtype, _) => {
            // Recursively apply canonicalization and packing to the storage array backing
            // each chunk of the extension array.
            let storage_chunks: Vec<Array> = chunks
                .iter()
                // Extension-typed arrays can be compressed into something that is not an
                // ExtensionArray, so we should canonicalize each chunk into ExtensionArray first.
                .map(|chunk| chunk.clone().into_extension().map(|ext| ext.storage()))
                .collect::<VortexResult<Vec<Array>>>()?;
            let storage_dtype = storage_chunks
                .first()
                .ok_or_else(|| vortex_err!("Expected at least one chunk in ChunkedArray"))?
                .dtype()
                .clone();
            let chunked_storage =
                ChunkedArray::try_new(storage_chunks, storage_dtype)?.into_array();

            Ok(Canonical::Extension(ExtensionArray::new(
                ext_dtype.clone(),
                chunked_storage,
            )))
        }

        // TODO(aduffy): better list support
        DType::List(..) => {
            todo!()
        }

        DType::Bool(_) => {
            let bool_array = pack_bools(chunks.as_slice(), validity)?;
            Ok(Canonical::Bool(bool_array))
        }
        DType::Primitive(ptype, _) => {
            let prim_array = pack_primitives(chunks.as_slice(), *ptype, validity)?;
            Ok(Canonical::Primitive(prim_array))
        }
        DType::Utf8(_) => {
            let varbin_array = pack_views(chunks.as_slice(), dtype, validity)?;
            Ok(Canonical::VarBinView(varbin_array))
        }
        DType::Binary(_) => {
            let varbin_array = pack_views(chunks.as_slice(), dtype, validity)?;
            Ok(Canonical::VarBinView(varbin_array))
        }
        DType::Null => {
            let len = chunks.iter().map(|chunk| chunk.len()).sum();
            let null_array = NullArray::new(len);
            Ok(Canonical::Null(null_array))
        }
    }
}

/// Swizzle the pointers within a ChunkedArray of StructArrays to instead be a single
/// StructArray, where the Array for each Field is a ChunkedArray.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn swizzle_struct_chunks(
    chunks: &[Array],
    validity: Validity,
    struct_dtype: &StructDType,
) -> VortexResult<StructArray> {
    let chunks: Vec<StructArray> = chunks.iter().map(StructArray::try_from).try_collect()?;

    let len = chunks.iter().map(|chunk| chunk.len()).sum();

    let mut field_arrays = Vec::new();

    for (field_idx, field_dtype) in struct_dtype.dtypes().iter().enumerate() {
        let mut field_chunks = Vec::new();
        for chunk in &chunks {
            field_chunks.push(
                chunk
                    .field(field_idx)
                    .expect("all chunks must have same dtype"),
            );
        }
        let field_array = ChunkedArray::try_new(field_chunks, field_dtype.clone())?;
        field_arrays.push(field_array.into_array());
    }

    StructArray::try_new(struct_dtype.names().clone(), field_arrays, len, validity)
}

/// Builds a new [BoolArray] by repacking the values from the chunks in a single contiguous array.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn pack_bools(chunks: &[Array], validity: Validity) -> VortexResult<BoolArray> {
    let len = chunks.iter().map(|chunk| chunk.len()).sum();
    let mut buffer = BooleanBufferBuilder::new(len);
    for chunk in chunks {
        let chunk = chunk.clone().into_bool()?;
        buffer.append_buffer(&chunk.boolean_buffer());
    }

    BoolArray::try_new(buffer.finish(), validity)
}

/// Builds a new [PrimitiveArray] by repacking the values from the chunks into a single
/// contiguous array.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn pack_primitives(
    chunks: &[Array],
    ptype: PType,
    validity: Validity,
) -> VortexResult<PrimitiveArray> {
    let len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
    let mut buffer = MutableBuffer::with_capacity(len * ptype.byte_width());
    for chunk in chunks {
        let chunk = chunk.clone().into_primitive()?;
        buffer.extend_from_slice(chunk.buffer());
    }

    Ok(PrimitiveArray::new(
        Buffer::from(buffer).into(),
        ptype,
        validity,
    ))
}

/// Builds a new [VarBinViewArray] by repacking the values from the chunks into a single
/// contiguous array.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn pack_views(
    chunks: &[Array],
    dtype: &DType,
    validity: Validity,
) -> VortexResult<VarBinViewArray> {
    let mut views = Vec::new();
    let mut buffers = Vec::new();
    for chunk in chunks {
        // Each chunk's views have buffer IDs that are zero-referenced.
        // As part of the packing operation, we need to rewrite them to be referenced to the global
        // merged buffers list.
        let buffers_offset = buffers.len();
        let canonical_chunk = chunk.clone().into_varbinview()?;

        for buffer in canonical_chunk.buffers() {
            let canonical_buffer = buffer.into_canonical()?.into_varbinview()?.into_array();
            buffers.push(canonical_buffer);
        }

        for view in canonical_chunk.view_slice() {
            if view.is_inlined() {
                // Inlined views can be copied directly into the output
                views.push(*view);
            } else {
                // Referencing views must have their buffer_index adjusted with new offsets
                let view_ref = view.as_view();
                views.push(BinaryView::new_view(
                    view.len(),
                    *view_ref.prefix(),
                    (buffers_offset as u32) + view_ref.buffer_index(),
                    view_ref.offset(),
                ));
            }
        }
    }

    // Reinterpret views from Vec<BinaryView> to Vec<u8>.
    // BinaryView is 16 bytes, so we need to be careful to set the length
    // and capacity of the new Vec accordingly.
    let (ptr, length, capacity) = views.into_raw_parts();
    let views_u8: Vec<u8> = unsafe { Vec::from_raw_parts(ptr.cast(), 16 * length, 16 * capacity) };

    let arrow_views_array = UInt8Array::new(ScalarBuffer::from(views_u8), None);

    VarBinViewArray::try_new(
        Array::from_arrow(&arrow_views_array, false),
        buffers,
        dtype.clone(),
        validity,
    )
}

#[cfg(test)]
mod tests {
    use arrow_array::builder::StringViewBuilder;
    use vortex_dtype::{DType, Nullability};

    use crate::accessor::ArrayAccessor;
    use crate::array::chunked::canonical::pack_views;
    use crate::arrow::FromArrowArray;
    use crate::compute::slice;
    use crate::validity::Validity;
    use crate::Array;

    fn varbin_array() -> Array {
        let mut builder = StringViewBuilder::new();
        builder.append_value("foo");
        builder.append_value("bar");
        builder.append_value("baz");
        builder.append_value("quak");
        let arrow_view_array = builder.finish();

        Array::from_arrow(&arrow_view_array, false)
    }

    #[test]
    pub fn pack_sliced_varbin() {
        let array1 = slice(&varbin_array(), 1, 3).unwrap();
        let array2 = slice(&varbin_array(), 2, 4).unwrap();
        let packed = pack_views(
            &[array1, array2],
            &DType::Utf8(Nullability::NonNullable),
            Validity::NonNullable,
        )
        .unwrap();
        assert_eq!(packed.len(), 4);
        let values = packed
            .with_iterator(|iter| {
                iter.flatten()
                    .map(|v| unsafe { String::from_utf8_unchecked(v.to_vec()) })
                    .collect::<Vec<_>>()
            })
            .unwrap();
        assert_eq!(values, &["bar", "baz", "baz", "quak"]);
    }
}
