use arrow_buffer::{BooleanBufferBuilder, Buffer, MutableBuffer};
use itertools::Itertools;
use vortex_dtype::{DType, Nullability, PType, StructDType};
use vortex_error::{vortex_bail, vortex_err, ErrString, VortexResult};

use crate::array::chunked::ChunkedArray;
use crate::array::extension::ExtensionArray;
use crate::array::null::NullArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::BoolArray;
use crate::compute::slice;
use crate::compute::unary::{scalar_at_unchecked, try_cast};
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
            let varbin_array = pack_varbin(chunks.as_slice(), validity, dtype)?;
            Ok(Canonical::VarBin(varbin_array))
        }
        DType::Binary(_) => {
            let varbin_array = pack_varbin(chunks.as_slice(), validity, dtype)?;
            Ok(Canonical::VarBin(varbin_array))
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
                    .ok_or_else(|| vortex_err!("All chunks must have same dtype; missing field at index {}, current chunk dtype: {}", field_idx, chunk.dtype()))?,
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

/// Builds a new [VarBinArray] by repacking the values from the chunks into a single
/// contiguous array.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn pack_varbin(chunks: &[Array], validity: Validity, dtype: &DType) -> VortexResult<VarBinArray> {
    let len: usize = chunks.iter().map(|c| c.len()).sum();
    let mut offsets = Vec::with_capacity(len + 1);
    offsets.push(0);
    let mut data_bytes = Vec::new();

    for chunk in chunks {
        let chunk = chunk.clone().into_varbin()?;
        let offsets_arr = try_cast(
            chunk.offsets().into_primitive()?.array(),
            &DType::Primitive(PType::I32, Nullability::NonNullable),
        )?
        .into_primitive()?;

        let first_offset_value: usize =
            usize::try_from(&scalar_at_unchecked(offsets_arr.array(), 0))?;
        let last_offset_value: usize = usize::try_from(&scalar_at_unchecked(
            offsets_arr.array(),
            offsets_arr.len() - 1,
        ))?;
        let primitive_bytes =
            slice(&chunk.bytes(), first_offset_value, last_offset_value)?.into_primitive()?;
        data_bytes.extend_from_slice(primitive_bytes.buffer());

        let adjustment_from_previous = *offsets
            .last()
            .ok_or_else(|| vortex_err!("VarBinArray offsets must have at least one element"))?;
        offsets.extend(
            offsets_arr
                .maybe_null_slice::<i32>()
                .iter()
                .skip(1)
                .map(|off| off + adjustment_from_previous - first_offset_value as i32),
        );
    }

    VarBinArray::try_new(
        PrimitiveArray::from(offsets).into_array(),
        PrimitiveArray::from(data_bytes).into_array(),
        dtype.clone(),
        validity,
    )
}

#[cfg(test)]
mod tests {
    use vortex_dtype::{DType, Nullability};

    use crate::accessor::ArrayAccessor;
    use crate::array::builder::VarBinBuilder;
    use crate::array::chunked::canonical::pack_varbin;
    use crate::array::VarBinArray;
    use crate::compute::slice;
    use crate::validity::Validity;

    fn varbin_array() -> VarBinArray {
        let mut builder = VarBinBuilder::<i32>::with_capacity(4);
        builder.push_value("foo");
        builder.push_value("bar");
        builder.push_value("baz");
        builder.push_value("quak");
        builder.finish(DType::Utf8(Nullability::NonNullable))
    }

    #[test]
    pub fn pack_sliced_varbin() {
        let array1 = slice(varbin_array().array(), 1, 3).unwrap();
        let array2 = slice(varbin_array().array(), 2, 4).unwrap();
        let packed = pack_varbin(
            &[array1, array2],
            Validity::NonNullable,
            &DType::Utf8(Nullability::NonNullable),
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
