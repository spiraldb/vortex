use arrow_buffer::{BooleanBuffer, MutableBuffer, ScalarBuffer};

use vortex_dtype::{DType, match_each_native_ptype, Nullability, PType, StructDType};
use vortex_error::{vortex_bail, VortexResult};

use itertools::Itertools;
use crate::{Array, ArrayDType, ArrayFlatten, ArrayTrait, ArrayValidity, Flattened, IntoArray};
use crate::accessor::ArrayAccessor;
use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::extension::ExtensionArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::r#struct::StructArray;
use crate::array::varbin::builder::VarBinBuilder;
use crate::array::varbin::VarBinArray;
use crate::validity::{LogicalValidity, Validity};

impl ArrayFlatten for ChunkedArray {
    fn flatten(self) -> VortexResult<Flattened> {
        try_flatten_chunks(self.chunks().collect(), self.dtype().clone())
    }
}

pub fn try_flatten_chunks(chunks: Vec<Array>, dtype: DType) -> VortexResult<Flattened> {
    match &dtype {
        // Structs can have their internal field pointers swizzled to push the chunking down
        // one level internally without copying or decompressing any data.
        DType::Struct(struct_dtype, _) => {
            let struct_array = swizzle_struct_chunks(chunks.as_slice(), struct_dtype)?;
            Ok(Flattened::Struct(struct_array))
        }

        // Extension arrays contain an internal array, so we can push down a ChunkedArray
        // to be the storage type of the extension DType.
        DType::Extension(ext_dtype, _) => {
            let ext_array = ExtensionArray::new(
                ext_dtype.clone(),
                ChunkedArray::try_new(chunks, dtype.clone())?.into_array()
            );

            Ok(Flattened::Extension(ext_array))
        }

        // Lists just flatten into their inner PType
        DType::List(_, _) => {
            todo!()
        }

        DType::Bool(nullability) => {
            let bool_array = pack_bools(chunks.as_slice(), *nullability)?;
            Ok(Flattened::Bool(bool_array))
        }
        DType::Primitive(ptype, nullability) => {
            let prim_array = pack_primitives(chunks.as_slice(), *ptype, *nullability)?;
            Ok(Flattened::Primitive(prim_array))
        }
        DType::Utf8(nullability) => {
            let varbin_array = pack_varbin(chunks.as_slice(), &dtype, *nullability)?;
            Ok(Flattened::VarBin(varbin_array))
        }
        DType::Binary(nullability) => {
            let varbin_array = pack_varbin(chunks.as_slice(), &dtype, *nullability)?;
            Ok(Flattened::VarBin(varbin_array))
        }
        DType::Null => {
            vortex_bail!(ComputeError: "DType::Null cannot be flattened")
        }
    }
}

/// Swizzle the pointers within a ChunkedArray of StructArrays to instead be a single
/// StructArray pointed at ChunkedArrays of each constituent format.
fn swizzle_struct_chunks(chunks: &[Array], struct_dtype: &StructDType) -> VortexResult<StructArray> {
    let chunks: Vec<StructArray> = chunks.iter()
        .map(StructArray::try_from)
        .try_collect()?;

    let len = chunks.iter().map(|chunk| chunk.len()).sum();
    let validity = chunks.iter()
        .map(|chunk| chunk.logical_validity())
        .collect::<Validity>();

    let mut field_arrays = Vec::new();
    let field_names = struct_dtype.names().clone();
    let field_dtypes = struct_dtype.dtypes().clone();

    for (field_idx, field_dtype) in field_dtypes.iter().enumerate() {
        let mut field_chunks = Vec::new();
        for chunk in &chunks {
            field_chunks.push(chunk.field(field_idx).expect("structarray should contain field"));
        }
        let field_array = ChunkedArray::try_new(field_chunks, field_dtype.clone())?;
        field_arrays.push(field_array.into_array());
    }

    Ok(StructArray::try_new(field_names, field_arrays, len, validity)?)
}

/// Builds a new [BoolArray] by repacking the values from the chunks in a single contiguous array.
fn pack_bools(chunks: &[Array], nullability: Nullability) -> VortexResult<BoolArray> {
    let len = chunks.iter().map(|chunk| chunk.len()).sum();
    let mut logical_validities = Vec::new();
    let mut bools = Vec::with_capacity(len);
    for chunk in chunks {
        let chunk = chunk.clone().flatten_bool()?;
        logical_validities.push(chunk.logical_validity());
        bools.extend(chunk.boolean_buffer().iter());
    }

    BoolArray::try_new(
        BooleanBuffer::from(bools),
        validity_from_chunks(logical_validities, nullability),
    )
}

/// Builds a new [PrimitiveArray] by repacking the values from the chunks into a single
/// contiguous array.
fn pack_primitives(chunks: &[Array], ptype: PType, nullability: Nullability) -> VortexResult<PrimitiveArray> {
    let len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
    let mut logical_validities = Vec::new();
    let mut buffer = MutableBuffer::with_capacity(len * ptype.byte_width());
    for chunk in chunks {
        let chunk = chunk.clone().flatten_primitive()?;
        logical_validities.push(chunk.logical_validity());
        buffer.extend_from_slice(chunk.buffer());
    }

    match_each_native_ptype!(ptype, |$T| {
        Ok(PrimitiveArray::try_new(
            ScalarBuffer::<$T>::from(buffer),
            validity_from_chunks(logical_validities, nullability))?)
    })
}

// TODO(aduffy): This can be slow for really large arrays.
// TODO(aduffy): this doesn't propagate the validity fully
fn pack_varbin(chunks: &[Array], dtype: &DType, _nullability: Nullability) -> VortexResult<VarBinArray> {
    let len = chunks.iter()
        .map(|chunk| chunk.len())
        .sum();
    let mut builder = VarBinBuilder::<i32>::with_capacity(len);

    for chunk in chunks {
        let chunk = chunk.clone().flatten_varbin()?;
        chunk.with_iterator(|iter| {
            for datum in iter {
                builder.push(datum);
            }
        })?;
    }

    Ok(builder.finish(dtype.clone()))
}

fn validity_from_chunks(logical_validities: Vec<LogicalValidity>, nullability: Nullability) -> Validity {
    if nullability == Nullability::NonNullable {
        Validity::NonNullable
    } else {
        logical_validities.into_iter().collect()
    }
}