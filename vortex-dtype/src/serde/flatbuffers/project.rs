use std::sync::Arc;

use vortex_error::{vortex_err, VortexResult};

use crate::field::Field;
use crate::{flatbuffers as fb, DType, StructDType};

/// Convert name references in projection list into index references.
///
/// This is mostly useful if you want to deduplicate multiple projections against serialized schema.
pub fn resolve_field_references<'a, 'b: 'a>(
    fb: fb::Struct_<'b>,
    projection: &'a [Field],
) -> impl Iterator<Item = VortexResult<usize>> + 'a {
    projection.iter().map(move |field| match field {
        Field::Name(n) => {
            let names = fb
                .names()
                .ok_or_else(|| vortex_err!("Missing field names"))?;
            names
                .iter()
                .position(|name| name == n)
                .ok_or_else(|| vortex_err!("Unknown field name {n}"))
        }
        Field::Index(i) => Ok(*i),
    })
}

/// Deserialize flatbuffer schema selecting only columns defined by projection
pub fn deserialize_and_project(fb: fb::DType<'_>, projection: &[Field]) -> VortexResult<DType> {
    let fb_struct = fb
        .type__as_struct_()
        .ok_or_else(|| vortex_err!("The top-level type should be a struct"))?;
    let nullability = fb_struct.nullable().into();

    let (names, dtypes): (Vec<Arc<str>>, Vec<DType>) =
        resolve_field_references(fb_struct, projection)
            .map(|idx| idx.and_then(|i| read_field(fb_struct, i)))
            .collect::<VortexResult<Vec<_>>>()?
            .into_iter()
            .unzip();

    Ok(DType::Struct(
        StructDType::new(names.into(), dtypes),
        nullability,
    ))
}

fn read_field(fb_struct: fb::Struct_, idx: usize) -> VortexResult<(Arc<str>, DType)> {
    let name = fb_struct
        .names()
        .ok_or_else(|| vortex_err!("Missing field names"))?
        .get(idx);
    let fb_dtype = fb_struct
        .dtypes()
        .ok_or_else(|| vortex_err!("Missing field dtypes"))?
        .get(idx);
    let dtype = DType::try_from(fb_dtype)?;

    Ok((name.into(), dtype))
}
