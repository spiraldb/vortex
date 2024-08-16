use std::sync::Arc;

use vortex_error::{vortex_err, VortexResult};

use crate::field::Field;
use crate::{flatbuffers as fb, DType, StructDType};

pub fn deserialize_and_project(fb: fb::DType<'_>, projection: &[Field]) -> VortexResult<DType> {
    let fb_struct = fb
        .type__as_struct_()
        .ok_or_else(|| vortex_err!("The top-level type should be a struct"))?;
    let nullability = fb_struct.nullable().into();

    let (names, dtypes): (Vec<Arc<str>>, Vec<DType>) = projection
        .iter()
        .map(|field| {
            let idx = match field {
                Field::Name(n) => {
                    let names = fb_struct
                        .names()
                        .ok_or_else(|| vortex_err!("Missing field names"))?;
                    names
                        .iter()
                        .position(|name| name == n)
                        .ok_or_else(|| vortex_err!("Unknown field name {n}"))?
                }
                Field::Index(i) => *i,
            };
            read_field(fb_struct, idx)
        })
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
