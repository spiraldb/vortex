use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use self::projection::Projection;

pub mod projection;

#[derive(Clone, Debug)]
pub struct Schema(pub(crate) DType);

impl Schema {
    pub fn new(schema_dtype: DType) -> Self {
        Self(schema_dtype)
    }

    pub fn project(&self, projection: Projection) -> VortexResult<Self> {
        match projection {
            Projection::All => Ok(self.clone()),
            Projection::Flat(indices) => {
                let DType::Struct(s, n) = &self.0 else {
                    vortex_bail!("Can't project non struct types")
                };
                s.project(indices.as_ref())
                    .map(|p| Self(DType::Struct(p, *n)))
            }
        }
    }

    pub fn dtype(&self) -> &DType {
        &self.0
    }
}

impl From<Schema> for DType {
    fn from(value: Schema) -> Self {
        value.0
    }
}
