use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use super::projections::Projection;

#[derive(Clone, Debug)]
pub struct Schema(pub(crate) DType);

impl Schema {
    pub fn project(&self, projection: Projection) -> VortexResult<Self> {
        match projection {
            Projection::All => Ok(self.clone()),
            Projection::Partial(indices) => {
                let DType::Struct(s, n) = &self.0 else {
                    vortex_bail!("Can't project non struct types")
                };
                s.project(indices.as_ref())
                    .map(|p| Self(DType::Struct(p, *n)))
            }
        }
    }
}
