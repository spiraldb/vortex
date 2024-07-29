use vortex_dtype::{FieldNames, StructDType};
use vortex_error::VortexResult;

use super::projections::Projection;

pub struct Schema(pub(crate) StructDType);

impl Schema {
    pub fn fields(&self) -> &FieldNames {
        self.0.names()
    }

    pub fn project(&self, projection: Projection) -> VortexResult<Self> {
        self.0.project(projection.indices()).map(Self)
    }
}
