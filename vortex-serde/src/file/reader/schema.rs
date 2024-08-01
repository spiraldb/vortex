use vortex_dtype::{DType, FieldNames, StructDType};
use vortex_error::VortexResult;

use super::projections::Projection;

#[derive(Clone, Debug)]
pub struct Schema(pub(crate) StructDType);

impl Schema {
    pub fn fields(&self) -> &FieldNames {
        self.0.names()
    }

    pub fn types(&self) -> &[DType] {
        self.0.dtypes().as_ref()
    }

    pub fn project(&self, projection: Projection) -> VortexResult<Self> {
        self.0.project(projection.indices()).map(Self)
    }
}
