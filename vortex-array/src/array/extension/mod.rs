use serde::{Deserialize, Serialize};
use vortex_dtype::{ExtDType, ExtID};

use crate::stats::ArrayStatisticsCompute;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDType, ArrayFlatten};

mod compute;

impl_encoding!("vortex.ext", Extension);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionMetadata {
    storage_dtype: DType,
}

impl ExtensionArray {
    pub fn new(ext_dtype: ExtDType, storage: Array) -> Self {
        Self::try_from_parts(
            DType::Extension(ext_dtype, storage.dtype().nullability()),
            ExtensionMetadata {
                storage_dtype: storage.dtype().clone(),
            },
            [storage].into(),
            Default::default(),
        )
        .expect("Invalid ExtensionArray")
    }

    pub fn storage(&self) -> Array {
        self.array()
            .child(0, &self.metadata().storage_dtype)
            .expect("Missing storage array")
    }

    #[allow(dead_code)]
    #[inline]
    pub fn id(&self) -> &ExtID {
        self.ext_dtype().id()
    }

    #[inline]
    pub fn ext_dtype(&self) -> &ExtDType {
        let DType::Extension(ext, _) = self.dtype() else {
            unreachable!();
        };
        ext
    }
}

impl ArrayFlatten for ExtensionArray {
    fn flatten(self) -> VortexResult<Flattened> {
        Ok(Flattened::Extension(self))
    }
}

impl ArrayValidity for ExtensionArray {
    fn is_valid(&self, index: usize) -> bool {
        self.storage().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.storage().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for ExtensionArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("storage", &self.storage())
    }
}

impl ArrayStatisticsCompute for ExtensionArray {
    // TODO(ngates): pass through stats to the underlying and cast the scalars.
}

impl ArrayTrait for ExtensionArray {
    fn len(&self) -> usize {
        self.storage().len()
    }
}

impl EncodingCompression for ExtensionEncoding {}
