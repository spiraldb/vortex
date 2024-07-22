use std::sync::Arc;

use serde::{Deserialize, Serialize};
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::stats::{ArrayStatisticsCompute, Stat, StatsSet};
use crate::validity::{ArrayValidity, LogicalValidity, Validity};
use crate::variants::{ArrayVariants, NullArrayTrait};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDef, ArrayTrait, Canonical, IntoCanonical};

mod compute;

impl_encoding!("vortex.null", 1u16, Null);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullMetadata {
    len: usize,
}

impl NullArray {
    pub fn new(len: usize) -> Self {
        Self::try_from_parts(
            DType::Null,
            len,
            NullMetadata { len },
            Arc::new([]),
            StatsSet::nulls(len, &DType::Null),
        )
        .expect("NullArray::new cannot fail")
    }
}

impl IntoCanonical for NullArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        Ok(Canonical::Null(self))
    }
}

impl ArrayValidity for NullArray {
    fn is_valid(&self, _: usize) -> bool {
        false
    }

    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllInvalid(self.len())
    }
}

impl ArrayStatisticsCompute for NullArray {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::nulls(self.len(), &DType::Null))
    }
}

impl AcceptArrayVisitor for NullArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_validity(&Validity::AllInvalid)
    }
}

impl ArrayTrait for NullArray {
    fn nbytes(&self) -> usize {
        0
    }
}

impl ArrayVariants for NullArray {
    fn as_null_array(&self) -> Option<&dyn NullArrayTrait> {
        Some(self)
    }
}

impl NullArrayTrait for NullArray {}
