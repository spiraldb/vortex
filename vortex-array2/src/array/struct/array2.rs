use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::r#struct::StructArray2;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::stats::{ArrayStatistics, ArrayStatisticsCompute};
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{ArrayEncodingRef, ArrayFlatten, ArrayMetadata, ArrayTrait, Flattened};

impl ArrayEncodingRef for StructArray2<'_> {
    fn encoding(&self) -> EncodingRef {
        todo!()
    }
}

impl ArrayCompute for StructArray2<'_> {}

impl ArrayFlatten for StructArray2<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        todo!()
    }
}

impl ArrayValidity for StructArray2<'_> {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }

    fn logical_validity(&self) -> LogicalValidity {
        todo!()
    }
}

impl AcceptArrayVisitor for StructArray2<'_> {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayStatistics for StructArray2<'_> {}

impl ArrayStatisticsCompute for StructArray2<'_> {}

impl ArrayTrait for StructArray2<'_> {
    fn dtype(&self) -> &DType {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn metadata(&self) -> Arc<dyn ArrayMetadata> {
        todo!()
    }
}
