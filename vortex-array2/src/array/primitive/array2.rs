use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::primitive::PrimitiveArray2;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::stats::{ArrayStatistics, ArrayStatisticsCompute};
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{ArrayEncodingRef, ArrayMetadata, ArrayTrait};

impl ArrayEncodingRef for PrimitiveArray2<'_> {
    fn encoding(&self) -> EncodingRef {
        todo!()
    }
}

impl ArrayCompute for PrimitiveArray2<'_> {}

impl ArrayValidity for PrimitiveArray2<'_> {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }

    fn logical_validity(&self) -> LogicalValidity {
        todo!()
    }
}

impl AcceptArrayVisitor for PrimitiveArray2<'_> {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayStatistics for PrimitiveArray2<'_> {}

impl ArrayStatisticsCompute for PrimitiveArray2<'_> {}

impl ArrayTrait for PrimitiveArray2<'_> {
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
