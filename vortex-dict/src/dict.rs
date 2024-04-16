use serde::{Deserialize, Serialize};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::{impl_encoding, ArrayFlatten};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Signedness};

impl_encoding!("vortex.dict", Dict);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DictMetadata {
    codes_dtype: DType,
}

impl DictArray<'_> {
    pub fn try_new(codes: Array, dict: Array) -> VortexResult<Self> {
        if !matches!(codes.dtype(), DType::Int(_, Signedness::Unsigned, _)) {
            vortex_bail!(MismatchedTypes: "unsigned int", codes.dtype());
        }
        // Ok(Self::try {
        //     codes,
        //     values: dict,
        //     stats: Arc::new(RwLock::new(StatsSet::new())),
        // })
        todo!()
    }

    #[inline]
    pub fn values(&self) -> Array {
        self.array()
            .child(0, &DType::BYTES)
            .expect("Missing values")
    }

    #[inline]
    pub fn codes(&self) -> Array {
        self.array().child(1, &DType::BYTES).expect("Missing codes")
    }
}

impl ArrayFlatten for DictArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        todo!()
    }
}

impl ArrayValidity for DictArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        todo!()
    }

    fn logical_validity(&self) -> LogicalValidity {
        todo!()
    }
}

impl vortex::visitor::AcceptArrayVisitor for DictArray<'_> {
    fn accept(&self, visitor: &mut dyn vortex::visitor::ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl vortex::stats::ArrayStatisticsCompute for DictArray<'_> {}

impl ArrayTrait for DictArray<'_> {
    fn len(&self) -> usize {
        self.codes().len()
    }
}
