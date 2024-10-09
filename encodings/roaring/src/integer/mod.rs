use std::fmt::{Debug, Display};

pub use compress::*;
use croaring::{Bitmap, Portable};
use serde::{Deserialize, Serialize};
use vortex::array::PrimitiveArray;
use vortex::compute::unary::try_cast;
use vortex::encoding::ids;
use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute, Stat, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType as _, ArrayTrait, Canonical, IntoArray, IntoCanonical,
    TypedArray,
};
use vortex_buffer::Buffer;
use vortex_dtype::Nullability::NonNullable;
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, VortexExpect as _, VortexResult};

mod compress;
mod compute;

impl_encoding!("vortex.roaring_int", ids::ROARING_INT, RoaringInt);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoaringIntMetadata {
    ptype: PType,
}

impl Display for RoaringIntMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl RoaringIntArray {
    pub fn try_new(bitmap: Bitmap, ptype: PType) -> VortexResult<Self> {
        if !ptype.is_unsigned_int() {
            vortex_bail!(MismatchedTypes: "unsigned int", ptype);
        }

        let length = bitmap.statistics().cardinality as usize;
        let max = bitmap.maximum();
        if max.map(|mv| mv as u64 > ptype.max_value()).unwrap_or(false) {
            vortex_bail!(
                "RoaringInt maximum value is greater than the maximum value for the primitive type"
            );
        }

        let mut stats = StatsSet::new();
        stats.set(Stat::NullCount, 0.into());
        stats.set(Stat::Max, max.into());
        stats.set(Stat::Min, bitmap.minimum().into());
        stats.set(Stat::IsConstant, (length <= 1).into());
        stats.set(Stat::IsSorted, true.into());
        stats.set(Stat::IsStrictSorted, true.into());

        Ok(Self {
            typed: TypedArray::try_from_parts(
                DType::Primitive(ptype, NonNullable),
                length,
                RoaringIntMetadata { ptype },
                Some(Buffer::from(bitmap.serialize::<Portable>())),
                vec![].into(),
                StatsSet::new(),
            )?,
        })
    }

    pub fn owned_bitmap(&self) -> Bitmap {
        Bitmap::deserialize::<Portable>(
            self.as_ref()
                .buffer()
                .vortex_expect("RoaringBoolArray buffer is missing")
                .as_ref(),
        )
    }

    pub fn ptype(&self) -> PType {
        self.metadata().ptype
    }

    pub fn encode(array: Array) -> VortexResult<Array> {
        if let Ok(parray) = PrimitiveArray::try_from(array) {
            Ok(roaring_int_encode(parray)?.into_array())
        } else {
            vortex_bail!("RoaringInt can only encode primitive arrays")
        }
    }
}

impl ArrayTrait for RoaringIntArray {}

impl ArrayVariants for RoaringIntArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for RoaringIntArray {}

impl ArrayValidity for RoaringIntArray {
    fn is_valid(&self, _index: usize) -> bool {
        true
    }

    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllValid(self.len())
    }
}

impl IntoCanonical for RoaringIntArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        try_cast(
            PrimitiveArray::from_vec(self.owned_bitmap().to_vec(), Validity::NonNullable),
            self.dtype(),
        )
        .and_then(Array::into_canonical)
    }
}

impl AcceptArrayVisitor for RoaringIntArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(
            self.as_ref()
                .buffer()
                .vortex_expect("Missing buffer in RoaringIntArray"),
        )
    }
}

impl ArrayStatisticsCompute for RoaringIntArray {
    fn compute_statistics(&self, stat: Stat) -> VortexResult<StatsSet> {
        // possibly faster to write an accumulator over the iterator, though not necessarily
        if stat == Stat::TrailingZeroFreq || stat == Stat::BitWidthFreq || stat == Stat::RunCount {
            let primitive =
                PrimitiveArray::from_vec(self.owned_bitmap().to_vec(), Validity::NonNullable);
            primitive.statistics().compute(stat);
            Ok(primitive.statistics().to_set())
        } else {
            Ok(StatsSet::new())
        }
    }
}
