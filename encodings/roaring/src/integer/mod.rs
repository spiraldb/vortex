use std::fmt::Debug;

pub use compress::*;
use croaring::{Bitmap, Portable};
use serde::{Deserialize, Serialize};
use vortex::array::{Primitive, PrimitiveArray};
use vortex::iter::ArrayIter;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoCanonical, TypedArray,
};
use vortex_buffer::Buffer;
use vortex_dtype::Nullability::NonNullable;
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, vortex_err, VortexResult};

mod compress;
mod compute;

impl_encoding!("vortex.roaring_int", 18u16, RoaringInt);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoaringIntMetadata {
    ptype: PType,
}

impl RoaringIntArray {
    pub fn try_new(bitmap: Bitmap, ptype: PType) -> VortexResult<Self> {
        if !ptype.is_unsigned_int() {
            vortex_bail!("RoaringInt expected unsigned int");
        }
        let length = bitmap.statistics().cardinality as usize;
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

    pub fn bitmap(&self) -> Bitmap {
        //TODO(@jdcasale): figure out a way to avoid this deserialization per-call
        Bitmap::deserialize::<Portable>(
            self.array()
                .buffer()
                .expect("RoaringBoolArray buffer is missing")
                .as_ref(),
        )
    }

    pub fn ptype(&self) -> PType {
        self.metadata().ptype
    }

    pub fn encode(array: Array) -> VortexResult<Array> {
        if array.encoding().id() == Primitive::ID {
            Ok(roaring_int_encode(PrimitiveArray::try_from(array)?)?.into_array())
        } else {
            Err(vortex_err!("RoaringInt can only encode primitive arrays"))
        }
    }
}

impl ArrayTrait for RoaringIntArray {}

impl ArrayVariants for RoaringIntArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for RoaringIntArray {
    fn float32_iter(&self) -> Option<ArrayIter<f32>> {
        todo!()
    }

    fn float64_iter(&self) -> Option<vortex::iter::ArrayIter<f64>> {
        todo!()
    }

    fn unsigned32_iter(&self) -> Option<ArrayIter<u32>> {
        todo!()
    }
}

impl ArrayValidity for RoaringIntArray {
    fn is_valid(&self, _index: usize) -> bool {
        true
    }

    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllValid(self.bitmap().iter().count())
    }
}

impl IntoCanonical for RoaringIntArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        todo!()
    }
}

impl AcceptArrayVisitor for RoaringIntArray {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayStatisticsCompute for RoaringIntArray {}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;
    use vortex::compute::unary::scalar_at;
    use vortex::IntoArray;

    use crate::RoaringIntArray;

    #[test]
    #[cfg_attr(miri, ignore)]
    pub fn test_scalar_at() {
        let ints = PrimitiveArray::from(vec![2u32, 12, 22, 32]).into_array();
        let array = RoaringIntArray::encode(ints).unwrap();

        assert_eq!(scalar_at(&array, 0).unwrap(), 2u32.into());
        assert_eq!(scalar_at(&array, 1).unwrap(), 12u32.into());
    }
}
