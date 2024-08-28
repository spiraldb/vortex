use serde::{Deserialize, Serialize};
use vortex::array::PrimitiveArray;
use vortex::iter::AccessorRef;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoArrayVariant,
    IntoCanonical,
};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::compress::zigzag_encode;
use crate::zigzag_decode;

impl_encoding!("vortex.zigzag", 21u16, ZigZag);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZigZagMetadata;

impl ZigZagArray {
    pub fn new(encoded: Array) -> Self {
        Self::try_new(encoded).unwrap()
    }

    pub fn try_new(encoded: Array) -> VortexResult<Self> {
        let encoded_dtype = encoded.dtype().clone();
        if !encoded_dtype.is_unsigned_int() {
            vortex_bail!(MismatchedTypes: "unsigned int", encoded_dtype);
        }

        let dtype = DType::from(PType::try_from(&encoded_dtype)?.to_signed())
            .with_nullability(encoded_dtype.nullability());

        let len = encoded.len();
        let children = [encoded];

        Self::try_from_parts(dtype, len, ZigZagMetadata, children.into(), StatsSet::new())
    }

    pub fn encode(array: &Array) -> VortexResult<Array> {
        PrimitiveArray::try_from(array)
            .map_err(|_| vortex_err!("ZigZag can only encoding primitive arrays"))
            .map(|parray| zigzag_encode(&parray))?
            .map(|encoded| encoded.into_array())
    }

    pub fn encoded(&self) -> Array {
        let ptype = PType::try_from(self.dtype()).expect("ptype");
        let encoded = DType::from(ptype.to_unsigned()).with_nullability(self.dtype().nullability());
        self.array()
            .child(0, &encoded, self.len())
            .expect("Missing encoded array")
    }
}

impl ArrayTrait for ZigZagArray {}

impl ArrayVariants for ZigZagArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for ZigZagArray {
    fn u8_accessor(&self) -> Option<AccessorRef<u8>> {
        todo!()
    }

    fn u16_accessor(&self) -> Option<AccessorRef<u16>> {
        todo!()
    }
    fn u32_accessor(&self) -> Option<AccessorRef<u32>> {
        todo!()
    }

    fn u64_accessor(&self) -> Option<AccessorRef<u64>> {
        todo!()
    }

    fn f32_accessor(&self) -> Option<AccessorRef<f32>> {
        todo!()
    }

    fn f64_accessor(&self) -> Option<AccessorRef<f64>> {
        todo!()
    }

    fn i8_accessor(&self) -> Option<AccessorRef<i8>> {
        todo!()
    }

    fn i16_accessor(&self) -> Option<AccessorRef<i16>> {
        todo!()
    }

    fn i32_accessor(&self) -> Option<AccessorRef<i32>> {
        todo!()
    }

    fn i64_accessor(&self) -> Option<AccessorRef<i64>> {
        todo!()
    }
}

impl ArrayValidity for ZigZagArray {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for ZigZagArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("encoded", &self.encoded())
    }
}

impl ArrayStatisticsCompute for ZigZagArray {}

impl IntoCanonical for ZigZagArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        Ok(Canonical::Primitive(zigzag_decode(
            &self.encoded().into_primitive()?,
        )))
    }
}
