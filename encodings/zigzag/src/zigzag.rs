use serde::{Deserialize, Serialize};
use vortex::array::primitive::PrimitiveArray;
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, IntoArrayData};
use vortex_dtype::PType;
use vortex_error::{vortex_bail, vortex_err};

use crate::compress::zigzag_encode;

impl_encoding!("vortex.zigzag", ZigZag);

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

        let dtype = DType::from(PType::try_from(&encoded_dtype).expect("ptype").to_signed())
            .with_nullability(encoded_dtype.nullability());

        let children = vec![encoded.into_array_data()];
        Self::try_from_parts(dtype, ZigZagMetadata, children.into(), StatsSet::new())
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
            .child(0, &encoded)
            .expect("Missing encoded array")
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

impl ArrayFlatten for ZigZagArray {
    fn flatten(self) -> VortexResult<Flattened> {
        todo!("ZigZagArray::flatten")
    }
}

impl ArrayTrait for ZigZagArray {
    fn len(&self) -> usize {
        self.encoded().len()
    }
}
