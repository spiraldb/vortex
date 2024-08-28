use std::fmt::Debug;

pub use compress::*;
use serde::{Deserialize, Serialize};
use vortex::iter::AccessorRef;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoCanonical};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

mod compress;
mod compute;

impl_encoding!("fastlanes.for", 15u16, FoR);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoRMetadata {
    reference: Scalar,
    shift: u8,
}

impl FoRArray {
    pub fn try_new(child: Array, reference: Scalar, shift: u8) -> VortexResult<Self> {
        if reference.is_null() {
            vortex_bail!("Reference value cannot be null",);
        }
        let reference = reference.cast(
            &reference
                .dtype()
                .with_nullability(child.dtype().nullability()),
        )?;
        Self::try_from_parts(
            reference.dtype().clone(),
            child.len(),
            FoRMetadata { reference, shift },
            [child].into(),
            StatsSet::new(),
        )
    }

    #[inline]
    pub fn encoded(&self) -> Array {
        let dtype = if self.ptype().is_signed_int() {
            &DType::Primitive(self.ptype().to_unsigned(), self.dtype().nullability())
        } else {
            self.dtype()
        };
        self.array()
            .child(0, dtype, self.len())
            .expect("Missing FoR child")
    }

    #[inline]
    pub fn reference(&self) -> &Scalar {
        &self.metadata().reference
    }

    #[inline]
    pub fn shift(&self) -> u8 {
        self.metadata().shift
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype().try_into().unwrap()
    }
}

impl ArrayValidity for FoRArray {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl IntoCanonical for FoRArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        decompress(self).map(Canonical::Primitive)
    }
}

impl AcceptArrayVisitor for FoRArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("encoded", &self.encoded())
    }
}

impl ArrayStatisticsCompute for FoRArray {}

impl ArrayTrait for FoRArray {
    fn nbytes(&self) -> usize {
        self.encoded().nbytes()
    }
}

impl ArrayVariants for FoRArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for FoRArray {
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

    fn f32_accessor(&self) -> Option<AccessorRef<f32>> {
        todo!()
    }

    fn f64_accessor(&self) -> Option<AccessorRef<f64>> {
        todo!()
    }
}
