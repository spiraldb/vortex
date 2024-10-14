use std::fmt::{Debug, Display};

pub use compress::*;
use serde::{Deserialize, Serialize};
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Array, ArrayDType, ArrayTrait, Canonical, IntoCanonical};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, vortex_panic, VortexExpect as _, VortexResult};
use vortex_scalar::ScalarValue;

mod compress;
mod compute;

impl_encoding!("fastlanes.for", ids::FL_FOR, FoR);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoRMetadata {
    reference: ScalarValue,
    shift: u8,
}

impl Display for FoRMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl FoRArray {
    pub fn try_new(child: Array, reference: ScalarValue, shift: u8) -> VortexResult<Self> {
        let Some(refpv) = reference.as_pvalue()? else {
            vortex_bail!("Reference value cannot be null");
        };

        let child_ptype = PType::try_from(child.dtype())?;
        if refpv.ptype().byte_width() != child_ptype.byte_width() {
            vortex_bail!(
                "Reference value ({}) must have the same width as the child array dtype ({})",
                reference,
                child.dtype()
            );
        }

        Self::try_from_parts(
            DType::Primitive(refpv.ptype(), child.dtype().nullability()),
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
        self.as_ref()
            .child(0, dtype, self.len())
            .vortex_expect("FoRArray is missing encoded child array")
    }

    #[inline]
    pub fn reference(&self) -> &ScalarValue {
        &self.metadata().reference
    }

    #[inline]
    pub fn shift(&self) -> u8 {
        self.metadata().shift
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype().try_into().unwrap_or_else(|err| {
            vortex_panic!(
                err,
                "Failed to convert FoRArray DType {} to PType",
                self.dtype()
            )
        })
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

impl PrimitiveArrayTrait for FoRArray {}
