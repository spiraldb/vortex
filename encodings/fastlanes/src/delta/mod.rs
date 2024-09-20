use std::fmt::Debug;

pub use compress::*;
use serde::{Deserialize, Serialize};
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoCanonical};
use vortex_dtype::match_each_unsigned_integer_ptype;
use vortex_error::{vortex_bail, vortex_panic, VortexExpect as _, VortexResult};

mod compress;
mod compute;

impl_encoding!("fastlanes.delta", ids::FL_DELTA, Delta);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMetadata {
    validity: ValidityMetadata,
    len: usize,
}

impl DeltaArray {
    pub fn try_new(bases: Array, deltas: Array, validity: Validity) -> VortexResult<Self> {
        if bases.dtype() != deltas.dtype() {
            vortex_bail!(
                "DeltaArray: bases and deltas must have the same dtype, got {:?} and {:?}",
                bases.dtype(),
                deltas.dtype()
            );
        }

        let dtype = bases.dtype().clone();
        let len = deltas.len();
        let metadata = DeltaMetadata {
            validity: validity.to_metadata(len)?,
            len,
        };

        let mut children = vec![bases, deltas];
        if let Some(varray) = validity.into_array() {
            children.push(varray)
        }

        let delta = Self::try_from_parts(dtype, len, metadata, children.into(), StatsSet::new())?;
        if delta.bases().len() != delta.bases_len() {
            vortex_bail!(
                "DeltaArray: bases.len() ({}) != expected_bases_len ({}), based on len ({}) and lane count ({})",
                delta.bases().len(),
                delta.bases_len(),
                len,
                delta.lanes()
            );
        }

        Ok(delta)
    }

    #[inline]
    pub fn bases(&self) -> Array {
        self.as_ref()
            .child(0, self.dtype(), self.bases_len())
            .vortex_expect("DeltaArray is missing bases child array")
    }

    #[inline]
    pub fn deltas(&self) -> Array {
        self.as_ref()
            .child(1, self.dtype(), self.len())
            .vortex_expect("DeltaArray is missing deltas child array")
    }

    #[inline]
    fn lanes(&self) -> usize {
        let ptype = self.dtype().try_into().unwrap_or_else(|err| {
            vortex_panic!(
                err,
                "Failed to convert DeltaArray DType {} to PType",
                self.dtype()
            )
        });
        match_each_unsigned_integer_ptype!(ptype, |$T| {
            <$T as fastlanes::FastLanes>::LANES
        })
    }

    pub fn validity(&self) -> Validity {
        self.metadata().validity.to_validity(|| {
            self.as_ref()
                .child(2, &Validity::DTYPE, self.len())
                .vortex_expect("DeltaArray: validity child")
        })
    }

    fn bases_len(&self) -> usize {
        let num_chunks = self.len() / 1024;
        let remainder_base_size = if self.len() % 1024 > 0 { 1 } else { 0 };
        num_chunks * self.lanes() + remainder_base_size
    }
}

impl ArrayTrait for DeltaArray {}

impl ArrayVariants for DeltaArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for DeltaArray {}

impl IntoCanonical for DeltaArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        delta_decompress(self).map(Canonical::Primitive)
    }
}

impl ArrayValidity for DeltaArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for DeltaArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("bases", &self.bases())?;
        visitor.visit_child("deltas", &self.deltas())
    }
}

impl ArrayStatisticsCompute for DeltaArray {}
