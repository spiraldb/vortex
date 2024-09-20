use std::fmt::Debug;
use std::sync::Arc;

pub use compress::*;
use flexbuffers::{FlexbufferSerializer, Reader};
use serde::{Deserialize, Serialize};
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoCanonical,
    TryDeserializeArrayMetadata, TrySerializeArrayMetadata,
};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, vortex_err, vortex_panic, VortexExpect as _, VortexResult};
use vortex_scalar::Scalar;

mod compress;
mod compute;

impl_encoding!("fastlanes.for", ids::FL_FOR, FoR);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoRMetadata {
    reference: Scalar,
    shift: u8,
}

impl TrySerializeArrayMetadata for FoRMetadata {
    fn try_serialize_metadata(&self) -> VortexResult<Arc<[u8]>> {
        let mut ser = FlexbufferSerializer::new();
        self.serialize(&mut ser)?;
        Ok(ser.take_buffer().into())
    }
}

impl<'m> TryDeserializeArrayMetadata<'m> for FoRMetadata {
    fn try_deserialize_metadata(metadata: Option<&'m [u8]>) -> VortexResult<Self> {
        let bytes = metadata.ok_or_else(|| vortex_err!("Array requires metadata bytes"))?;
        Ok(FoRMetadata::deserialize(Reader::get_root(bytes)?)?)
    }
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
        self.as_ref()
            .child(0, dtype, self.len())
            .vortex_expect("FoRArray is missing encoded child array")
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
