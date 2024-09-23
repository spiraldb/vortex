use std::fmt::Debug;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use vortex::array::PrimitiveArray;
use vortex::encoding::ids;
use vortex::iter::{Accessor, AccessorRef};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity, Validity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoCanonical,
};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, vortex_panic, VortexExpect as _, VortexResult};

use crate::alp::Exponents;
use crate::compress::{alp_encode, decompress};
use crate::ALPFloat;

impl_encoding!("vortex.alp", ids::ALP, ALP);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ALPMetadata {
    exponents: Exponents,
    encoded_dtype: DType,
    patches_dtype: Option<DType>,
}

impl ALPArray {
    pub fn try_new(
        encoded: Array,
        exponents: Exponents,
        patches: Option<Array>,
    ) -> VortexResult<Self> {
        let encoded_dtype = encoded.dtype().clone();
        let dtype = match encoded.dtype() {
            DType::Primitive(PType::I32, nullability) => DType::Primitive(PType::F32, *nullability),
            DType::Primitive(PType::I64, nullability) => DType::Primitive(PType::F64, *nullability),
            d => vortex_bail!(MismatchedTypes: "int32 or int64", d),
        };

        let length = encoded.len();
        if let Some(parray) = patches.as_ref() {
            if parray.len() != length {
                vortex_bail!(
                    "Mismatched length in ALPArray between encoded({}) {} and it's patches({}) {}",
                    encoded.encoding().id(),
                    encoded.len(),
                    parray.encoding().id(),
                    parray.len()
                )
            }
        }

        let patches_dtype = patches.as_ref().map(|a| a.dtype().as_nullable());
        let mut children = Vec::with_capacity(2);
        children.push(encoded);
        if let Some(patch) = patches {
            children.push(patch);
        }

        Self::try_from_parts(
            dtype,
            length,
            ALPMetadata {
                exponents,
                encoded_dtype,
                patches_dtype,
            },
            children.into(),
            Default::default(),
        )
    }

    pub fn encode(array: Array) -> VortexResult<Array> {
        if let Ok(parray) = PrimitiveArray::try_from(array) {
            Ok(alp_encode(&parray)?.into_array())
        } else {
            vortex_bail!("ALP can only encode primitive arrays");
        }
    }

    pub fn encoded(&self) -> Array {
        self.as_ref()
            .child(0, &self.metadata().encoded_dtype, self.len())
            .vortex_expect("Missing encoded child in ALPArray")
    }

    #[inline]
    pub fn exponents(&self) -> Exponents {
        self.metadata().exponents
    }

    pub fn patches(&self) -> Option<Array> {
        self.metadata().patches_dtype.as_ref().map(|dt| {
            self.as_ref().child(1, dt, self.len()).unwrap_or_else(|e| {
                vortex_panic!(
                    e,
                    "ALPArray: patches child missing: dtype: {}, len: {}",
                    dt,
                    self.len()
                )
            })
        })
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype()
            .try_into()
            .vortex_expect("Failed to convert DType to PType")
    }
}

impl ArrayTrait for ALPArray {}

impl ArrayVariants for ALPArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

struct ALPAccessor<F: ALPFloat> {
    encoded: Arc<dyn Accessor<F::ALPInt>>,
    patches: Option<Arc<dyn Accessor<F>>>,
    validity: Validity,
    exponents: Exponents,
}

impl<F: ALPFloat> ALPAccessor<F> {
    fn new(
        encoded: AccessorRef<F::ALPInt>,
        patches: Option<AccessorRef<F>>,
        exponents: Exponents,
        validity: Validity,
    ) -> Self {
        Self {
            encoded,
            patches,
            validity,
            exponents,
        }
    }
}

impl<F: ALPFloat> Accessor<F> for ALPAccessor<F> {
    fn array_len(&self) -> usize {
        self.encoded.array_len()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity.is_valid(index)
    }

    fn value_unchecked(&self, index: usize) -> F {
        match self.patches.as_ref() {
            Some(patches) if patches.is_valid(index) => patches.value_unchecked(index),
            _ => {
                let encoded = self.encoded.value_unchecked(index);
                F::decode_single(encoded, self.exponents)
            }
        }
    }

    fn array_validity(&self) -> Validity {
        self.validity.clone()
    }

    fn decode_batch(&self, start_idx: usize) -> Vec<F> {
        let mut values = self
            .encoded
            .decode_batch(start_idx)
            .into_iter()
            .map(|v| F::decode_single(v, self.exponents))
            .collect::<Vec<F>>();

        if let Some(patches_accessor) = self.patches.as_ref() {
            for (index, item) in values.iter_mut().enumerate() {
                let index = index + start_idx;

                if patches_accessor.is_valid(index) {
                    *item = patches_accessor.value_unchecked(index);
                }
            }
        }

        values
    }
}

impl PrimitiveArrayTrait for ALPArray {
    fn f32_accessor(&self) -> Option<AccessorRef<f32>> {
        match self.dtype() {
            DType::Primitive(PType::F32, _) => {
                let patches = self
                    .patches()
                    .and_then(|p| p.with_dyn(|a| a.as_primitive_array_unchecked().f32_accessor()));

                let encoded = self
                    .encoded()
                    .with_dyn(|a| a.as_primitive_array_unchecked().i32_accessor())
                    .vortex_expect(
                        "Failed to get underlying encoded i32 array for ALP-encoded f32 array",
                    );

                Some(Arc::new(ALPAccessor::new(
                    encoded,
                    patches,
                    self.exponents(),
                    self.logical_validity().into_validity(),
                )))
            }
            _ => None,
        }
    }

    fn f64_accessor(&self) -> Option<AccessorRef<f64>> {
        match self.dtype() {
            DType::Primitive(PType::F64, _) => {
                let patches = self
                    .patches()
                    .and_then(|p| p.with_dyn(|a| a.as_primitive_array_unchecked().f64_accessor()));

                let encoded = self
                    .encoded()
                    .with_dyn(|a| a.as_primitive_array_unchecked().i64_accessor())
                    .vortex_expect(
                        "Failed to get underlying encoded i64 array for ALP-encoded f64 array",
                    );
                Some(Arc::new(ALPAccessor::new(
                    encoded,
                    patches,
                    self.exponents(),
                    self.logical_validity().into_validity(),
                )))
            }
            _ => None,
        }
    }
}

impl ArrayValidity for ALPArray {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl IntoCanonical for ALPArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        decompress(self).map(Canonical::Primitive)
    }
}

impl AcceptArrayVisitor for ALPArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("encoded", &self.encoded())?;
        if let Some(patches) = self.patches().as_ref() {
            visitor.visit_child("patches", patches)?;
        }
        Ok(())
    }
}

impl ArrayStatisticsCompute for ALPArray {}
