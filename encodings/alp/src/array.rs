use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use vortex::array::PrimitiveArray;
use vortex::compute::unary::scalar_at_unchecked;
use vortex::iter::{Accessor, ArrayIter};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoCanonical,
};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::alp::Exponents;
use crate::compress::{alp_encode, decompress};
use crate::ALPFloat;

impl_encoding!("vortex.alp", 13u16, ALP);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ALPMetadata {
    exponents: Exponents,
    encoded_dtype: DType,
    patches_dtype: Option<DType>,
    patches_len: usize,
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

        let patches_dtype = patches.as_ref().map(|a| a.dtype().as_nullable());
        let patches_len = patches.as_ref().map(|a| a.len()).unwrap_or(0);
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
                patches_len,
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
        self.array()
            .child(0, &self.metadata().encoded_dtype, self.len())
            .expect("Missing encoded array")
    }

    #[inline]
    pub fn exponents(&self) -> Exponents {
        self.metadata().exponents
    }

    pub fn patches(&self) -> Option<Array> {
        self.metadata().patches_dtype.as_ref().map(|dt| {
            self.array()
                .child(1, dt, self.metadata().patches_len)
                .unwrap_or_else(|| {
                    panic!(
                        "Missing patches with present metadata flag; dtype: {}, patches_len: {}",
                        dt,
                        self.metadata().patches_len
                    )
                })
        })
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype().try_into().unwrap()
    }
}

impl ArrayTrait for ALPArray {}

impl ArrayVariants for ALPArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

struct AlpAccessor<F: ALPFloat> {
    array: ALPArray,
    _marker: PhantomData<F>,
}

impl<F: ALPFloat> AlpAccessor<F> {
    pub(crate) fn new(array: ALPArray) -> Self {
        Self {
            array,
            _marker: PhantomData,
        }
    }
}

impl<'a, F> Accessor<'a, F> for AlpAccessor<F>
where
    F: ALPFloat + TryFrom<Scalar, Error = VortexError>,
    F::ALPInt: TryFrom<Scalar, Error = VortexError>,
{
    fn len(&self) -> usize {
        todo!()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.array.is_valid(index)
    }

    fn value_unchecked(&self, index: usize) -> F {
        if let Some(patches) = self.array.patches().and_then(|p| {
            p.with_dyn(|arr| {
                // We need to make sure the value is actually in the patches array
                arr.is_valid(index)
            })
            .then_some(p)
        }) {
            let s = scalar_at_unchecked(&patches, index);
            return s.try_into().unwrap();
        }

        let encoded_val = scalar_at_unchecked(&self.array.encoded(), index);
        let encoded_val = encoded_val.try_into().unwrap();
        F::decode_single(encoded_val, self.array.exponents())
    }
}

impl PrimitiveArrayTrait for ALPArray {
    fn float32_iter(&self) -> Option<ArrayIter<f32>> {
        match self.dtype() {
            DType::Primitive(PType::F32, _) => {
                let access = Arc::new(AlpAccessor::new(self.clone()));
                let iter = ArrayIter::new(access as _);
                Some(iter)
            }
            _ => None,
        }
    }

    fn float64_iter(&self) -> Option<ArrayIter<f64>> {
        match self.dtype() {
            DType::Primitive(PType::F64, _) => {
                let access = Arc::new(AlpAccessor::new(self.clone()));
                let iter = ArrayIter::new(access as _);
                Some(iter)
            }
            _ => None,
        }
    }

    fn unsigned32_iter(&self) -> Option<ArrayIter<u32>> {
        todo!()
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
