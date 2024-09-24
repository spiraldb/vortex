use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use vortex::accessor::ArrayAccessor;
use vortex::array::BoolArray;
use vortex::compute::take;
use vortex::compute::unary::scalar_at;
use vortex::encoding::ids;
use vortex::stats::StatsSet;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoArrayVariant,
    IntoCanonical,
};
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{vortex_bail, vortex_panic, VortexExpect as _, VortexResult};

impl_encoding!("vortex.dict", ids::DICT, Dict);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DictMetadata {
    codes_dtype: DType,
    values_len: usize,
}

impl DictArray {
    pub fn try_new(codes: Array, values: Array) -> VortexResult<Self> {
        if !codes.dtype().is_unsigned_int() {
            vortex_bail!(MismatchedTypes: "unsigned int", codes.dtype());
        }
        Self::try_from_parts(
            values.dtype().clone(),
            codes.len(),
            DictMetadata {
                codes_dtype: codes.dtype().clone(),
                values_len: values.len(),
            },
            [values, codes].into(),
            StatsSet::new(),
        )
    }

    #[inline]
    pub fn values(&self) -> Array {
        self.as_ref()
            .child(0, self.dtype(), self.metadata().values_len)
            .vortex_expect("DictArray is missing its values child array")
    }

    #[inline]
    pub fn codes(&self) -> Array {
        self.as_ref()
            .child(1, &self.metadata().codes_dtype, self.len())
            .vortex_expect("DictArray is missing its codes child array")
    }
}

impl ArrayTrait for DictArray {}

impl IntoCanonical for DictArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        take(self.values(), self.codes())?.into_canonical()
    }
}

impl ArrayValidity for DictArray {
    fn is_valid(&self, index: usize) -> bool {
        let values_index = scalar_at(&self.codes(), index)
            .unwrap_or_else(|err| {
                vortex_panic!(err, "Failed to get index {} from DictArray codes", index)
            })
            .as_ref()
            .try_into()
            .vortex_expect("Failed to convert dictionary code to usize");
        self.values().with_dyn(|a| a.is_valid(values_index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        if self.dtype().is_nullable() {
            let primitive_codes = self
                .codes()
                .into_primitive()
                .vortex_expect("Failed to convert DictArray codes to primitive array");
            match_each_integer_ptype!(primitive_codes.ptype(), |$P| {
                ArrayAccessor::<$P>::with_iterator(&primitive_codes, |iter| {
                    LogicalValidity::Array(
                        BoolArray::from(iter.flatten().map(|c| *c != 0).collect::<Vec<_>>())
                            .into_array(),
                    )
                }).vortex_expect("Failed to convert DictArray codes into logical validity")
            })
        } else {
            LogicalValidity::AllValid(self.len())
        }
    }
}

impl AcceptArrayVisitor for DictArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("values", &self.values())?;
        visitor.visit_child("codes", &self.codes())
    }
}
