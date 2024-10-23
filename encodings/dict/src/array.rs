use std::fmt::{Debug, Display};

use arrow_buffer::{BooleanBuffer, ScalarBuffer};
use serde::{Deserialize, Serialize};
use vortex::array::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::array::{BoolArray, ConstantArray, VarBinViewArray};
use vortex::compute::unary::{scalar_at, try_cast};
use vortex::compute::{compare, take, Operator};
use vortex::encoding::ids;
use vortex::stats::StatsSet;
use vortex::validity::{ArrayValidity, LogicalValidity, Validity};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayTrait, Canonical, IntoArray, IntoArrayVariant,
    IntoCanonical,
};
use vortex_dtype::{match_each_integer_ptype, DType, PType};
use vortex_error::{vortex_bail, vortex_panic, VortexExpect as _, VortexResult};

impl_encoding!("vortex.dict", ids::DICT, Dict);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DictMetadata {
    codes_ptype: PType,
    values_len: usize,
}

impl Display for DictMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl DictArray {
    pub fn try_new(codes: Array, values: Array) -> VortexResult<Self> {
        if !codes.dtype().is_unsigned_int() || codes.dtype().is_nullable() {
            vortex_bail!(MismatchedTypes: "non-nullable unsigned int", codes.dtype());
        }
        Self::try_from_parts(
            values.dtype().clone(),
            codes.len(),
            DictMetadata {
                codes_ptype: PType::try_from(codes.dtype())
                    .vortex_expect("codes dtype must be uint"),

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
            .child(1, &DType::from(self.metadata().codes_ptype), self.len())
            .vortex_expect("DictArray is missing its codes child array")
    }
}

impl ArrayTrait for DictArray {}

impl IntoCanonical for DictArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        match self.dtype() {
            DType::Utf8(_) | DType::Binary(_) => canonicalize_string(self),
            _ => canonicalize_primitive(self),
        }
    }
}

/// Canonicalize a set of codes and values.
fn canonicalize_string(array: DictArray) -> VortexResult<Canonical> {
    let values = array.values().into_varbinview()?;
    let codes = try_cast(array.codes(), PType::U64.into())?.into_primitive()?;

    let value_views = ScalarBuffer::<u128>::from(values.views().clone().into_arrow());

    // Gather the views from value_views into full_views using the dictionary codes.
    let full_views: Vec<u128> = codes
        .maybe_null_slice::<u64>()
        .iter()
        .map(|code| value_views[*code as usize])
        .collect();

    let validity = if array.dtype().is_nullable() {
        // For nullable arrays, a code of 0 indicates null value.
        Validity::Array(compare(
            codes.as_ref(),
            ConstantArray::new(0u64, codes.len()).as_ref(),
            Operator::Eq,
        )?)
    } else {
        Validity::NonNullable
    };

    VarBinViewArray::try_new(
        full_views.into(),
        values.buffers().collect(),
        array.dtype().clone(),
        validity,
    )
    .map(Canonical::VarBinView)
}

fn canonicalize_primitive(array: DictArray) -> VortexResult<Canonical> {
    let canonical_values: Array = array.values().into_canonical()?.into();
    take(canonical_values, array.codes())?.into_canonical()
}

impl ArrayValidity for DictArray {
    fn is_valid(&self, index: usize) -> bool {
        let values_index = scalar_at(self.codes(), index)
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
                let is_valid = primitive_codes
                    .maybe_null_slice::<$P>();
                let is_valid_buffer = BooleanBuffer::collect_bool(is_valid.len(), |idx| {
                    is_valid[idx] != 0
                });
                LogicalValidity::Array(BoolArray::from(is_valid_buffer).into_array())
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
