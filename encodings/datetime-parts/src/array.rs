use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use vortex::array::StructArray;
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, ExtensionArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoCanonical,
};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexExpect as _, VortexResult};

use crate::compute::decode_to_temporal;

impl_encoding!("vortex.datetimeparts", ids::DATE_TIME_PARTS, DateTimeParts);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTimePartsMetadata {
    // Validity lives in the days array
    // TODO(ngates): we should actually model this with a Tuple array when we have one.
    days_dtype: DType,
    seconds_dtype: DType,
    subseconds_dtype: DType,
}

impl DateTimePartsArray {
    pub fn try_new(
        dtype: DType,
        days: Array,
        seconds: Array,
        subsecond: Array,
    ) -> VortexResult<Self> {
        if !days.dtype().is_int() {
            vortex_bail!(MismatchedTypes: "any integer", days.dtype());
        }
        if !seconds.dtype().is_int() {
            vortex_bail!(MismatchedTypes: "any integer", seconds.dtype());
        }
        if !subsecond.dtype().is_int() {
            vortex_bail!(MismatchedTypes: "any integer", subsecond.dtype());
        }

        let length = days.len();
        if length != seconds.len() || length != subsecond.len() {
            vortex_bail!(
                "Mismatched lengths {} {} {}",
                days.len(),
                seconds.len(),
                subsecond.len()
            );
        }

        Self::try_from_parts(
            dtype,
            length,
            DateTimePartsMetadata {
                days_dtype: days.dtype().clone(),
                seconds_dtype: seconds.dtype().clone(),
                subseconds_dtype: subsecond.dtype().clone(),
            },
            [days, seconds, subsecond].into(),
            StatsSet::new(),
        )
    }

    pub fn days(&self) -> Array {
        self.as_ref()
            .child(0, &self.metadata().days_dtype, self.len())
            .vortex_expect("DatetimePartsArray missing days array")
    }

    pub fn seconds(&self) -> Array {
        self.as_ref()
            .child(1, &self.metadata().seconds_dtype, self.len())
            .vortex_expect("DatetimePartsArray missing seconds array")
    }

    pub fn subsecond(&self) -> Array {
        self.as_ref()
            .child(2, &self.metadata().subseconds_dtype, self.len())
            .vortex_expect("DatetimePartsArray missing subsecond array")
    }
}

impl ArrayTrait for DateTimePartsArray {}

impl ArrayVariants for DateTimePartsArray {
    fn as_extension_array(&self) -> Option<&dyn ExtensionArrayTrait> {
        Some(self)
    }
}

impl ExtensionArrayTrait for DateTimePartsArray {
    fn storage_array(&self) -> Array {
        // FIXME(ngates): this needs to be a tuple array so we can implement Compare
        StructArray::try_new(
            vec!["days".into(), "seconds".into(), "subseconds".into()].into(),
            [self.days(), self.seconds(), self.subsecond()].into(),
            self.len(),
            self.logical_validity().into_validity(),
        )
        .vortex_expect("Failed to create struct array")
        .into_array()
    }
}

impl IntoCanonical for DateTimePartsArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        Ok(Canonical::Extension(decode_to_temporal(&self)?.into()))
    }
}

impl ArrayValidity for DateTimePartsArray {
    fn is_valid(&self, index: usize) -> bool {
        self.days().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.days().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for DateTimePartsArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("days", &self.days())?;
        visitor.visit_child("seconds", &self.seconds())?;
        visitor.visit_child("subsecond", &self.subsecond())
    }
}

impl ArrayStatisticsCompute for DateTimePartsArray {}
