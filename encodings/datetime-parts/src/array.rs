use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};
use vortex::array::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::array::StructArray;
use vortex::compute::unary::try_cast;
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity};
use vortex::variants::{ArrayVariants, ExtensionArrayTrait};
use vortex::{impl_encoding, Array, ArrayDType, ArrayTrait, Canonical, IntoArray, IntoCanonical};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, VortexExpect as _, VortexResult, VortexUnwrap};

use crate::compute::decode_to_temporal;

impl_encoding!("vortex.datetimeparts", ids::DATE_TIME_PARTS, DateTimeParts);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTimePartsMetadata {
    // Validity lives in the days array
    // TODO(ngates): we should actually model this with a Tuple array when we have one.
    days_ptype: PType,
    seconds_ptype: PType,
    subseconds_ptype: PType,
}

impl Display for DateTimePartsMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl DateTimePartsArray {
    pub fn try_new(
        dtype: DType,
        days: Array,
        seconds: Array,
        subsecond: Array,
    ) -> VortexResult<Self> {
        if !days.dtype().is_int() || (dtype.is_nullable() != days.dtype().is_nullable()) {
            vortex_bail!(
                "Expected integer with nullability {}, got {}",
                dtype.is_nullable(),
                days.dtype()
            );
        }
        if !seconds.dtype().is_int() || seconds.dtype().is_nullable() {
            vortex_bail!(MismatchedTypes: "non-nullable integer", seconds.dtype());
        }
        if !subsecond.dtype().is_int() || subsecond.dtype().is_nullable() {
            vortex_bail!(MismatchedTypes: "non-nullable integer", subsecond.dtype());
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

        let metadata = DateTimePartsMetadata {
            days_ptype: days.dtype().try_into()?,
            seconds_ptype: seconds.dtype().try_into()?,
            subseconds_ptype: subsecond.dtype().try_into()?,
        };

        Self::try_from_parts(
            dtype,
            length,
            metadata,
            [days, seconds, subsecond].into(),
            StatsSet::new(),
        )
    }

    pub fn days(&self) -> Array {
        self.as_ref()
            .child(
                0,
                &DType::Primitive(self.metadata().days_ptype, self.dtype().nullability()),
                self.len(),
            )
            .vortex_expect("DatetimePartsArray missing days array")
    }

    pub fn seconds(&self) -> Array {
        self.as_ref()
            .child(1, &self.metadata().seconds_ptype.into(), self.len())
            .vortex_expect("DatetimePartsArray missing seconds array")
    }

    pub fn subsecond(&self) -> Array {
        self.as_ref()
            .child(2, &self.metadata().subseconds_ptype.into(), self.len())
            .vortex_expect("DatetimePartsArray missing subsecond array")
    }

    pub fn validity(&self) -> Validity {
        if self.dtype().is_nullable() {
            self.days()
                .with_dyn(|a| a.logical_validity())
                .into_validity()
        } else {
            Validity::NonNullable
        }
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
        // we don't want to write validity twice, so we pull it up to the top
        let days = try_cast(self.days(), &self.days().dtype().as_nonnullable()).vortex_unwrap();
        StructArray::try_new(
            vec!["days".into(), "seconds".into(), "subseconds".into()].into(),
            [days, self.seconds(), self.subsecond()].into(),
            self.len(),
            self.validity(),
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
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
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
