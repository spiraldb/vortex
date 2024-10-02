use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use vortex::array::StructArray;
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, ExtensionArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Array, ArrayDType, ArrayTrait, Canonical, IntoArray, IntoCanonical};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, VortexExpect as _, VortexResult};

use crate::compute::decode_to_temporal;

impl_encoding!("vortex.datetimeparts", ids::DATE_TIME_PARTS, DateTimeParts);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTimePartsMetadata {
    // Validity lives in the days array
    // TODO(ngates): we should actually model this with a Tuple array when we have one.
    validity: ValidityMetadata,
    days_ptype: PType,
    seconds_ptype: PType,
    subseconds_ptype: PType,
}

impl DateTimePartsArray {
    pub fn try_new(
        dtype: DType,
        validity: Validity,
        days: Array,
        seconds: Array,
        subsecond: Array,
    ) -> VortexResult<Self> {
        if !days.dtype().is_int() || days.dtype().is_nullable() {
            vortex_bail!(MismatchedTypes: "non-nullable integer", days.dtype());
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
            validity: validity.to_metadata(length)?,
            days_ptype: days.dtype().try_into()?,
            seconds_ptype: seconds.dtype().try_into()?,
            subseconds_ptype: subsecond.dtype().try_into()?,
        };

        let mut children: Vec<Array> = [days, seconds, subsecond].into();
        if let Validity::Array(a) = validity {
            children.push(a);
        }

        Self::try_from_parts(
            dtype,
            length,
            metadata,
            children.into(),
            StatsSet::new(),
        )
    }

    pub fn days(&self) -> Array {
        self.as_ref()
            .child(0, &self.metadata().days_ptype.into(), self.len())
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
        self.metadata()
            .validity
            .to_validity(|| self.as_ref().child(3, &Validity::DTYPE, self.len()).vortex_expect("DatetimePartsArray missing validity array"))
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
        visitor.visit_child("subsecond", &self.subsecond())?;
        if let Some(validity) = self.validity().into_array() {
            visitor.visit_child("validity", &validity)?;
        }
        Ok(())
    }
}

impl ArrayStatisticsCompute for DateTimePartsArray {}
