use serde::{Deserialize, Serialize};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, ToArrayData};
use vortex_error::vortex_bail;

impl_encoding!("vortex.datetimeparts", DateTimeParts);

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
            DateTimePartsMetadata {
                days_dtype: days.dtype().clone(),
                seconds_dtype: seconds.dtype().clone(),
                subseconds_dtype: subsecond.dtype().clone(),
            },
            [
                days.to_array_data(),
                seconds.to_array_data(),
                subsecond.to_array_data(),
            ]
            .into(),
            StatsSet::new(),
        )
    }

    pub fn days(&self) -> Array {
        self.array()
            .child(0, &self.metadata().days_dtype)
            .expect("Missing days array")
    }

    pub fn seconds(&self) -> Array {
        self.array()
            .child(1, &self.metadata().seconds_dtype)
            .expect("Missing seconds array")
    }

    pub fn subsecond(&self) -> Array {
        self.array()
            .child(2, &self.metadata().subseconds_dtype)
            .expect("Missing subsecond array")
    }
}

impl ArrayFlatten for DateTimePartsArray {
    fn flatten(self) -> VortexResult<Flattened> {
        // TODO(ngates): flatten into vortex.localdatetime or appropriate per dtype
        todo!()
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

impl ArrayTrait for DateTimePartsArray {
    fn len(&self) -> usize {
        self.days().len()
    }
}
