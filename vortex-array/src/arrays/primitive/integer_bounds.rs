// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Integer bounds and sortedness computed directly from primitive values.
//!
//! Array validation and integer narrowing need the minimum, maximum and sortedness of integer
//! arrays. They are computed here without the aggregate function registry, so they work with any
//! session. Exact cached statistics are used when present, and computed bounds are written back to
//! the statistics cache.

use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_mask::Mask;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::arrays::PrimitiveArray;
use crate::arrays::primitive::PrimitiveArrayExt;
use crate::expr::stats::Precision;
use crate::expr::stats::Stat;
use crate::expr::stats::StatsProviderExt;
use crate::match_each_integer_ptype;

/// The minimum, maximum and sortedness of the valid values of an integer array.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntegerBounds {
    /// The smallest valid value.
    pub min: i128,
    /// The largest valid value.
    pub max: i128,
    /// Whether the valid values are in non-decreasing order.
    pub is_sorted: bool,
}

/// Returns the [`IntegerBounds`] of the valid values of `array`, or `None` if it has no valid
/// values.
///
/// Null positions are skipped, so `is_sorted` only describes the order of the valid values. The
/// `IsSorted` statistic is only cached for arrays without nulls, where the two meanings agree.
///
/// # Errors
///
/// Fails if `array` is not an integer array, or if it cannot be executed to a [`PrimitiveArray`].
pub fn integer_bounds(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<IntegerBounds>> {
    vortex_ensure!(
        array.dtype().is_int(),
        "integer bounds require an integer array, got {}",
        array.dtype()
    );
    let ptype = array.dtype().as_ptype();

    if let Some(bounds) = cached_bounds(array) {
        return Ok(Some(bounds));
    }

    let primitive = array.clone().execute::<PrimitiveArray>(ctx)?;
    let mask = PrimitiveArrayExt::validity(&primitive).execute_mask(primitive.len(), ctx)?;

    match_each_integer_ptype!(ptype, |P| {
        let values = primitive.as_slice::<P>();
        let Some((min, max, is_sorted)) = scan(values, &mask) else {
            return Ok(None);
        };

        let stats = array.statistics();
        stats.set(Stat::Min, Precision::exact(min));
        stats.set(Stat::Max, Precision::exact(max));
        if matches!(mask, Mask::AllTrue(_)) {
            stats.set(Stat::IsSorted, Precision::exact(is_sorted));
        }

        Ok(Some(IntegerBounds {
            min: i128::from(min),
            max: i128::from(max),
            is_sorted,
        }))
    })
}

/// Reads the bounds from exact cached statistics, if all three are present.
fn cached_bounds(array: &ArrayRef) -> Option<IntegerBounds> {
    let stats = array.statistics();
    let is_sorted = stats.get_as::<bool>(Stat::IsSorted).as_exact()?;
    match_each_integer_ptype!(array.dtype().as_ptype(), |P| {
        let min = stats.get_as::<P>(Stat::Min).as_exact()?;
        let max = stats.get_as::<P>(Stat::Max).as_exact()?;
        Some(IntegerBounds {
            min: i128::from(min),
            max: i128::from(max),
            is_sorted,
        })
    })
}

/// Returns the minimum, maximum and sortedness of the values selected by `mask`.
fn scan<P: Copy + PartialOrd>(values: &[P], mask: &Mask) -> Option<(P, P, bool)> {
    let mut valid = values
        .iter()
        .enumerate()
        .filter(|(idx, _)| match mask {
            Mask::AllTrue(_) => true,
            Mask::AllFalse(_) => false,
            Mask::Values(values) => values.value(*idx),
        })
        .map(|(_, value)| *value);

    let first = valid.next()?;
    let (mut min, mut max, mut prev, mut is_sorted) = (first, first, first, true);
    for value in valid {
        if value < min {
            min = value;
        }
        if value > max {
            max = value;
        }
        is_sorted &= prev <= value;
        prev = value;
    }
    Some((min, max, is_sorted))
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use super::IntegerBounds;
    use super::integer_bounds;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::arrays::PrimitiveArray;
    use crate::expr::stats::Precision;
    use crate::expr::stats::Stat;
    use crate::expr::stats::StatsProviderExt;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);

    #[test]
    fn bounds_of_sorted_values() -> VortexResult<()> {
        let array = PrimitiveArray::from_iter([0u32, 2, 2, 7]).into_array();
        let mut ctx = SESSION.create_execution_ctx();
        assert_eq!(
            integer_bounds(&array, &mut ctx)?,
            Some(IntegerBounds {
                min: 0,
                max: 7,
                is_sorted: true
            })
        );
        assert_eq!(
            array.statistics().get_as::<bool>(Stat::IsSorted),
            Precision::exact(true)
        );
        Ok(())
    }

    #[test]
    fn bounds_skip_nulls() -> VortexResult<()> {
        let array =
            PrimitiveArray::from_option_iter([Some(5i64), None, Some(-3), None]).into_array();
        let mut ctx = SESSION.create_execution_ctx();
        assert_eq!(
            integer_bounds(&array, &mut ctx)?,
            Some(IntegerBounds {
                min: -3,
                max: 5,
                is_sorted: false
            })
        );
        assert_eq!(
            array.statistics().get_as::<bool>(Stat::IsSorted),
            Precision::Absent
        );
        Ok(())
    }

    #[test]
    fn bounds_of_all_null_array() -> VortexResult<()> {
        let array = PrimitiveArray::from_option_iter([None::<u8>, None]).into_array();
        let mut ctx = SESSION.create_execution_ctx();
        assert_eq!(integer_bounds(&array, &mut ctx)?, None);
        Ok(())
    }
}
