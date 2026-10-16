// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex::array::match_each_float_ptype;
use vortex::array::stats::StatsSet;
use vortex::dtype::DType;
use vortex::dtype::PType;
use vortex::error::VortexExpect as _;
use vortex::error::VortexResult;
use vortex::expr::stats::Precision;
use vortex::expr::stats::Stat;
use vortex::scalar::Scalar;
use vortex::scalar::ScalarValue;

use crate::convert::ToDuckDBScalar as _;
use crate::duckdb::LogicalType;
use crate::duckdb::Value;

#[derive(Debug)]
pub struct ColumnStatistics {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub max_string_length: u64,
    /// Exact non-null distinct count when strict ordering proves uniqueness.
    pub distinct_count: Option<u64>,
    pub has_null: bool,
    pub logical_type: LogicalType,
}

impl ColumnStatistics {
    pub fn try_from(stats: ColumnStatisticsAggregate, dtype: DType) -> VortexResult<Self> {
        let to_value = |value: ScalarValue| {
            Scalar::try_new(dtype.clone(), Some(value))
                .and_then(|scalar| scalar.try_to_duckdb_scalar())
                .ok()
        };
        let min = stats.min.and_then(to_value);
        // Vortex's max skips NaN, while DuckDB orders NaN above every other value.
        let max = if !dtype.is_float() || stats.nan_count == Some(0) {
            stats.max.and_then(to_value)
        } else if stats.nan_count.is_some() {
            to_value(match_each_float_ptype!(dtype.as_ptype(), |F| {
                ScalarValue::from(F::NAN)
            }))
        } else {
            None
        };

        let max_string_length = stats
            .max_string_length
            .map_or(0, |len| (1u64 << 63) | (len as u64));

        // Useful estimate if we didn't get null count stats
        let has_null = stats.has_null && dtype.is_nullable();

        let logical_type = LogicalType::try_from(dtype)?;

        Ok(Self {
            min,
            max,
            max_string_length,
            distinct_count: stats.distinct_count,
            has_null,
            logical_type,
        })
    }
}

#[derive(Default)]
pub struct ColumnStatisticsAggregate {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub max_string_length: Option<u32>,
    /// Exact non-null distinct count when strict ordering proves uniqueness.
    pub distinct_count: Option<u64>,
    /// May be true if null count stat isn't present
    pub has_null: bool,
    pub nan_count: Option<u64>,
}

impl ColumnStatisticsAggregate {
    /// Collects statistics and derives a distinct count only from a proven unique column.
    pub fn new(stats: &StatsSet, dtype: &DType, row_count: u64) -> Self {
        let min = match stats.get(Stat::Min) {
            Precision::Exact(min) => Some(min),
            _ => None,
        };
        let max = match stats.get(Stat::Max) {
            Precision::Exact(max) => Some(max),
            _ => None,
        };

        let max_string_length =
            if let Precision::Exact(value) = stats.get(Stat::UncompressedSizeInBytes) {
                // DuckDB's string length is u32
                #[allow(clippy::cast_possible_truncation)]
                Some(value.as_primitive().as_u64().vortex_expect("not a u64") as u32)
            } else {
                None
            };

        let null_count = match stats.get(Stat::NullCount) {
            Precision::Exact(cnt) => cnt.as_primitive().as_u64(),
            _ if !dtype.is_nullable() => Some(0),
            _ => None,
        };
        let has_null = null_count.is_none_or(|count| count > 0);
        let distinct_count =
            if dtype.is_int() && stats.get(Stat::IsStrictSorted) == Precision::Exact(true.into()) {
                null_count.and_then(|count| row_count.checked_sub(count))
            } else {
                None
            };

        let nan_count = stats
            .get_as::<u64>(Stat::NaNCount, &PType::U64.into())
            .as_exact();

        Self {
            min,
            max,
            max_string_length,
            distinct_count,
            has_null,
            nan_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex::dtype::Nullability;
    use vortex::dtype::PType;
    use vortex::error::vortex_err;

    use super::*;

    #[rstest]
    #[case::proven_unique(Precision::Exact(true), Some(0), false, PType::I64, Some(1000))]
    #[case::nullable_unique(Precision::Exact(true), Some(1), true, PType::I64, Some(999))]
    #[case::unknown_null_count(Precision::Exact(true), None, true, PType::I64, None)]
    #[case::non_nullable(Precision::Exact(true), None, false, PType::I64, Some(1000))]
    #[case::not_sorted(Precision::Exact(false), Some(0), false, PType::I64, None)]
    #[case::range_is_insufficient(Precision::Absent, Some(0), false, PType::I64, None)]
    #[case::inexact_property(Precision::Inexact(true), Some(0), false, PType::I64, None)]
    #[case::invalid_null_count(Precision::Exact(true), Some(1001), true, PType::I64, None)]
    #[case::float_equality_not_proven(Precision::Exact(true), Some(0), false, PType::F64, None)]
    fn distinct_count_requires_a_proof(
        #[case] strict: Precision<bool>,
        #[case] null_count: Option<u64>,
        #[case] nullable: bool,
        #[case] ptype: PType,
        #[case] expected: Option<u64>,
    ) -> VortexResult<()> {
        let dtype = DType::Primitive(ptype, Nullability::from(nullable));
        let mut stats = StatsSet::default();
        stats.set(Stat::IsStrictSorted, strict.map(ScalarValue::from));
        if let Some(count) = null_count {
            stats.set(Stat::NullCount, Precision::Exact(count.into()));
        }
        for (stat, value) in [(Stat::Min, 0i32), (Stat::Max, 999)] {
            let value = Scalar::from(value)
                .cast(&dtype)?
                .into_value()
                .ok_or_else(|| vortex_err!("missing numeric bound"))?;
            stats.set(stat, Precision::Exact(value));
        }
        assert_eq!(
            ColumnStatisticsAggregate::new(&stats, &dtype, 1000).distinct_count,
            expected
        );
        Ok(())
    }
}
