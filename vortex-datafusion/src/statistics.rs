use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result as DFResult, ScalarValue, Statistics};
use itertools::Itertools;
use vortex::array::ChunkedArray;
use vortex::stats::{ArrayStatistics, Stat};
use vortex_error::{vortex_err, VortexExpect, VortexResult};

pub fn chunked_array_df_stats(array: &ChunkedArray, projection: &[usize]) -> DFResult<Statistics> {
    let mut nbytes: usize = 0;
    let column_statistics = array.as_ref().with_dyn(|a| {
        let struct_arr = a
            .as_struct_array()
            .ok_or_else(|| vortex_err!("Not a struct array"))?;
        projection
            .iter()
            .map(|i| {
                struct_arr
                    .field(*i)
                    .ok_or_else(|| vortex_err!("Projection references unknown field {i}"))
            })
            .map_ok(|arr| {
                nbytes += arr.nbytes();
                ColumnStatistics {
                    null_count: arr
                        .statistics()
                        .get_as::<u64>(Stat::NullCount)
                        .map(|n| n as usize)
                        .map(Precision::Exact)
                        .unwrap_or(Precision::Absent),
                    max_value: arr
                        .statistics()
                        .get(Stat::Max)
                        .map(|n| {
                            ScalarValue::try_from(n)
                                .vortex_expect("cannot convert scalar to df scalar")
                        })
                        .map(Precision::Exact)
                        .unwrap_or(Precision::Absent),
                    min_value: arr
                        .statistics()
                        .get(Stat::Min)
                        .map(|n| {
                            ScalarValue::try_from(n)
                                .vortex_expect("cannot convert scalar to df scalar")
                        })
                        .map(Precision::Exact)
                        .unwrap_or(Precision::Absent),
                    distinct_count: Precision::Absent,
                }
            })
            .collect::<VortexResult<Vec<_>>>()
    })?;
    Ok(Statistics {
        num_rows: Precision::Exact(array.len()),
        total_byte_size: Precision::Exact(nbytes),
        column_statistics,
    })
}
