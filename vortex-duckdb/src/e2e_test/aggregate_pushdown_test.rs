// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Integer aggregates must retain DuckDB's wider accumulation range.

use std::sync::Arc;

use rstest::rstest;
use tempfile::NamedTempFile;
use vortex::array::IntoArray;
use vortex::array::arrays::ConstantArray;
use vortex::array::arrays::StructArray;
use vortex::error::VortexResult;
use vortex::error::vortex_err;
use vortex::file::WriteOptionsSessionExt;
use vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex::scalar::Scalar;

use crate::RUNTIME;
use crate::SESSION;
use crate::duckdb::Connection;
use crate::duckdb::Database;
use crate::duckdb::ExtractedValue;

fn constant_file(value: Scalar, len: usize) -> VortexResult<NamedTempFile> {
    let file = NamedTempFile::with_suffix(".vortex")?;
    let array = StructArray::try_from_iter([("i", ConstantArray::new(value, len).into_array())])?
        .into_array();

    // Store the constant directly so billions of logical rows need only a small fixture.
    SESSION
        .write_options()
        .with_strategy(Arc::new(FlatLayoutStrategy::default()))
        .blocking(&*RUNTIME)
        .write(file.reopen()?, array.to_array_iterator())?;

    Ok(file)
}

fn connection() -> VortexResult<Connection> {
    let db = Database::open_in_memory()?;
    db.register_vortex_scan_replacement()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads=1; SET disabled_optimizers='statistics_propagation';")?;

    Ok(conn)
}

#[cfg(target_pointer_width = "64")]
#[rstest]
#[case::signed_overflow(i32::MAX.into(), 4_294_967_299, 9_223_372_039_002_259_453, f64::from(i32::MAX))]
#[case::signed_underflow(i32::MIN.into(), 4_294_967_297, -9_223_372_039_002_259_456, f64::from(i32::MIN))]
#[case::unsigned_overflow(u32::MAX.into(), 4_294_967_298, 18_446_744_078_004_518_910, f64::from(u32::MAX))]
fn test_integer_aggregates_exceed_vortex_sum_range(
    #[case] value: Scalar,
    #[case] len: usize,
    #[case] expected_sum: i128,
    #[case] expected_mean: f64,
) -> VortexResult<()> {
    let file = constant_file(value, len)?;
    let conn = connection()?;
    let query = format!("SELECT sum(i), avg(i) FROM '{}'", file.path().display());
    let chunk = conn
        .query(&query)?
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("Expected one aggregate row"))?;
    assert_eq!(chunk.len(), 1);

    let sum = chunk
        .get_vector(0)
        .get_value(0, 1)
        .ok_or_else(|| vortex_err!("Expected a sum value"))?
        .extract();
    assert!(
        matches!(sum, ExtractedValue::HugeInt(actual) if actual == expected_sum),
        "{sum:?}"
    );

    let mean = chunk
        .get_vector(1)
        .get_value(0, 1)
        .ok_or_else(|| vortex_err!("Expected a mean value"))?
        .extract();
    assert!(
        matches!(mean, ExtractedValue::Double(actual) if actual == expected_mean),
        "{mean:?}"
    );

    Ok(())
}

#[rstest]
#[case::i8(1i8.into())]
#[case::i16(1i16.into())]
#[case::i32(1i32.into())]
#[case::i64(1i64.into())]
#[case::u8(1u8.into())]
#[case::u16(1u16.into())]
#[case::u32(1u32.into())]
#[case::u64(1u64.into())]
fn test_integer_sum_and_mean_stay_in_duckdb(
    #[case] value: Scalar,
    #[values("sum", "avg")] aggregate: &str,
) -> VortexResult<()> {
    let file = constant_file(value, 3)?;
    let conn = connection()?;
    let query = format!(
        "EXPLAIN SELECT {aggregate}(i) FROM '{}'",
        file.path().display(),
    );
    let mut plan = String::new();
    for chunk in conn.query(&query)? {
        for row in 0..chunk.len() {
            let value = chunk
                .get_vector(1)
                .get_value(row, chunk.len())
                .ok_or_else(|| vortex_err!("Expected an explain plan"))?;
            plan.push_str(&value.as_string());
        }
    }
    assert!(plan.contains("UNGROUPED_AGGREGATE"), "{plan}");

    Ok(())
}
