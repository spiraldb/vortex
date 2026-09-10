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

#[track_caller]
fn check_aggregates(
    conn: &Connection,
    query: &str,
    expected_sum: i128,
    expected_mean: f64,
) -> VortexResult<()> {
    let chunk = conn
        .query(query)?
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

fn explain(conn: &Connection, query: &str) -> VortexResult<String> {
    let mut plan = String::new();
    for chunk in conn.query(&format!("EXPLAIN {query}"))? {
        for row in 0..chunk.len() {
            let value = chunk
                .get_vector(1)
                .get_value(row, chunk.len())
                .ok_or_else(|| vortex_err!("Expected an explain plan"))?;
            plan.push_str(&value.as_string());
        }
    }
    Ok(plan)
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
    check_aggregates(&conn, &query, expected_sum, expected_mean)
}

// DuckDB casts prevent mixed-query pushdown for i8 and unsigned inputs.
#[rstest]
#[case::i8(i8::MAX.into(), false)]
#[case::i16(i16::MAX.into(), true)]
#[case::i32(i32::MAX.into(), true)]
#[case::i64(i64::MAX.into(), false)]
#[case::u8(u8::MAX.into(), false)]
#[case::u16(u16::MAX.into(), false)]
#[case::u32(u32::MAX.into(), false)]
#[case::u64(u64::MAX.into(), false)]
fn test_mixed_integer_aggregate_pushdown(
    #[case] value: Scalar,
    #[case] pushed: bool,
    #[values("sum", "avg", "mean")] aggregate: &str,
) -> VortexResult<()> {
    let file = constant_file(value, 3)?;
    let conn = connection()?;
    let plan = explain(
        &conn,
        &format!(
            "SELECT {aggregate}(i), min(i), max(i), count(i) FROM '{}'",
            file.path().display(),
        ),
    )?;
    assert_eq!(!plan.contains("UNGROUPED_AGGREGATE"), pushed, "{plan}");
    Ok(())
}

#[rstest]
#[case::without_statistics(false)]
#[case::with_statistics(true)]
fn test_sum_no_overflow_pushdown(#[case] statistics: bool) -> VortexResult<()> {
    let file = constant_file(1i64.into(), 3)?;
    let conn = connection()?;
    if statistics {
        conn.query("SET disabled_optimizers='';")?;
    }
    let query = format!("SELECT sum(i) FROM '{}'", file.path().display());
    let plan = explain(&conn, &query)?;
    assert_eq!(!plan.contains("UNGROUPED_AGGREGATE"), statistics, "{plan}");
    let chunk = conn
        .query(&query)?
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("Expected one aggregate row"))?;
    let sum = chunk
        .get_vector(0)
        .get_value(0, 1)
        .ok_or_else(|| vortex_err!("Expected a sum value"))?
        .extract();
    assert!(matches!(sum, ExtractedValue::HugeInt(3)), "{sum:?}");
    Ok(())
}

#[cfg(target_pointer_width = "64")]
#[rstest]
#[case::fits(4_294_967_296, true)]
#[case::overflows(4_294_967_297, false)]
fn test_integer_aggregate_row_bound_boundary(
    #[case] len: usize,
    #[case] pushed: bool,
    #[values("", " WHERE i < 0")] filter: &str,
) -> VortexResult<()> {
    let file = constant_file(i32::MIN.into(), len)?;
    let conn = connection()?;
    let query = format!(
        "SELECT sum(i), avg(i) FROM '{}'{filter}",
        file.path().display()
    );
    let plan = explain(&conn, &query)?;
    assert_eq!(!plan.contains("UNGROUPED_AGGREGATE"), pushed, "{plan}");
    check_aggregates(
        &conn,
        &query,
        i128::from(i32::MIN) * len as i128,
        f64::from(i32::MIN),
    )
}

#[cfg(target_pointer_width = "64")]
#[rstest]
fn test_multiple_files_do_not_use_first_file_row_bound(
    #[values(false, true)] prune_first: bool,
) -> VortexResult<()> {
    let first = constant_file(1i32.into(), 1)?;
    let second = constant_file(i32::MAX.into(), 4_294_967_299)?;
    let conn = connection()?;
    let mut query = format!(
        "SELECT sum(i), avg(i) FROM read_vortex(['{}', '{}'], filename=true)",
        first.path().display(),
        second.path().display()
    );
    if prune_first {
        query.push_str(&format!(" WHERE filename = '{}'", second.path().display()));
    }
    let plan = explain(&conn, &query)?;
    assert!(plan.contains("UNGROUPED_AGGREGATE"), "{plan}");
    let expected_sum = 9_223_372_039_002_259_453i128 + i128::from(!prune_first);
    let count = 4_294_967_299u64 + u64::from(!prune_first);
    check_aggregates(
        &conn,
        &query,
        expected_sum,
        expected_sum as f64 / count as f64,
    )
}

#[test]
fn test_single_row_i64_aggregates() -> VortexResult<()> {
    let file = constant_file(i64::MAX.into(), 1)?;
    let conn = connection()?;
    let query = format!("SELECT sum(i), avg(i) FROM '{}'", file.path().display());
    let plan = explain(&conn, &query)?;
    assert!(!plan.contains("UNGROUPED_AGGREGATE"), "{plan}");
    check_aggregates(&conn, &query, i128::from(i64::MAX), i64::MAX as f64)
}
