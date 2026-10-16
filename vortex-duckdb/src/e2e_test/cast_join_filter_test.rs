// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![allow(clippy::too_many_arguments)]

use std::fs::File;

use rstest::rstest;
use serde_json::Value;
use tempfile::NamedTempFile;
use vortex::error::VortexResult;
use vortex::error::vortex_err;

use crate::duckdb::Connection;
use crate::duckdb::Database;

fn scan_rows(plan: &Value) -> Option<u64> {
    if plan["extra_info"]["Function"] == "Vortex Scan" {
        return plan["operator_cardinality"].as_u64();
    }
    plan["children"].as_array()?.iter().find_map(scan_rows)
}

fn count(conn: &Connection, query: &str) -> VortexResult<i64> {
    let chunk = conn
        .query(query)?
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("missing result"))?;
    Ok(chunk.get_vector(0).as_slice_with_len::<i64>(1)[0])
}

#[rstest]
#[case::signed("INTEGER", "i - 50000", true, false)]
#[case::unsigned("UINTEGER", "i + 4294867296", true, false)]
#[case::wide_signed("BIGINT", "i + 9007199254740992", false, false)]
#[case::wide_unsigned("UBIGINT", "i + 9007199254740992", false, false)]
#[case::i8("TINYINT", "i % 31 - 15", true, true)]
#[case::i16("SMALLINT", "i % 31 - 15", true, true)]
#[case::u8("UTINYINT", "i % 31", true, true)]
#[case::u16("USMALLINT", "i % 31", true, true)]
fn exact_double_cast_filters(
    #[case] key_type: &str,
    #[case] key_value: &str,
    #[case] optimized: bool,
    #[case] small_domain: bool,
    #[values(false, true)] nullable: bool,
    #[values(0, 4096)] in_threshold: u64,
    #[values(false, true)] nonfinite: bool,
    #[values(false, true)] multiple_files: bool,
) -> VortexResult<()> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join(if multiple_files {
        "part-*.vortex"
    } else {
        "keys.vortex"
    });
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query(&format!(
        "SET threads = 1; SET dynamic_or_filter_threshold = {in_threshold}"
    ))?;
    let probe_key = if nullable {
        format!("CASE WHEN i % 17 = 0 THEN NULL ELSE {key_value} END")
    } else {
        key_value.to_owned()
    };
    conn.query(&format!("CREATE TABLE probe_reference AS SELECT ({probe_key})::{key_type} AS id FROM range(100000) t(i)"))?;
    if multiple_files {
        for (index, predicate) in [(0, "rowid < 50000"), (1, "rowid >= 50000")] {
            conn.query(&format!(
                "COPY (SELECT * FROM probe_reference WHERE {predicate}) TO '{}' (FORMAT vortex)",
                directory
                    .path()
                    .join(format!("part-{index}.vortex"))
                    .display()
            ))?;
        }
    } else {
        conn.query(&format!(
            "COPY probe_reference TO '{}' (FORMAT vortex)",
            file.display()
        ))?;
    }
    let build_key = if nonfinite {
        format!(
            "CASE WHEN i = 0 THEN NULL WHEN i = 1 THEN 'NaN'::DOUBLE WHEN i = 2 THEN 'Infinity'::DOUBLE WHEN i = 3 THEN '-Infinity'::DOUBLE WHEN i = 4 THEN 1e100 WHEN i = 5 THEN ({key_value})::DOUBLE + 0.5 WHEN i = 6 THEN '-0.0'::DOUBLE ELSE ({key_value})::DOUBLE END"
        )
    } else {
        format!("({key_value})::DOUBLE")
    };
    let sparse = if small_domain {
        "i < 30000 AND i % 10000 = 0"
    } else {
        "i BETWEEN 40000 AND 49990 AND i % 10 = 0"
    };
    let special = if nonfinite { "i < 7 OR " } else { "" };
    conn.query(&format!("CREATE TABLE build AS SELECT {build_key} AS id, {special}({sparse}) AS keep FROM range(100000) t(i), range(2)"))?;
    let expected = count(
        &conn,
        "SELECT count(*) FROM probe_reference p JOIN build b ON p.id::DOUBLE = b.id WHERE b.keep",
    )?;
    let query = format!(
        "SELECT count(*) FROM read_vortex('{}') p JOIN build b ON p.id::DOUBLE = b.id WHERE b.keep",
        file.display()
    );
    let profile = NamedTempFile::with_suffix(".json")?;
    conn.query(&format!(
        "SET enable_profiling = 'json'; SET profiling_output = '{}'",
        profile.path().display()
    ))?;
    assert_eq!(count(&conn, &query)?, expected);
    let plan: Value = serde_json::from_reader(File::open(profile.path())?)
        .map_err(|error| vortex_err!("{error}"))?;
    let scanned = scan_rows(&plan).ok_or_else(|| vortex_err!("missing scan: {plan}"))?;
    if optimized {
        let limit = if small_domain { 15000 } else { 3000 };
        assert!(
            scanned < limit,
            "DOUBLE cast prevented pruning: {scanned}; {plan}"
        );
        if in_threshold > 0 && !nonfinite {
            assert!(
                plan.to_string().contains("IN ("),
                "missing IN filter: {plan}"
            );
        }
    } else {
        assert_eq!(
            scanned, 100000,
            "Inexact casts must keep their existing scan: {plan}"
        );
    }
    conn.query("SET enable_profiling = 'no_output'")?;
    let null_safe_expected = count(
        &conn,
        "SELECT count(*) FROM probe_reference p JOIN build b ON p.id::DOUBLE IS NOT DISTINCT FROM b.id WHERE b.keep",
    )?;
    assert_eq!(
        count(
            &conn,
            &format!(
                "SELECT count(*) FROM read_vortex('{}') p JOIN build b ON p.id::DOUBLE IS NOT DISTINCT FROM b.id WHERE b.keep",
                file.display()
            )
        )?,
        null_safe_expected
    );
    for _ in 0..2 {
        conn.query(&format!("SELECT p.id FROM read_vortex('{}') p JOIN build b ON p.id::DOUBLE = b.id WHERE b.keep LIMIT 1", file.display()))?;
        assert_eq!(count(&conn, &query)?, expected);
    }
    Ok(())
}

#[test]
fn unmatchable_double_in_values_preserve_empty_join() -> VortexResult<()> {
    let file = NamedTempFile::with_suffix(".vortex")?;
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 1; SET dynamic_or_filter_threshold = 4096")?;
    conn.query(&format!(
        "COPY (SELECT i::INTEGER AS id FROM range(100) t(i)) TO '{}' (FORMAT vortex)",
        file.path().display()
    ))?;
    conn.query("CREATE TABLE build AS SELECT CASE i WHEN 0 THEN 0.5 WHEN 1 THEN 1.5 ELSE 1e100 END AS id, i < 3 AS keep FROM range(100) t(i), range(2)")?;
    assert_eq!(
        count(
            &conn,
            &format!(
                "SELECT count(*) FROM read_vortex('{}') p JOIN build b ON p.id::DOUBLE = b.id WHERE b.keep",
                file.path().display()
            )
        )?,
        0
    );
    Ok(())
}
