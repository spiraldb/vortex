// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;

use rstest::rstest;
use serde_json::Value;
use tempfile::NamedTempFile;
use vortex::error::VortexResult;
use vortex::error::vortex_err;

use crate::duckdb::Connection;
use crate::duckdb::Database;

fn query_string(conn: &Connection, sql: &str) -> VortexResult<String> {
    let result = conn.query(sql)?;
    let chunk = result
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("missing result"))?;
    let value = chunk
        .get_vector(0)
        .get_value(0, 1)
        .ok_or_else(|| vortex_err!("missing value"))?;
    Ok(value.as_string().to_string())
}

fn scan_rows(plan: &Value) -> u64 {
    let rows = if plan["extra_info"]["Function"] == "Vortex Scan" {
        plan["operator_cardinality"].as_u64().unwrap_or(0)
    } else {
        0
    };
    rows + plan["children"]
        .as_array()
        .map_or(0, |children| children.iter().map(scan_rows).sum())
}

#[rstest]
#[case::numeric("i / 3")]
#[case::strings("repeat('shared-prefix-', 16) || lpad(CAST(i / 3 AS BIGINT)::VARCHAR, 12, '0')")]
#[case::nullable("CASE WHEN i % 97 = 0 THEN NULL ELSE i / 3 END")]
#[case::floating("CASE WHEN i % 97 = 0 THEN 'nan'::DOUBLE ELSE i::DOUBLE END")]
#[case::all_null("NULL::BIGINT")]
#[case::decimal("CAST(i / 3 AS DECIMAL(16, 2))")]
#[case::date("DATE '2000-01-01' + (i / 3)::INTEGER")]
#[case::timestamp("TIMESTAMP '2000-01-01' + i * INTERVAL '1 microsecond'")]
fn top_n_scan_order_preserves_results(#[case] key: &str) -> VortexResult<()> {
    let file = NamedTempFile::with_suffix(".vortex")?;
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 4")?;
    conn.query(&format!(
        "CREATE TABLE reference AS SELECT i AS id, {key} AS key FROM range(200000) t(i)"
    ))?;
    conn.query(&format!(
        "COPY reference TO '{}' (FORMAT vortex)",
        file.path().display()
    ))?;
    for order in [
        "ASC NULLS FIRST",
        "ASC NULLS LAST",
        "DESC NULLS FIRST",
        "DESC NULLS LAST",
    ] {
        for (filter, offset) in [("", 0), ("WHERE id % 7 = 0", 19), ("", 70000)] {
            let query = |source: &str| {
                format!(
                    "SELECT to_json(list(id ORDER BY key {order}, id)) FROM (SELECT id, key FROM {source} {filter} ORDER BY key {order}, id LIMIT 31 OFFSET {offset})"
                )
            };
            let expected = query_string(&conn, &query("reference"))?;
            let actual = query_string(
                &conn,
                &query(&format!("read_vortex('{}')", file.path().display())),
            )?;
            assert_eq!(actual, expected, "{order}, {filter}, {offset}");
        }
    }
    Ok(())
}

#[test]
fn descending_order_visits_the_highest_zones_first() -> VortexResult<()> {
    let file = NamedTempFile::with_suffix(".vortex")?;
    let profile = NamedTempFile::with_suffix(".json")?;
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 1; SET late_materialization_max_rows = 0")?;
    conn.query(&format!(
        "COPY (SELECT CASE WHEN i < 500000 THEN i + 500000 ELSE i - 500000 END AS id FROM range(1000000) t(i)) TO '{}' (FORMAT vortex)",
        file.path().display()
    ))?;
    conn.query(&format!(
        "SET enable_profiling = 'json'; SET profiling_output = '{}'",
        profile.path().display()
    ))?;
    let actual = query_string(
        &conn,
        &format!(
            "SELECT to_json(list(id ORDER BY id DESC)) FROM (SELECT id FROM read_vortex('{}') ORDER BY id DESC LIMIT 100)",
            file.path().display()
        ),
    )?;
    let plan: Value =
        serde_json::from_reader(File::open(profile.path())?).map_err(|err| vortex_err!("{err}"))?;
    assert!(
        plan.to_string().contains("Scan Order"),
        "missing scan-order callback: {plan}"
    );
    let rows = scan_rows(&plan);
    assert!(
        rows > 0 && rows < 250000,
        "descending scan did not prioritize high zones: {rows}"
    );
    conn.query("PRAGMA disable_profiling")?;
    assert_eq!(
        actual,
        query_string(
            &conn,
            "SELECT to_json(list(i ORDER BY i DESC)) FROM range(999900, 1000000) t(i)"
        )?
    );
    Ok(())
}

#[rstest]
#[case::ascending("ASC", 2300)]
#[case::descending("DESC", 2300)]
fn offset_pruning_preserves_multi_file_row_ids(
    #[case] order: &str,
    #[case] offset: usize,
) -> VortexResult<()> {
    let dir = tempfile::tempdir()?;
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 4")?;
    let mut paths = Vec::new();
    // Physical file order differs from both sort directions.
    for index in [2, 0, 3, 1] {
        let path = dir.path().join(format!("part-{index}.vortex"));
        conn.query(&format!("COPY (SELECT i AS id, 'payload-' || i::VARCHAR AS payload FROM range({}, {}) t(i)) TO '{}' (FORMAT vortex)", index * 1000, (index + 1) * 1000, path.display()))?;
        paths.push(format!("'{}'", path.display()));
    }
    let files = format!("[{}]", paths.join(","));
    conn.query(&format!(
        "CREATE TABLE reference AS SELECT * FROM read_vortex({files})"
    ))?;
    // Reading the files above populates footer caches, allowing DuckDB to reduce OFFSET.
    for _ in 0..2 {
        let query = |source: &str| {
            format!(
                "SELECT to_json(list(payload ORDER BY id {order})) FROM (SELECT id, payload FROM {source} ORDER BY id {order} LIMIT 17 OFFSET {offset})"
            )
        };
        let expected = query_string(&conn, &query("reference"))?;
        let actual = query_string(&conn, &query(&format!("read_vortex({files})")))?;
        assert_eq!(actual, expected);
    }
    Ok(())
}
