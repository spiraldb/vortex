// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;

use rstest::rstest;
use serde_json::Value;
use tempfile::NamedTempFile;
use vortex::error::VortexResult;
use vortex::error::vortex_err;

use crate::duckdb::Database;

fn scan_rows(plan: &Value, filename: &str) -> Option<u64> {
    if plan["extra_info"]["Filename(s)"].as_str() == Some(filename) {
        return plan["operator_cardinality"].as_u64();
    }
    plan["children"]
        .as_array()?
        .iter()
        .find_map(|child| scan_rows(child, filename))
}

#[rstest]
fn choose_filtered_build_without_replacing_key_only_build(
    #[values(false, true)] key_only: bool,
    #[values(false, true)] nested: bool,
) -> VortexResult<()> {
    let directory = tempfile::tempdir()?;
    let left = directory.path().join("wide.vortex");
    let right = directory.path().join("probe.vortex");
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 1; SET dynamic_or_filter_threshold = 0")?;
    let payloads = (0..4)
        .map(|i| format!("'payload-{i}-' || i::VARCHAR AS p{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.query(&format!(
        "COPY (SELECT i AS id, {payloads}, CASE WHEN i BETWEEN 40000 AND 49990 AND i % 10 = 0 THEN 'keep-' ELSE 'drop-' END || i::VARCHAR AS tag FROM range(200000) t(i), range(2)) TO '{}' (FORMAT vortex)",
        left.display()
    ))?;
    conn.query(&format!(
        "COPY (SELECT i AS id, 'right-' || i::VARCHAR AS payload FROM range(100000) t(i)) TO '{}' (FORMAT vortex)",
        right.display()
    ))?;
    let aggregates = (0..4)
        .map(|i| format!("min(l.p{i})"))
        .collect::<Vec<_>>()
        .join(", ");
    let right_payload = if key_only { "" } else { ", min(r.payload)" };
    let nested_join = if nested {
        format!("JOIN read_vortex('{}') r2 USING(id)", right.display())
    } else {
        String::new()
    };
    let nested_payload = if nested && !key_only {
        ", min(r2.payload)"
    } else {
        ""
    };
    let query = format!(
        "SELECT count(*), {aggregates}{right_payload}{nested_payload} FROM read_vortex('{}') l JOIN read_vortex('{}') r USING(id) {nested_join} WHERE l.tag LIKE 'keep-%'",
        left.display(),
        right.display()
    );
    let profile = NamedTempFile::with_suffix(".json")?;
    conn.query(&format!(
        "SET enable_profiling = 'json'; SET profiling_output = '{}'",
        profile.path().display()
    ))?;
    let result = conn.query(&query)?;
    let chunk = result
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("missing result"))?;
    assert_eq!(chunk.get_vector(0).as_slice_with_len::<i64>(1), &[2000]);
    for index in 0..4 {
        let value = chunk
            .get_vector(index + 1)
            .get_value(0, 1)
            .ok_or_else(|| vortex_err!("missing payload result"))?;
        assert_eq!(value.to_string(), format!("payload-{index}-40000"));
    }
    if !key_only {
        let value = chunk
            .get_vector(5)
            .get_value(0, 1)
            .ok_or_else(|| vortex_err!("missing right payload"))?;
        assert_eq!(value.to_string(), "right-40000");
        if nested {
            let value = chunk
                .get_vector(6)
                .get_value(0, 1)
                .ok_or_else(|| vortex_err!("missing nested payload"))?;
            assert_eq!(value.to_string(), "right-40000");
        }
    }
    let plan: Value = serde_json::from_reader(File::open(profile.path())?)
        .map_err(|error| vortex_err!("{error}"))?;
    if !nested {
        let scanned = scan_rows(&plan, &right.display().to_string())
            .ok_or_else(|| vortex_err!("missing probe scan: {plan}"))?;
        if key_only {
            assert_eq!(
                scanned, 100000,
                "Compact key-only build should be preserved: {plan}"
            );
        } else {
            assert!(
                scanned < 3000,
                "Filtered payload build should prune the probe: {scanned}; {plan}"
            );
        }
    }
    Ok(())
}

#[test]
fn capped_probe_estimate_enables_filtered_compound_build() -> VortexResult<()> {
    let directory = tempfile::tempdir()?;
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 1; SET dynamic_or_filter_threshold = 0")?;
    for (name, sql) in [
        (
            "a",
            "SELECT i AS id, i % 10 AS kind, CASE WHEN i BETWEEN 40000 AND 49990 AND i % 10 = 0 THEN 'keep' ELSE 'drop' END AS tag FROM range(100000) t(i)",
        ),
        (
            "b",
            "SELECT i % 10 AS kind, 'payload-' || i::VARCHAR AS payload, CASE WHEN i = 0 THEN 'keep' ELSE 'drop' END AS tag FROM range(100000) t(i)",
        ),
        ("c", "SELECT 0::BIGINT AS kind"),
        (
            "wide",
            "SELECT i AS id, 'right-' || i::VARCHAR AS payload FROM range(200000) t(i)",
        ),
    ] {
        let path = directory.path().join(format!("{name}.vortex"));
        conn.query(&format!(
            "COPY ({sql}) TO '{}' (FORMAT vortex)",
            path.display()
        ))?;
        conn.query(&format!(
            "CREATE VIEW {name} AS SELECT * FROM read_vortex('{}')",
            path.display()
        ))?;
    }
    let profile = NamedTempFile::with_suffix(".json")?;
    conn.query(&format!(
        "SET enable_profiling = 'json'; SET profiling_output = '{}'",
        profile.path().display()
    ))?;
    let result = conn.query(
        "SELECT count(*), min(l.payload), min(r.payload) FROM (SELECT a.id, b.payload FROM a JOIN b USING(kind) JOIN c USING(kind) WHERE a.tag = 'keep' AND b.tag = 'keep' ORDER BY a.id) l JOIN wide r USING(id)",
    )?;
    let chunk = result
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("missing result"))?;
    assert_eq!(chunk.get_vector(0).as_slice_with_len::<i64>(1), &[1000]);
    for (index, expected) in [(1, "payload-0"), (2, "right-40000")] {
        let value = chunk
            .get_vector(index)
            .get_value(0, 1)
            .ok_or_else(|| vortex_err!("missing payload result"))?;
        assert_eq!(value.to_string(), expected);
    }
    let plan: Value = serde_json::from_reader(File::open(profile.path())?)
        .map_err(|error| vortex_err!("{error}"))?;
    let wide_path = directory.path().join("wide.vortex");
    let scanned = scan_rows(&plan, &wide_path.display().to_string())
        .ok_or_else(|| vortex_err!("missing wide scan: {plan}"))?;
    assert!(
        scanned < 60000,
        "Inflated probe estimate prevented runtime pruning: {scanned}; {plan}"
    );
    Ok(())
}
