// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;

use rstest::rstest;
use serde_json::Value;
use tempfile::NamedTempFile;
use vortex::error::VortexResult;
use vortex::error::vortex_err;

use crate::duckdb::Database;

fn scan_estimate(plan: &Value) -> Option<u64> {
    if plan["operator_type"] == "TABLE_SCAN" {
        return plan["extra_info"]["Estimated Cardinality"]
            .as_str()?
            .parse()
            .ok();
    }
    plan["children"].as_array()?.iter().find_map(scan_estimate)
}

#[rstest]
#[case::one_equality("id = 42")]
#[case::repeated_values("category = 0")]
#[case::two_equalities("id = 42 AND category = 0")]
#[case::consumed_comparison("id > 42")]
fn retained_equality_is_discounted_once(#[case] predicate: &str) -> VortexResult<()> {
    let directory = tempfile::tempdir()?;
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    // Keep the fixture unordered so it tests selectivity without a sorted-key uniqueness proof.
    conn.query("SET threads = 1; CREATE TABLE source AS SELECT id, id % 7 AS category FROM (SELECT (i * 37) % 10000 AS id FROM range(10000) t(i))")?;
    let mut estimates = Vec::new();
    for format in ["parquet", "vortex"] {
        let path = directory.path().join(format!("source.{format}"));
        conn.query(&format!(
            "COPY source TO '{}' (FORMAT {format})",
            path.display()
        ))?;
        let profile = NamedTempFile::with_suffix(".json")?;
        conn.query(&format!(
            "SET enable_profiling = 'json'; SET profiling_output = '{}'",
            profile.path().display()
        ))?;
        let result = conn.query(&format!(
            "SELECT id FROM read_{format}('{}') WHERE {predicate}",
            path.display()
        ))?;
        assert!(result.row_count() > 0);
        let plan: Value = serde_json::from_reader(File::open(profile.path())?)
            .map_err(|err| vortex_err!("{err}"))?;
        estimates.push(
            scan_estimate(&plan).ok_or_else(|| vortex_err!("missing scan estimate: {plan}"))?,
        );
        conn.query("PRAGMA disable_profiling")?;
    }
    assert_eq!(
        estimates,
        [2000, 2000],
        "Parquet and Vortex must each apply the filter selectivity once"
    );
    Ok(())
}
