// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;

use rstest::rstest;
use serde_json::Value;
use tempfile::NamedTempFile;
use vortex::array::IntoArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::error::VortexResult;
use vortex::error::vortex_err;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::BlockingRuntime;

use crate::RUNTIME;
use crate::SESSION;
use crate::duckdb::Connection;
use crate::duckdb::Database;

fn vortex_scan_cardinality(plan: &Value) -> Option<u64> {
    if plan["extra_info"]["Function"] == "Vortex Scan" {
        return plan["operator_cardinality"].as_u64();
    }
    plan["children"]
        .as_array()?
        .iter()
        .find_map(vortex_scan_cardinality)
}

fn count_query(conn: &Connection, query: &str) -> VortexResult<i64> {
    let result = conn.query(query)?;
    let chunk = result
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("missing count result"))?;
    Ok(chunk.get_vector(0).as_slice_with_len::<i64>(1)[0])
}

#[rstest]
#[case::i32("INTEGER")]
#[case::i64("BIGINT")]
#[case::u32("UINTEGER")]
#[case::u64("UBIGINT")]
#[case::float("DOUBLE")]
#[case::string("VARCHAR")]
#[case::decimal("DECIMAL(12, 2)")]
fn bloom_hashes_match_duckdb_for_keys(
    #[case] key_type: &str,
    #[values(false, true)] nullable: bool,
) -> VortexResult<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("keys.vortex");
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 4; SET dynamic_or_filter_threshold = 0")?;
    let key = if nullable {
        "CASE WHEN i < 20000 OR i % 17 = 0 THEN NULL ELSE i END"
    } else {
        "i"
    };
    conn.query(&format!(
        "CREATE TABLE probe_reference AS SELECT CAST({key} AS {key_type}) AS key FROM range(100000) t(i)"
    ))?;
    conn.query(&format!(
        "COPY probe_reference TO '{}' (FORMAT vortex)",
        file.display()
    ))?;
    conn.query(&format!(
        "CREATE TABLE build AS SELECT CAST(CASE WHEN i = 0 THEN NULL ELSE i END AS {key_type}) AS key, i = 0 OR (i BETWEEN 40000 AND 49990 AND i % 10 = 0) AS keep FROM range(100000) t(i), range(2)"
    ))?;
    let reference = count_query(
        &conn,
        "SELECT count(*) FROM probe_reference JOIN build USING (key) WHERE build.keep",
    )?;
    let query = format!(
        "SELECT count(*) FROM read_vortex('{}') JOIN build USING (key) WHERE build.keep",
        file.display()
    );
    assert_eq!(count_query(&conn, &query)?, reference);
    let null_safe_reference = count_query(
        &conn,
        "SELECT count(*) FROM probe_reference probe JOIN build ON probe.key IS NOT DISTINCT FROM build.key WHERE build.keep",
    )?;
    let null_safe_query = format!(
        "SELECT count(*) FROM read_vortex('{}') probe JOIN build ON probe.key IS NOT DISTINCT FROM build.key WHERE build.keep",
        file.display()
    );
    assert_eq!(count_query(&conn, &null_safe_query)?, null_safe_reference);
    // Repeated early termination must safely release the snapshot and any outstanding scan work.
    for _ in 0..3 {
        conn.query(&format!(
            "SELECT key FROM read_vortex('{}') JOIN build USING (key) WHERE build.keep LIMIT 1",
            file.display()
        ))?;
        assert_eq!(count_query(&conn, &query)?, reference);
    }
    Ok(())
}

#[test]
fn consumed_build_predicate_enables_bloom_filter() -> VortexResult<()> {
    let directory = tempfile::tempdir()?;
    let probe = directory.path().join("probe.vortex");
    let build = directory.path().join("build.vortex");
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 1; SET dynamic_or_filter_threshold = 0")?;
    conn.query(&format!(
        "COPY (SELECT i AS id FROM range(100000) t(i)) TO '{}' (FORMAT vortex)",
        probe.display()
    ))?;
    conn.query(&format!(
        "COPY (SELECT i AS id, CASE WHEN i BETWEEN 40000 AND 49990 AND i % 10 = 0 THEN 'keep-' ELSE 'drop-' END || i::VARCHAR AS tag FROM range(100000) t(i), range(2)) TO '{}' (FORMAT vortex)",
        build.display()
    ))?;
    let profile = NamedTempFile::with_suffix(".json")?;
    conn.query(&format!(
        "SET enable_profiling = 'json'; SET profiling_output = '{}'",
        profile.path().display()
    ))?;
    assert_eq!(
        count_query(
            &conn,
            &format!(
                "SELECT count(*) FROM read_vortex('{}') probe JOIN read_vortex('{}') build USING (id) WHERE build.tag LIKE 'keep-%'",
                probe.display(),
                build.display()
            )
        )?,
        2000
    );
    let plan: Value = serde_json::from_reader(File::open(profile.path())?)
        .map_err(|error| vortex_err!("{error}"))?;
    let scanned = vortex_scan_cardinality(&plan)
        .ok_or_else(|| vortex_err!("missing Vortex scan in plan: {plan}"))?;
    assert!(
        scanned < 3000,
        "Consumed build predicate failed to enable Bloom pruning: {scanned}"
    );
    Ok(())
}

#[test]
fn derived_join_bounds_do_not_mark_build_selective() -> VortexResult<()> {
    let directory = tempfile::tempdir()?;
    let probe = directory.path().join("probe.vortex");
    let build = directory.path().join("build.vortex");
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 1; SET dynamic_or_filter_threshold = 0")?;
    conn.query(&format!(
        "COPY (SELECT (i % 49000) * 1000 AS id FROM range(200000) t(i)) TO '{}' (FORMAT vortex)",
        probe.display()
    ))?;
    conn.query(&format!(
        "COPY (SELECT (i % 50000) * 1000 AS id FROM range(100000) t(i)) TO '{}' (FORMAT vortex)",
        build.display()
    ))?;
    let profile = NamedTempFile::with_suffix(".json")?;
    conn.query(&format!(
        "SET enable_profiling = 'json'; SET profiling_output = '{}'",
        profile.path().display()
    ))?;
    assert_eq!(
        count_query(
            &conn,
            &format!(
                "SELECT count(*) FROM read_vortex('{}') probe JOIN read_vortex('{}') build USING (id)",
                probe.display(),
                build.display()
            )
        )?,
        400000
    );
    let plan: Value = serde_json::from_reader(File::open(profile.path())?)
        .map_err(|error| vortex_err!("{error}"))?;
    assert!(
        !plan.to_string().contains("IN BF("),
        "Derived bounds should not enable a Bloom filter that passes every probe key: {plan}"
    );
    Ok(())
}

#[test]
fn bloom_join_filter_prunes_sparse_keys() -> VortexResult<()> {
    let file = NamedTempFile::with_suffix(".vortex")?;
    RUNTIME.block_on(async {
        let table = StructArray::try_from_iter([(
            "id",
            PrimitiveArray::from_iter(0..100_000i64).into_array(),
        )])?;
        let mut writer = async_fs::File::create(file.path()).await?;
        SESSION
            .write_options()
            .write(&mut writer, table.into_array().to_array_stream())
            .await?;
        VortexResult::Ok(())
    })?;

    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    conn.query("SET threads = 1; SET dynamic_or_filter_threshold = 0")?;
    // Duplicate keys prevent DuckDB's runtime perfect hash join from suppressing Bloom filters.
    conn.query(
        "CREATE TABLE build AS SELECT i AS id, i BETWEEN 40000 AND 49990 AND i % 10 = 0 AS keep FROM range(100000) t(i), range(2)",
    )?;
    let query = format!(
        "SELECT count(*) FROM read_vortex('{}') probe JOIN build USING (id) WHERE build.keep",
        file.path().display()
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
        .ok_or_else(|| vortex_err!("missing join result"))?;
    assert_eq!(chunk.get_vector(0).as_slice_with_len::<i64>(1), &[2000]);

    let plan: Value =
        serde_json::from_reader(File::open(profile.path())?).map_err(|err| vortex_err!("{err}"))?;
    let scanned = vortex_scan_cardinality(&plan)
        .ok_or_else(|| vortex_err!("missing Vortex scan in plan: {plan}"))?;
    // Min/max bounds alone leave 9,991 rows; the Bloom filter keeps the sparse keys and a few
    // false positives. The exact join still returns both copies of each matching key.
    assert!(
        scanned < 3_000,
        "Bloom filter did not prune sparse keys: {scanned}"
    );
    Ok(())
}
