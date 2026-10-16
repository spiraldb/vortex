// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use rstest::rstest;
use tempfile::NamedTempFile;
use vortex::array::IntoArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::stats::PRUNING_STATS;
use vortex::error::VortexResult;
use vortex::error::vortex_err;
use vortex::expr::stats::Stat;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::BlockingRuntime;

use crate::RUNTIME;
use crate::SESSION;
use crate::duckdb::Database;

#[rstest]
#[case::unique([Some(0), Some(1), Some(2), Some(3)], Some(4))]
#[case::nullable([None, Some(1), Some(2), Some(3)], Some(3))]
#[case::duplicates_and_hole([Some(0), Some(0), Some(0), Some(3)], None)]
#[case::unsorted_unique([Some(3), Some(2), Some(1), Some(0)], None)]
fn only_verified_integer_keys_report_distinct_counts(
    #[case] values: [Option<i32>; 4],
    #[case] expected: Option<u64>,
) -> VortexResult<()> {
    let file = NamedTempFile::with_suffix(".vortex")?;
    RUNTIME.block_on(async {
        let table = StructArray::from_fields(&[(
            "id",
            PrimitiveArray::from_option_iter(values).into_array(),
        )])?;
        let mut writer = async_fs::File::create(file.path()).await?;
        SESSION
            .write_options()
            .with_file_statistics(
                PRUNING_STATS
                    .iter()
                    .copied()
                    .chain([Stat::IsStrictSorted])
                    .collect(),
            )
            .write(&mut writer, table.into_array().to_array_stream())
            .await?;
        VortexResult::Ok(())
    })?;
    let db = Database::open_in_memory()?;
    crate::initialize(&db)?;
    let conn = db.connect()?;
    let check = expected.map_or_else(
        || "NOT contains(stats(id), '[Approx Unique:')".to_string(),
        |count| format!("contains(stats(id), '[Approx Unique: {count}]')"),
    );
    for _ in 0..3 {
        let result = conn.query(&format!(
            "SELECT CAST({check} AS BIGINT) FROM read_vortex('{}') LIMIT 1",
            file.path().display()
        ))?;
        let chunk = result
            .into_iter()
            .next()
            .ok_or_else(|| vortex_err!("missing statistics result"))?;
        assert_eq!(chunk.get_vector(0).as_slice_with_len::<i64>(1), &[1]);
    }
    let result = conn.query(&format!(
        "SELECT count(DISTINCT id % 2) FROM read_vortex('{}')",
        file.path().display()
    ))?;
    let chunk = result
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("missing projected distinct count"))?;
    assert_eq!(chunk.get_vector(0).as_slice_with_len::<i64>(1), &[2]);
    Ok(())
}
