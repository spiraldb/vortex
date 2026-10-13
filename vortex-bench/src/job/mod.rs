// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Join Order Benchmark (JOB).
//!
//! The 113 join-heavy queries from Leis et al., "How Good Are Query Optimizers, Really?"
//! (VLDB 2015), over the IMDB dataset. Queries and data mirror DuckDB's in-tree
//! `benchmark/imdb` suite: the queries are copied from `benchmark/imdb_plan_cost/queries/`
//! into `vortex-bench/sql/job/`, and the 21 tables are downloaded from the Parquet files
//! DuckDB publishes for that suite.
//!
//! The upstream Parquet files are re-encoded with zstd level 3 so the Parquet baseline uses
//! the same codec as every other generated benchmark dataset.
//!
//! ## Query numbering
//!
//! Upstream queries are named `1a` ... `33c`. Benchmark query indices must be integers, so
//! queries are numbered `1..=113` in upstream order (`1a` = 1, `1b` = 2, ..., `33c` = 113).
//! [`JOB_QUERY_NAMES`] holds the mapping.

use std::fs;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use glob::Pattern;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use tracing::info;
use url::Url;
use vortex::error::VortexExpect;

use crate::Benchmark;
use crate::BenchmarkDataset;
use crate::Format;
use crate::TableSpec;
use crate::datasets::data_downloads::download_many;
use crate::utils::file::idempotent;
use crate::utils::file::resolve_data_url;

/// Release that hosts DuckDB's JOB Parquet files, as referenced by DuckDB's
/// `benchmark/imdb/init/load.sql`. Each table lives at `{UPSTREAM_BASE_URL}/job_{table}.parquet`.
const UPSTREAM_BASE_URL: &str = "https://github.com/duckdb/duckdb-data/releases/download/v1.0";

/// The 21 IMDB tables referenced by the JOB queries.
pub(crate) const TABLES: &[&str] = &[
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    "role_type",
    "title",
];

/// Upstream JOB query names. Query index `i` (1-based) runs `JOB_QUERY_NAMES[i - 1]`, stored
/// at `vortex-bench/sql/job/{name}.sql`.
pub const JOB_QUERY_NAMES: &[&str] = &[
    "01a", "01b", "01c", "01d", "02a", "02b", "02c", "02d", "03a", "03b", "03c", "04a", "04b",
    "04c", "05a", "05b", "05c", "06a", "06b", "06c", "06d", "06e", "06f", "07a", "07b", "07c",
    "08a", "08b", "08c", "08d", "09a", "09b", "09c", "09d", "10a", "10b", "10c", "11a", "11b",
    "11c", "11d", "12a", "12b", "12c", "13a", "13b", "13c", "13d", "14a", "14b", "14c", "15a",
    "15b", "15c", "15d", "16a", "16b", "16c", "16d", "17a", "17b", "17c", "17d", "17e", "17f",
    "18a", "18b", "18c", "19a", "19b", "19c", "19d", "20a", "20b", "20c", "21a", "21b", "21c",
    "22a", "22b", "22c", "22d", "23a", "23b", "23c", "24a", "24b", "25a", "25b", "25c", "26a",
    "26b", "26c", "27a", "27b", "27c", "28a", "28b", "28c", "29a", "29b", "29c", "30a", "30b",
    "30c", "31a", "31b", "31c", "32a", "32b", "33a", "33b", "33c",
];

/// All JOB queries as `(index, sql)` pairs, numbered as described in the module docs.
pub fn job_queries() -> impl Iterator<Item = (usize, String)> {
    JOB_QUERY_NAMES
        .iter()
        .enumerate()
        .map(|(i, name)| (i + 1, job_query(name)))
}

fn job_query(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("sql")
        .join("job")
        .join(name)
        .with_extension("sql");
    fs::read_to_string(path).vortex_expect("cannot load JOB query from file")
}

/// Benchmark over the [Join Order Benchmark][paper] on the IMDB dataset.
///
/// [paper]: https://www.vldb.org/pvldb/vol9/p204-leis.pdf
pub struct JobBenchmark {
    data_url: Url,
}

impl JobBenchmark {
    pub fn new(data_url: Url) -> Self {
        Self { data_url }
    }

    pub fn with_remote_data_dir(use_remote_data_dir: Option<String>) -> anyhow::Result<Self> {
        let data_url = resolve_data_url(use_remote_data_dir.as_deref(), "job")?;
        Ok(Self { data_url })
    }

    fn base_dir(&self) -> anyhow::Result<PathBuf> {
        self.data_url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!(
                "Failed to convert data URL to filesystem path - ensure data_url uses 'file://' scheme"
            ))
    }
}

#[async_trait::async_trait]
impl Benchmark for JobBenchmark {
    fn doc_path(&self) -> &'static str {
        "vortex-bench/sql/job/README.md"
    }

    fn queries(&self) -> anyhow::Result<Vec<(usize, String)>> {
        Ok(job_queries().collect())
    }

    async fn generate_base_data(&self) -> anyhow::Result<()> {
        if self.data_url.scheme() != "file" {
            return Ok(());
        }

        let base_dir = self.base_dir()?;
        let parquet_dir = base_dir.join(Format::Parquet.name());
        let missing: Vec<&str> = TABLES
            .iter()
            .copied()
            .filter(|t| !parquet_dir.join(format!("{t}.parquet")).exists())
            .collect();
        if missing.is_empty() {
            info!(
                "job: {} Parquet tables already present in {}",
                TABLES.len(),
                parquet_dir.display(),
            );
            return Ok(());
        }

        let upstream_dir = base_dir.join("upstream");
        fs::create_dir_all(&upstream_dir)?;
        fs::create_dir_all(&parquet_dir)?;

        download_many(missing.iter().map(|t| {
            (
                upstream_dir.join(format!("job_{t}.parquet")),
                format!("{UPSTREAM_BASE_URL}/job_{t}.parquet"),
            )
        }))
        .await?;

        for table in missing {
            let src = upstream_dir.join(format!("job_{table}.parquet"));
            let dst = parquet_dir.join(format!("{table}.parquet"));
            tokio::task::spawn_blocking(move || reencode_parquet(&src, &dst)).await??;
        }

        info!(
            "job base data generated in {} ({} Parquet tables)",
            parquet_dir.display(),
            TABLES.len(),
        );
        Ok(())
    }

    fn dataset(&self) -> BenchmarkDataset {
        BenchmarkDataset::Job
    }

    fn dataset_name(&self) -> &str {
        "job"
    }

    fn dataset_display(&self) -> String {
        "job".to_owned()
    }

    fn data_url(&self) -> &Url {
        &self.data_url
    }

    fn table_specs(&self) -> Vec<TableSpec> {
        TABLES
            .iter()
            .map(|name| TableSpec::new(name, None))
            .collect()
    }

    #[expect(clippy::expect_used)]
    fn pattern(&self, table_name: &str, format: Format) -> Option<Pattern> {
        Some(
            format!("{}.{}", table_name, format.ext())
                .parse()
                .expect("valid glob pattern"),
        )
    }
}

/// Rewrite the upstream Parquet file at `src` to `dst` with zstd level 3 compression.
fn reencode_parquet(src: &Path, dst: &Path) -> anyhow::Result<PathBuf> {
    idempotent(dst, |tmp| {
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            File::open(src).with_context(|| format!("opening {}", src.display()))?,
        )?;
        let schema = builder.schema().clone();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .build();
        let mut writer = ArrowWriter::try_new(File::create(tmp)?, schema, Some(props))?;
        for batch in builder.build()? {
            writer.write(&batch?)?;
        }
        writer.close()?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_query_file_loads() {
        let queries: Vec<_> = job_queries().collect();
        assert_eq!(queries.len(), 113);
        assert_eq!(queries.first().map(|(i, _)| *i), Some(1));
        assert_eq!(queries.last().map(|(i, _)| *i), Some(113));
        assert!(queries.iter().all(|(_, sql)| sql.contains("SELECT")));
    }
}
