// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::env;
use std::fs;
use std::path::Path;

use anyhow::Result;
use url::Url;

use crate::Benchmark;
use crate::BenchmarkDataset;
use crate::IdempotentPath;
use crate::TableSpec;
use crate::clickbench::*;
use crate::utils::file::resolve_data_url;

/// ClickBench benchmark implementation
pub struct ClickBenchBenchmark {
    pub flavor: Flavor,
    pub queries_file: Option<String>,
    pub data_url: Url,
}

impl ClickBenchBenchmark {
    pub fn new(
        flavor: Flavor,
        queries_file: Option<String>,
        use_remote_data_dir: Option<String>,
    ) -> Result<Self> {
        let url = Self::create_data_url(use_remote_data_dir.as_deref(), flavor)?;
        Ok(Self {
            flavor,
            queries_file,
            data_url: url,
        })
    }

    fn create_data_url(remote_data_dir: Option<&str>, flavor: Flavor) -> Result<Url> {
        resolve_data_url(remote_data_dir, &format!("clickbench_{flavor}"))
    }
}

/// ClickBench sorted by event time, with shard filenames shuffled to exercise sort pushdown.
pub struct ClickBenchSortedBenchmark {
    pub queries_file: Option<String>,
    pub data_url: Url,
}

impl ClickBenchSortedBenchmark {
    /// Create the sorted ClickBench benchmark, optionally using a remote data directory.
    pub fn new(use_remote_data_dir: Option<String>) -> Result<Self> {
        Ok(Self {
            queries_file: None,
            data_url: resolve_data_url(use_remote_data_dir.as_deref(), CLICKBENCH_SORTED_NAME)?,
        })
    }
}

fn read_clickbench_queries(queries_file: Option<&str>) -> Result<Vec<(usize, String)>> {
    let queries_filepath = match queries_file {
        Some(file) => file.into(),
        None => Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("sql")
            .join("clickbench_queries.sql"),
    };

    Ok(fs::read_to_string(queries_filepath)?
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .enumerate()
        .collect())
}

#[async_trait::async_trait]
impl Benchmark for ClickBenchBenchmark {
    fn doc_path(&self) -> &'static str {
        "vortex-bench/sql/clickbench.md"
    }

    fn queries(&self) -> Result<Vec<(usize, String)>> {
        read_clickbench_queries(self.queries_file.as_deref())
    }

    async fn generate_base_data(&self) -> Result<()> {
        if self.data_url.scheme() != "file" {
            return Ok(());
        }

        let basepath = clickbench_flavor(self.flavor).to_data_path();
        self.flavor.download(basepath).await?;

        Ok(())
    }

    fn expected_row_counts(&self) -> Option<Vec<usize>> {
        Some(clickbench_expected_row_counts())
    }

    fn dataset(&self) -> BenchmarkDataset {
        BenchmarkDataset::ClickBench {
            flavor: self.flavor,
        }
    }

    fn dataset_name(&self) -> &str {
        "clickbench"
    }

    fn dataset_display(&self) -> String {
        format!("clickbench_{}", self.flavor)
    }

    fn data_url(&self) -> &Url {
        &self.data_url
    }

    fn table_specs(&self) -> Vec<TableSpec> {
        vec![TableSpec::new("hits", Some(HITS_SCHEMA.clone()))]
    }
}

#[async_trait::async_trait]
impl Benchmark for ClickBenchSortedBenchmark {
    fn doc_path(&self) -> &'static str {
        "vortex-bench/sql/clickbench.md#sorted-variant"
    }

    fn queries(&self) -> Result<Vec<(usize, String)>> {
        Ok(read_clickbench_queries(self.queries_file.as_deref())?
            .into_iter()
            .filter(|(idx, _)| CLICKBENCH_SORTED_QUERY_IDS.contains(idx))
            .collect())
    }

    async fn generate_base_data(&self) -> Result<()> {
        if self.data_url.scheme() != "file" {
            return Ok(());
        }

        generate_sorted_clickbench(CLICKBENCH_SORTED_NAME.to_data_path()).await
    }

    fn expected_row_counts(&self) -> Option<Vec<usize>> {
        Some(clickbench_expected_row_counts())
    }

    fn dataset(&self) -> BenchmarkDataset {
        BenchmarkDataset::ClickBenchSorted
    }

    fn dataset_name(&self) -> &str {
        CLICKBENCH_SORTED_NAME
    }

    fn dataset_display(&self) -> String {
        CLICKBENCH_SORTED_NAME.to_string()
    }

    fn data_url(&self) -> &Url {
        &self.data_url
    }

    fn table_specs(&self) -> Vec<TableSpec> {
        vec![TableSpec::new("hits", Some(HITS_SCHEMA.clone()))]
    }
}

fn clickbench_flavor(flavor: Flavor) -> String {
    format!("clickbench_{flavor}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const CORRECTNESS_QUERIES: &str = "sql/clickbench_correctness_queries.sql";

    #[test]
    fn correctness_query_file_preserves_all_query_indices() -> Result<()> {
        let canonical = read_clickbench_queries(None)?;
        let correctness_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(CORRECTNESS_QUERIES);
        let correctness = read_clickbench_queries(Some(&correctness_path.to_string_lossy()))?;

        assert_eq!(canonical.len(), 43);
        assert_eq!(correctness.len(), canonical.len());
        let differing_indices = canonical
            .iter()
            .zip(&correctness)
            .filter_map(|((idx, canonical_sql), (_, correctness_sql))| {
                (canonical_sql != correctness_sql).then_some(*idx)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            differing_indices,
            [17, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41]
        );
        for ((canonical_idx, canonical_sql), (correctness_idx, correctness_sql)) in
            canonical.iter().zip(&correctness)
        {
            assert_eq!(canonical_idx, correctness_idx);
            if !differing_indices.contains(canonical_idx) {
                assert_eq!(canonical_sql, correctness_sql);
            }
        }
        Ok(())
    }

    #[test]
    fn correctness_query_file_gives_q17_a_total_order() -> Result<()> {
        let correctness_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(CORRECTNESS_QUERIES);
        let correctness = read_clickbench_queries(Some(&correctness_path.to_string_lossy()))?;

        assert_eq!(
            correctness[17],
            (
                17,
                "SELECT \"UserID\", \"SearchPhrase\", COUNT(*) FROM hits GROUP BY \"UserID\", \
                 \"SearchPhrase\" ORDER BY \"UserID\", \"SearchPhrase\" LIMIT 10"
                    .to_string()
            )
        );
        Ok(())
    }

    #[test]
    fn correctness_query_file_gives_topk_queries_total_tie_breakers() -> Result<()> {
        let correctness_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(CORRECTNESS_QUERIES);
        let correctness = read_clickbench_queries(Some(&correctness_path.to_string_lossy()))?;

        let expected_order_suffixes = [
            (31, "ORDER BY c DESC, \"WatchID\", \"ClientIP\" LIMIT 10"),
            (32, "ORDER BY c DESC, \"WatchID\", \"ClientIP\" LIMIT 10"),
            (33, "ORDER BY c DESC, \"URL\" LIMIT 10"),
            (34, "ORDER BY c DESC, \"URL\" LIMIT 10"),
            (35, "ORDER BY c DESC, \"ClientIP\" LIMIT 10"),
            (36, "ORDER BY PageViews DESC, \"URL\" LIMIT 10"),
            (37, "ORDER BY PageViews DESC, \"Title\" LIMIT 10"),
            (38, "ORDER BY PageViews DESC, \"URL\" LIMIT 10 OFFSET 1000"),
            (
                39,
                "ORDER BY PageViews DESC, \"TraficSourceID\", \"SearchEngineID\", \
                 \"AdvEngineID\", Src, Dst LIMIT 10 OFFSET 1000",
            ),
            (
                40,
                "ORDER BY PageViews DESC, \"URLHash\", \"EventDate\" LIMIT 10 OFFSET 100",
            ),
            (
                41,
                "ORDER BY PageViews DESC, \"WindowClientWidth\", \"WindowClientHeight\" \
                 LIMIT 10 OFFSET 10000",
            ),
        ];
        for (query_idx, expected_suffix) in expected_order_suffixes {
            assert!(correctness[query_idx].1.contains(expected_suffix));
        }
        Ok(())
    }
}
