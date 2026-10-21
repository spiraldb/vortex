// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Output normalization of scratch paths and file byte ranges.
//!
//! Tests reference a per-test working directory through the `${WORK_DIR}`
//! substitution variable; the runner sets `WORK_DIR` (via `Runner::set_var`) to
//! that directory (see [`crate::scratch`]). [`PathNormalizing`] rewrites the
//! directory back to the `${WORK_DIR}` token in query output. Normalizing at the
//! source keeps comparisons stable and makes `--complete` write the portable token
//! rather than a machine-specific path.
//!
//! DataFusion plans also show the byte range of each file split, such as
//! `data.vortex:0..2119578`. These ranges change with every change to the file
//! size, so [`PathNormalizing`] removes them too.

use std::process::Command;
use std::sync::LazyLock;
use std::time::Duration;

use async_trait::async_trait;
use regex::Regex;
use sqllogictest::DBOutput;
use sqllogictest::runner::AsyncDB;

/// The substitution variable tests use for their scratch directory: `${WORK_DIR}`.
pub const WORK_DIR_VAR: &str = "WORK_DIR";

/// The token that scratch paths are normalized to in query output. Matches the
/// `${WORK_DIR}` substitution variable so expected output reads naturally.
pub const WORK_DIR_TOKEN: &str = "${WORK_DIR}";

/// Replaces every occurrence of `work_dir` in `cell` with [`WORK_DIR_TOKEN`].
/// Also handles object-store paths, which omit the leading `/`.
pub fn normalize_work_dir(cell: &str, work_dir: &str) -> String {
    // Replace absolute paths first so their leading slash does not survive.
    let normalized = cell.replace(work_dir, WORK_DIR_TOKEN);
    if let Some(object_store_path) = work_dir.strip_prefix('/')
        && !object_store_path.is_empty()
    {
        return normalized.replace(object_store_path, WORK_DIR_TOKEN);
    }
    normalized
}

/// Matches the byte range after a data file in a DataFusion file group, such as
/// `:0..2119578` in `data.vortex:0..2119578`. The file extension is group 1.
#[expect(clippy::expect_used, reason = "the pattern is a constant")]
static BYTE_RANGE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\.(?:vortex|parquet)):\d+\.\.\d+").expect("byte range regex is valid")
});

/// Removes the byte ranges of file splits from `cell`.
pub fn strip_byte_ranges(cell: &str) -> String {
    BYTE_RANGE.replace_all(cell, "$1").into_owned()
}

/// Wraps an [`AsyncDB`] and rewrites this test's scratch path in query output back
/// to `${WORK_DIR}`. It also removes the byte ranges of file splits.
///
/// `--complete` writes raw query rows, so normalizing here (rather than only in the
/// comparison normalizer) is what keeps completed expected output stable.
pub struct PathNormalizing<D> {
    inner: D,
    work_dir: String,
}

impl<D> PathNormalizing<D> {
    /// Wraps `inner`, rewriting `work_dir` to `${WORK_DIR}` in its output.
    pub fn new(inner: D, work_dir: impl Into<String>) -> Self {
        Self {
            inner,
            work_dir: work_dir.into(),
        }
    }
}

#[async_trait]
impl<D: AsyncDB + Send> AsyncDB for PathNormalizing<D> {
    type Error = D::Error;
    type ColumnType = D::ColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        Ok(match self.inner.run(sql).await? {
            DBOutput::Rows { types, rows } => DBOutput::Rows {
                types,
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|cell| {
                                strip_byte_ranges(&normalize_work_dir(cell, &self.work_dir))
                            })
                            .collect()
                    })
                    .collect(),
            },
            other => other,
        })
    }

    async fn shutdown(&mut self) {
        self.inner.shutdown().await
    }

    fn engine_name(&self) -> &str {
        self.inner.engine_name()
    }

    async fn sleep(dur: Duration) {
        D::sleep(dur).await
    }

    async fn run_command(command: Command) -> std::io::Result<std::process::Output> {
        D::run_command(command).await
    }

    fn error_sql_state(err: &Self::Error) -> Option<String> {
        D::error_sql_state(err)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::normalize_work_dir;
    use super::strip_byte_ranges;

    #[rstest]
    #[case("no paths here", "no paths here")]
    #[case("plain 42", "plain 42")]
    #[case("${WORK_DIR}/data.vortex", "${WORK_DIR}/data.vortex")]
    fn leaves_unrelated_text_untouched(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(
            normalize_work_dir(input, "/repo/scratch/123/df_create.slt"),
            expected
        );
    }

    #[rstest]
    #[case::filesystem(
        "output_url=/repo/scratch/123/df_create.slt/sink/data1.vortex foo",
        "output_url=${WORK_DIR}/sink/data1.vortex foo"
    )]
    #[case::object_store(
        "file_groups={1 group: [[repo/scratch/123/df_create.slt/data.vortex]]}",
        "file_groups={1 group: [[${WORK_DIR}/data.vortex]]}"
    )]
    #[case::mixed(
        "TableScan: /repo/scratch/123/df_create.slt/data.vortex\nfile_groups={1 group: [[repo/scratch/123/df_create.slt/data.vortex]]}",
        "TableScan: ${WORK_DIR}/data.vortex\nfile_groups={1 group: [[${WORK_DIR}/data.vortex]]}"
    )]
    #[case::file_url(
        "file:///repo/scratch/123/df_create.slt/data.vortex",
        "file://${WORK_DIR}/data.vortex"
    )]
    fn rewrites_work_dir_to_token(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(
            normalize_work_dir(input, "/repo/scratch/123/df_create.slt"),
            expected
        );
    }

    #[rstest]
    #[case::vortex(
        "file_groups={2 groups: [[${WORK_DIR}/a.vortex:0..100], [${WORK_DIR}/a.vortex:100..200]]}",
        "file_groups={2 groups: [[${WORK_DIR}/a.vortex], [${WORK_DIR}/a.vortex]]}"
    )]
    #[case::parquet("[[data/a.parquet:0..42]]", "[[data/a.parquet]]")]
    #[case::other_range("RANGE 0..10 in a.vortex", "RANGE 0..10 in a.vortex")]
    fn strips_byte_ranges(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(strip_byte_ranges(input), expected);
    }
}
