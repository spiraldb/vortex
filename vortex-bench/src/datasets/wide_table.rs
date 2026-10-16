// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::Float64Array;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use vortex::array::ArrayRef;
use vortex::array::ExecutionCtx;
use vortex_arrow::ArrowSessionExt;

use crate::IdempotentPath;
use crate::SESSION;
use crate::compress::read_projection;
use crate::datasets::Dataset;
use crate::idempotent_async;

/// Column types of a [`WideTable`].
#[derive(Clone, Copy, Debug)]
pub enum WideTableKind {
    /// Every column is a non-nullable `i64` that bit-packs.
    Int64,
    /// Columns cycle through bit-packable `i64`, random `f64`, low-cardinality strings and
    /// nullable `i32`, so each encoding path is exercised many times over.
    Mixed,
}

impl WideTableKind {
    fn name(self) -> &'static str {
        match self {
            WideTableKind::Int64 => "i64",
            WideTableKind::Mixed => "mixed",
        }
    }
}

/// A single-chunk table with a very large number of top-level columns.
///
/// The point of the dataset is the column count: it measures the per-column cost of writing and
/// reading a very wide schema rather than the cost of decoding the values. A table with
/// [`crate::compress::READ_PROJECTION_ROOT_COLUMNS`] columns is read back through the fixed
/// projection, and its name says so.
pub struct WideTable {
    kind: WideTableKind,
    num_columns: usize,
    row_count: usize,
    name: String,
}

impl WideTable {
    pub fn new(kind: WideTableKind, num_columns: usize, row_count: usize) -> Self {
        let mut name = format!(
            "wide {} table cols={num_columns} rows={row_count}",
            kind.name()
        );
        if let Some(projection) = read_projection(num_columns) {
            name.push_str(&format!(" project={}", projection.len()));
        }
        Self {
            kind,
            num_columns,
            row_count,
            name,
        }
    }

    fn record_batch(&self) -> Result<RecordBatch> {
        let mut rng = StdRng::seed_from_u64(0);
        let (fields, columns): (Vec<Field>, Vec<ArrowArrayRef>) = (0..self.num_columns)
            .map(|col_idx| {
                let name = col_idx.to_string();
                let kind = match self.kind {
                    WideTableKind::Int64 => 0,
                    WideTableKind::Mixed => col_idx % 4,
                };
                match kind {
                    0 => {
                        let base = rng.random_range(-1_000_000..1_000_000);
                        let values = (0..self.row_count).map(|_| base + rng.random_range(0..1024));
                        (
                            Field::new(name, DataType::Int64, false),
                            Arc::new(Int64Array::from_iter_values(values)) as ArrowArrayRef,
                        )
                    }
                    1 => {
                        let values = (0..self.row_count).map(|_| rng.random::<f64>());
                        (
                            Field::new(name, DataType::Float64, false),
                            Arc::new(Float64Array::from_iter_values(values)) as ArrowArrayRef,
                        )
                    }
                    2 => {
                        let values =
                            (0..self.row_count).map(|_| format!("v{}", rng.random_range(0..16)));
                        (
                            Field::new(name, DataType::Utf8, false),
                            Arc::new(StringArray::from_iter_values(values)) as ArrowArrayRef,
                        )
                    }
                    _ => {
                        let values = (0..self.row_count)
                            .map(|_| rng.random_bool(0.9).then(|| rng.random_range(0..100_000)));
                        (
                            Field::new(name, DataType::Int32, true),
                            Arc::new(Int32Array::from_iter(values)) as ArrowArrayRef,
                        )
                    }
                }
            })
            .unzip();

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }
}

#[async_trait]
impl Dataset for WideTable {
    fn name(&self) -> &str {
        &self.name
    }

    async fn to_vortex_array(&self, _ctx: &mut ExecutionCtx) -> Result<ArrayRef> {
        let batch = self.record_batch()?;
        let schema = batch.schema();
        Ok(SESSION.arrow().from_arrow_record_batch(batch, &schema)?)
    }

    async fn to_parquet_path(&self) -> Result<PathBuf> {
        let parquet_path = format!(
            "wide_table/wide_{}_cols{}_rows{}.parquet",
            self.kind.name(),
            self.num_columns,
            self.row_count
        )
        .to_data_path();

        idempotent_async(&parquet_path, |temp_path| async move {
            let batch = self.record_batch()?;
            let properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create(&temp_path)?, batch.schema(), Some(properties))?;
            writer.write(&batch)?;
            writer.close()?;

            Ok(())
        })
        .await
    }
}
