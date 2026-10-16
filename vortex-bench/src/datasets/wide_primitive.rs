// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
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
use vortex::array::IntoArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::validity::Validity;
use vortex::dtype::FieldNames;

use crate::IdempotentPath;
use crate::datasets::Dataset;
use crate::idempotent_async;

/// A single-chunk table of `num_columns` non-nullable `i64` columns.
///
/// Each column holds a random base plus a small random offset, so it bit-packs well. The point
/// of the dataset is the column count: it measures the per-column cost of writing and reading a
/// very wide schema rather than the cost of decoding the values.
pub struct WidePrimitiveTable {
    num_columns: usize,
    row_count: usize,
    name: String,
}

impl WidePrimitiveTable {
    pub fn new(num_columns: usize, row_count: usize) -> Self {
        Self {
            num_columns,
            row_count,
            name: format!("wide i64 table cols={num_columns} rows={row_count}"),
        }
    }

    fn column_names(&self) -> impl Iterator<Item = String> {
        (0..self.num_columns).map(|col_idx| col_idx.to_string())
    }

    fn columns(&self) -> impl Iterator<Item = Vec<i64>> + '_ {
        let mut rng = StdRng::seed_from_u64(0);
        (0..self.num_columns).map(move |_| {
            let base = rng.random_range(-1_000_000..1_000_000);
            (0..self.row_count)
                .map(|_| base + rng.random_range(0..1024))
                .collect()
        })
    }
}

#[async_trait]
impl Dataset for WidePrimitiveTable {
    fn name(&self) -> &str {
        &self.name
    }

    async fn to_vortex_array(&self, _ctx: &mut ExecutionCtx) -> Result<ArrayRef> {
        let names: FieldNames = self.column_names().collect();
        let fields = self
            .columns()
            .map(|values| PrimitiveArray::from_iter(values).into_array())
            .collect();
        Ok(StructArray::try_new(names, fields, self.row_count, Validity::NonNullable)?.into_array())
    }

    async fn to_parquet_path(&self) -> Result<PathBuf> {
        let parquet_path = format!(
            "wide_primitive/wide_i64_cols{}_rows{}.parquet",
            self.num_columns, self.row_count
        )
        .to_data_path();

        idempotent_async(&parquet_path, |temp_path| async move {
            let schema = Arc::new(Schema::new(
                self.column_names()
                    .map(|name| Field::new(name, DataType::Int64, false))
                    .collect::<Vec<_>>(),
            ));
            let columns: Vec<ArrowArrayRef> = self
                .columns()
                .map(|values| Arc::new(Int64Array::from(values)) as ArrowArrayRef)
                .collect();
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

            let properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build();
            let mut writer = ArrowWriter::try_new(File::create(&temp_path)?, schema, Some(properties))?;
            writer.write(&batch)?;
            writer.close()?;

            Ok(())
        })
        .await
    }
}
