use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef as ArrowArrayRef, PrimitiveArray as ArrowPrimitiveArray, RecordBatch,
    RecordBatchReader,
};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::ArrayRef;
use vortex::compute::take::take;
use vortex::ptype::PType;
use vortex::serde::ReadCtx;
use vortex_error::VortexResult;
use vortex_schema::DType;

pub fn take_vortex(path: &Path, indices: &[u64]) -> VortexResult<ArrayRef> {
    let chunked = {
        let mut file = File::open(path)?;
        let dummy_dtype: DType = PType::U8.into();
        let mut read_ctx = ReadCtx::new(&dummy_dtype, &mut file);
        let dtype = read_ctx.dtype()?;
        read_ctx.with_schema(&dtype).read()?
    };
    take(&chunked, &PrimitiveArray::from(indices.to_vec()))
}

pub fn take_arrow(path: &Path, indices: &[u64]) -> VortexResult<RecordBatch> {
    let file = File::open(path)?;

    // TODO(ngates): enable read_page_index
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // We figure out which row groups we need to read and a selection filter for each of them.
    let mut row_groups = HashMap::new();
    let mut row_group_offsets = vec![0];
    row_group_offsets.extend(
        builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .scan(0i64, |acc, x| {
                *acc += x;
                Some(*acc)
            }),
    );

    for idx in indices {
        let row_group_idx = row_group_offsets
            .binary_search(&(*idx as i64))
            .unwrap_or_else(|e| e - 1);
        if !row_groups.contains_key(&row_group_idx) {
            row_groups.insert(row_group_idx, Vec::new());
        }
        row_groups
            .get_mut(&row_group_idx)
            .unwrap()
            .push((*idx as i64) - row_group_offsets[row_group_idx]);
    }
    let row_group_indices = row_groups
        .keys()
        .sorted()
        .map(|i| row_groups.get(i).unwrap().clone())
        .collect_vec();

    let reader = builder
        .with_row_groups(row_groups.keys().copied().collect_vec())
        // FIXME(ngates): our indices code assumes the batch size == the row group sizes
        .with_batch_size(10_000_000)
        .build()
        .unwrap();

    let schema = reader.schema();

    let batches = reader
        .into_iter()
        .enumerate()
        .map(|(idx, batch)| {
            let batch = batch.unwrap();
            let indices = ArrowPrimitiveArray::<Int64Type>::from(row_group_indices[idx].clone());
            let indices_array: ArrowArrayRef = Arc::new(indices);
            take_record_batch(&batch, &indices_array).unwrap()
        })
        .collect_vec();

    Ok(concat_batches(&schema, &batches)?)
}
