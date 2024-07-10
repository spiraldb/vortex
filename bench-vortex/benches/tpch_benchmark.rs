use arrow_array::StructArray;
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::FromArrowArray;
use vortex::{Array, ArrayDType, ArrayData, IntoArray, IntoCanonical};
use vortex_dtype::{DType, Nullability};

/// Create a new TPC-H benchmark.

#[tokio::test]
async fn setup_tpch() {
    // First, load the data for TPC-H into vortex format
    let ctx = SessionContext::new();

    let batches = ctx
        .read_csv(
            "tpch/nation.tbl",
            CsvReadOptions::default()
                .file_extension("tbl")
                .has_header(false)
                .delimiter(b'|')
                .schema(&Schema::new(vec![
                    Field::new("n_nationkey", DataType::UInt64, false),
                    Field::new("n_name", DataType::Utf8, false),
                    Field::new("n_regionkey", DataType::UInt64, false),
                    Field::new("n_comment", DataType::Utf8, true),
                ])),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let arrays = batches
        .iter()
        .map(|batch| ArrayData::from_arrow(&StructArray::from(batch.clone()), false).into_array())
        .collect::<Vec<Array>>();

    let dtype = arrays[0].dtype().clone();
    let chunked = ChunkedArray::try_new(arrays, dtype).unwrap();

    // See what happens even without performing the operations instead.
    let vortex_struct = chunked.into_canonical().unwrap().into_struct().unwrap();
    assert_eq!(
        vortex_struct.dtypes()[1],
        DType::Utf8(Nullability::NonNullable)
    );
}
