use std::sync::Arc;

use futures::StreamExt;
use vortex::array::{ChunkedArray, PrimitiveArray, StructArray, VarBinArray};
use vortex::{ArrayDType, Context, IntoArray, IntoArrayVariant};
use vortex_dtype::PType;

use crate::layout::reader::projections::Projection;
use crate::layout::reader::VortexLayoutReaderBuilder;
use crate::layout::writer::layout_writer::LayoutWriter;
use crate::layout::{LayoutContext, LayoutReader};

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_read_simple() {
    let strings = ChunkedArray::from_iter([
        VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
    ])
    .into_array();

    let numbers = ChunkedArray::from_iter([
        PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array(),
        PrimitiveArray::from(vec![5u32, 6, 7, 8]).into_array(),
    ])
    .into_array();

    let st = StructArray::from_fields(&[("strings", strings), ("numbers", numbers)]);
    let buf = Vec::new();
    let mut writer = LayoutWriter::new(buf);
    writer = writer.write_array_columns(st.into_array()).await.unwrap();
    let written = writer.finalize().await.unwrap();

    let layout_ctx = Arc::new(LayoutContext::default());
    let ctx = Arc::new(Context::default());
    let layout_serde = LayoutReader::new(ctx, layout_ctx);
    let mut stream = VortexLayoutReaderBuilder::new(written, layout_serde)
        .with_batch_size(5)
        .build()
        .await
        .unwrap();
    let mut batch_count = 0;
    let mut row_count = 0;

    while let Some(array) = stream.next().await {
        let array = array.unwrap();
        batch_count += 1;
        row_count += array.len();
    }

    assert_eq!(batch_count, 2);
    assert_eq!(row_count, 8);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_read_projection() {
    let strings = ChunkedArray::from_iter([
        VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
    ])
    .into_array();

    let numbers = ChunkedArray::from_iter([
        PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array(),
        PrimitiveArray::from(vec![5u32, 6, 7, 8]).into_array(),
    ])
    .into_array();

    let st = StructArray::from_fields(&[("strings", strings), ("numbers", numbers)]);
    let buf = Vec::new();
    let mut writer = LayoutWriter::new(buf);
    writer = writer.write_array_columns(st.into_array()).await.unwrap();
    let written = writer.finalize().await.unwrap();

    let layout_ctx = Arc::new(LayoutContext::default());
    let ctx = Arc::new(Context::default());
    let layout_serde = LayoutReader::new(ctx, layout_ctx);
    let mut stream = VortexLayoutReaderBuilder::new(written, layout_serde)
        .with_projection(Projection::new([0]))
        .with_batch_size(5)
        .build()
        .await
        .unwrap();
    let mut item_count = 0;
    let mut batch_count = 0;

    while let Some(array) = stream.next().await {
        let array = array.unwrap();
        item_count += array.len();
        batch_count += 1;

        let struct_dtype = array.dtype().as_struct().unwrap();
        assert_eq!(struct_dtype.dtypes().len(), 1);
        assert_eq!(struct_dtype.names()[0].as_ref(), "strings");
    }

    assert_eq!(item_count, 8);
    assert_eq!(batch_count, 2);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn unequal_batches() {
    let strings = ChunkedArray::from_iter([
        VarBinArray::from(vec!["ab", "foo", "bar", "bob"]).into_array(),
        VarBinArray::from(vec!["baz", "ab", "foo", "bar", "baz", "alice"]).into_array(),
    ])
    .into_array();

    let numbers = ChunkedArray::from_iter([
        PrimitiveArray::from(vec![1u32, 2, 3, 4, 5]).into_array(),
        PrimitiveArray::from(vec![6u32, 7, 8, 9, 10]).into_array(),
    ])
    .into_array();

    let st = StructArray::from_fields(&[("strings", strings), ("numbers", numbers)]);
    let buf = Vec::new();
    let mut writer = LayoutWriter::new(buf);
    writer = writer.write_array_columns(st.into_array()).await.unwrap();
    let written = writer.finalize().await.unwrap();

    let layout_ctx = Arc::new(LayoutContext::default());
    let ctx = Arc::new(Context::default());
    let layout_serde = LayoutReader::new(ctx, layout_ctx);
    let mut stream = VortexLayoutReaderBuilder::new(written, layout_serde)
        .with_batch_size(5)
        .build()
        .await
        .unwrap();
    let mut batch_count = 0;
    let mut item_count = 0;

    while let Some(array) = stream.next().await {
        let array = array.unwrap();
        item_count += array.len();
        batch_count += 1;

        let numbers = array.with_dyn(|a| a.as_struct_array_unchecked().field_by_name("numbers"));

        if let Some(numbers) = numbers {
            let numbers = numbers.into_primitive().unwrap();
            assert_eq!(numbers.ptype(), PType::U32);
        } else {
            panic!("Expected column doesn't exist")
        }
    }
    assert_eq!(item_count, 10);
    assert_eq!(batch_count, 2);
}
