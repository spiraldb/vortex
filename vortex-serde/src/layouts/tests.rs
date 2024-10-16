#![allow(clippy::panic)]

use std::iter;
use std::sync::Arc;

use futures::StreamExt;
use vortex::accessor::ArrayAccessor;
use vortex::array::{ChunkedArray, PrimitiveArray, StructArray, VarBinArray};
use vortex::validity::Validity;
use vortex::{ArrayDType, IntoArray, IntoArrayVariant};
use vortex_dtype::field::Field;
use vortex_dtype::{DType, Nullability, PType};
use vortex_expr::{BinaryExpr, Column, Literal, Operator};

use crate::layouts::write::LayoutWriter;
use crate::layouts::{LayoutDeserializer, LayoutReaderBuilder, Projection, RowFilter};

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

    let mut stream = LayoutReaderBuilder::new(written, LayoutDeserializer::default())
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

    let mut stream = LayoutReaderBuilder::new(written, LayoutDeserializer::default())
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

    let mut stream = LayoutReaderBuilder::new(written, LayoutDeserializer::default())
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

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn write_chunked() {
    let strings = VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array();
    let string_dtype = strings.dtype().clone();
    let strings_chunked =
        ChunkedArray::try_new(iter::repeat(strings).take(4).collect(), string_dtype)
            .unwrap()
            .into_array();
    let numbers = PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array();
    let numbers_dtype = numbers.dtype().clone();
    let numbers_chunked =
        ChunkedArray::try_new(iter::repeat(numbers).take(4).collect(), numbers_dtype)
            .unwrap()
            .into_array();
    let st = StructArray::try_new(
        ["strings".into(), "numbers".into()].into(),
        vec![strings_chunked, numbers_chunked],
        16,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let st_dtype = st.dtype().clone();

    let chunked_st = ChunkedArray::try_new(iter::repeat(st).take(3).collect(), st_dtype)
        .unwrap()
        .into_array();
    let buf = Vec::new();
    let mut writer = LayoutWriter::new(buf);
    writer = writer.write_array_columns(chunked_st).await.unwrap();
    let written = writer.finalize().await.unwrap();
    let mut reader = LayoutReaderBuilder::new(written, LayoutDeserializer::default())
        .build()
        .await
        .unwrap();
    let mut array_len: usize = 0;
    while let Some(array) = reader.next().await {
        array_len += array.unwrap().len();
    }
    assert_eq!(array_len, 48);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn filter_string() {
    let names_orig = VarBinArray::from_iter(
        vec![Some("Joseph"), None, Some("Angela"), Some("Mikhail"), None],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let ages_orig =
        PrimitiveArray::from_nullable_vec(vec![Some(25), Some(31), None, Some(57), None])
            .into_array();
    let st = StructArray::try_new(
        ["name".into(), "age".into()].into(),
        vec![names_orig, ages_orig],
        5,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut writer = LayoutWriter::new(Vec::new());
    writer = writer.write_array_columns(st).await.unwrap();
    let written = writer.finalize().await.unwrap();
    let mut reader = LayoutReaderBuilder::new(written, LayoutDeserializer::default())
        .with_row_filter(RowFilter::new(Arc::new(BinaryExpr::new(
            Arc::new(Column::new(Field::from("name"))),
            Operator::Eq,
            Arc::new(Literal::new("Joseph".into())),
        ))))
        .build()
        .await
        .unwrap();

    let mut result = Vec::new();
    while let Some(array) = reader.next().await {
        result.push(array.unwrap());
    }
    assert_eq!(result.len(), 1);
    let names = result[0]
        .with_dyn(|a| a.as_struct_array_unchecked().field(0))
        .unwrap();
    assert_eq!(
        names
            .into_varbin()
            .unwrap()
            .with_iterator(|iter| iter
                .flatten()
                .map(|s| unsafe { String::from_utf8_unchecked(s.to_vec()) })
                .collect::<Vec<_>>())
            .unwrap(),
        vec!["Joseph".to_string()]
    );
    let ages = result[0]
        .with_dyn(|a| a.as_struct_array_unchecked().field(1))
        .unwrap();
    assert_eq!(
        ages.into_primitive().unwrap().maybe_null_slice::<i32>(),
        vec![25]
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn filter_or() {
    let names = VarBinArray::from_iter(
        vec![Some("Joseph"), None, Some("Angela"), Some("Mikhail"), None],
        DType::Utf8(Nullability::Nullable),
    );
    let ages = PrimitiveArray::from_nullable_vec(vec![Some(25), Some(31), None, Some(57), None]);
    let st = StructArray::try_new(
        ["name".into(), "age".into()].into(),
        vec![names.clone().into_array(), ages.clone().into_array()],
        5,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut writer = LayoutWriter::new(Vec::new());
    writer = writer.write_array_columns(st).await.unwrap();
    let written = writer.finalize().await.unwrap();
    let mut reader = LayoutReaderBuilder::new(written, LayoutDeserializer::default())
        .with_row_filter(RowFilter::new(Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(Field::from("name"))),
                Operator::Eq,
                Arc::new(Literal::new("Angela".into())),
            )),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("age"))),
                    Operator::Gte,
                    Arc::new(Literal::new(20.into())),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("age"))),
                    Operator::Lte,
                    Arc::new(Literal::new(30.into())),
                )),
            )),
        ))))
        .build()
        .await
        .unwrap();

    let mut result = Vec::new();
    while let Some(array) = reader.next().await {
        result.push(array.unwrap());
    }
    assert_eq!(result.len(), 1);
    let names = result[0]
        .with_dyn(|a| a.as_struct_array_unchecked().field(0))
        .unwrap();
    assert_eq!(
        names
            .into_varbin()
            .unwrap()
            .with_iterator(|iter| iter
                .flatten()
                .map(|s| unsafe { String::from_utf8_unchecked(s.to_vec()) })
                .collect::<Vec<_>>())
            .unwrap(),
        vec!["Joseph".to_string()]
    );
    let ages = result[0]
        .with_dyn(|a| a.as_struct_array_unchecked().field(1))
        .unwrap();
    assert_eq!(
        ages.into_primitive().unwrap().maybe_null_slice::<i32>(),
        vec![25]
    );
}
