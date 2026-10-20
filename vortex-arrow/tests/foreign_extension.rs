// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::tests_outside_test_module)]

//! Unknown Arrow extensions retain their identity while exposing their storage.

use std::sync::Arc;

use arrow_array::DictionaryArray;
use arrow_array::Int32Array;
use arrow_array::types::Int32Type;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY;
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::ExtensionArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::extension::ForeignExtDType;
use vortex_array::validity::Validity;
use vortex_arrow::ArrowSession;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

static BLOB_ID: CachedId = CachedId::new("spql.Blob");

fn foreign(storage: ArrayRef, metadata: &[u8]) -> VortexResult<ArrayRef> {
    Ok(ExtensionArray::try_new(
        ForeignExtDType::from_parts(*BLOB_ID, metadata.to_vec(), storage.dtype().clone())?,
        storage,
    )?
    .into_array())
}

fn list(elements: ArrayRef) -> VortexResult<ArrayRef> {
    Ok(ListArray::try_new(
        elements,
        buffer![0i32, 2, 3].into_array(),
        Validity::NonNullable,
        &mut array_session().create_execution_ctx(),
    )?
    .into_array())
}

#[rstest]
#[case::top_level("top")]
#[case::struct_child("struct")]
#[case::list_child("list")]
#[case::struct_storage("struct_storage")]
#[case::list_storage("list_storage")]
fn test_foreign_extension_roundtrip(
    #[case] placement: &str,
    #[values(false, true)] nullable: bool,
    #[values("", "{\"media_type\":\"image/🌀\"}\0")] metadata: &str,
) -> VortexResult<()> {
    let storage = if nullable {
        VarBinViewArray::from_iter_nullable_bin([Some(b"abc".as_slice()), None, Some(b"xyz")])
    } else {
        VarBinViewArray::from_iter_bin([b"abc", b"def", b"xyz"])
    }
    .into_array();
    let original = match placement {
        "struct_storage" => foreign(
            StructArray::from_fields(&[("blob", storage)])?.into_array(),
            metadata.as_bytes(),
        )?,
        "list_storage" => foreign(list(storage)?, metadata.as_bytes())?,
        _ => {
            let extension = foreign(storage, metadata.as_bytes())?;
            match placement {
                "struct" => StructArray::from_fields(&[("blob", extension)])?.into_array(),
                "list" => list(extension)?,
                _ => extension,
            }
        }
    };
    let session = array_session();
    assert!(!session.allows_unknown());
    let arrow_session = ArrowSession::default();
    let field = arrow_session.to_arrow_field("column", original.dtype())?;
    let extension_field = match placement {
        "struct" => match field.data_type() {
            DataType::Struct(fields) => fields[0].as_ref(),
            other => panic!("expected struct, got {other}"),
        },
        "list" => match field.data_type() {
            DataType::List(element) => element.as_ref(),
            other => panic!("expected list, got {other}"),
        },
        _ => &field,
    };
    assert_eq!(
        extension_field.metadata()[EXTENSION_TYPE_NAME_KEY],
        "spql.Blob"
    );
    assert_eq!(
        extension_field.metadata()[EXTENSION_TYPE_METADATA_KEY].as_bytes(),
        metadata.as_bytes()
    );
    assert_eq!(arrow_session.from_arrow_field(&field)?, *original.dtype());

    let mut ctx = session.create_execution_ctx();
    for target in [None, Some(&field)] {
        let arrow = arrow_session.execute_arrow(original.clone(), target, &mut ctx)?;
        assert_eq!(arrow.data_type(), field.data_type());
        let imported = arrow_session.from_arrow_array(arrow, &field)?;
        assert_eq!(imported.dtype(), original.dtype());
        assert_arrays_eq!(imported, original, &mut ctx);
    }
    Ok(())
}

#[test]
fn test_unknown_dictionary_extension_retains_identity() -> VortexResult<()> {
    let arrow_session = ArrowSession::default();
    let values = Arc::new(Int32Array::from(vec![42, 99]));
    let dictionary = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), None, Some(1), Some(0)]),
        values,
    )?);
    let field = Field::new(
        "blob",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
        true,
    )
    .with_metadata([(EXTENSION_TYPE_NAME_KEY.to_string(), "spql.Blob".to_string())].into());
    let imported = arrow_session.from_arrow_array(dictionary, &field)?;
    assert_eq!(imported.dtype(), &arrow_session.from_arrow_field(&field)?);
    let expected = foreign(
        PrimitiveArray::from_option_iter([Some(42i32), None, Some(99), Some(42)]).into_array(),
        b"",
    )?;
    assert_arrays_eq!(
        imported,
        expected,
        &mut array_session().create_execution_ctx()
    );
    Ok(())
}

#[test]
fn test_foreign_extension_rejects_non_utf8_metadata() -> VortexResult<()> {
    let array = foreign(buffer![1i32].into_array(), &[0xff])?;
    let error = ArrowSession::default()
        .to_arrow_field("blob", array.dtype())
        .expect_err("Arrow Rust cannot store non-UTF-8 field metadata");
    assert!(
        error
            .to_string()
            .contains("metadata cannot be represented as Arrow field metadata")
    );
    Ok(())
}
