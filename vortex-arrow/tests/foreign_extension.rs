// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::tests_outside_test_module)]

//! Unknown Arrow extensions retain their identity while exposing their storage.

use std::sync::Arc;

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::DictionaryArray;
use arrow_array::Int32Array;
use arrow_array::types::Int32Type;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY;
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use arrow_schema::extension::ExtensionType;
use arrow_schema::extension::FixedShapeTensor as ArrowTensor;
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::ExtensionArray;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::extension::ExtensionArrayExt;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::DType;
use vortex_array::dtype::extension::ForeignExtDType;
use vortex_array::validity::Validity;
use vortex_arrow::ArrowExport;
use vortex_arrow::ArrowExportOptions;
use vortex_arrow::ArrowExportVTable;
use vortex_arrow::ArrowImport;
use vortex_arrow::ArrowImportVTable;
use vortex_arrow::ArrowSession;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_session::registry::CachedId;
use vortex_session::registry::Id;

static OPAQUE_ID: CachedId = CachedId::new("test.opaque");

const METADATA_ENCODING_KEY: &str = "vortex:extension:metadata:encoding";

fn foreign(storage: ArrayRef, metadata: &[u8]) -> VortexResult<ArrayRef> {
    Ok(ExtensionArray::try_new(
        ForeignExtDType::from_parts(*OPAQUE_ID, metadata.to_vec(), storage.dtype().clone())?,
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
    #[values(b"".as_slice(), "{\"label\":\"日本語\"}\0".as_bytes(), b"\0\xff\x80\xfe".as_slice())]
    metadata: &[u8],
) -> VortexResult<()> {
    let storage = if nullable {
        VarBinViewArray::from_iter_nullable_bin([Some(b"abc".as_slice()), None, Some(b"xyz")])
    } else {
        VarBinViewArray::from_iter_bin([b"abc", b"def", b"xyz"])
    }
    .into_array();
    let original = match placement {
        "struct_storage" => foreign(
            StructArray::from_fields(&[("opaque", storage)])?.into_array(),
            metadata,
        )?,
        "list_storage" => foreign(list(storage)?, metadata)?,
        _ => {
            let extension = foreign(storage, metadata)?;
            match placement {
                "struct" => StructArray::from_fields(&[("opaque", extension)])?.into_array(),
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
        "test.opaque"
    );
    assert_eq!(
        extension_field.metadata()[EXTENSION_TYPE_METADATA_KEY],
        BASE64_STANDARD.encode(metadata)
    );
    assert_eq!(extension_field.metadata()[METADATA_ENCODING_KEY], "base64");
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

#[rstest]
fn test_unknown_dictionary_extension_retains_identity(
    #[values(false, true)] encoded: bool,
) -> VortexResult<()> {
    let arrow_session = ArrowSession::default();
    let values = Arc::new(Int32Array::from(vec![42, 99]));
    let dictionary = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), None, Some(1), Some(0)]),
        values,
    )?);
    let mut field = Field::new(
        "opaque",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
        true,
    )
    .with_metadata(
        [(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            "test.opaque".to_string(),
        )]
        .into(),
    );
    let metadata: &[u8] = if encoded { &[0xff] } else { b"" };
    if encoded {
        field.metadata_mut().extend([
            (METADATA_ENCODING_KEY.to_string(), "base64".to_string()),
            (EXTENSION_TYPE_METADATA_KEY.to_string(), "/w==".to_string()),
        ]);
    }
    let imported = arrow_session.from_arrow_array(dictionary, &field)?;
    assert_eq!(imported.dtype(), &arrow_session.from_arrow_field(&field)?);
    let expected = foreign(
        PrimitiveArray::from_option_iter([Some(42i32), None, Some(99), Some(42)]).into_array(),
        metadata,
    )?;
    assert_arrays_eq!(
        imported,
        expected,
        &mut array_session().create_execution_ctx()
    );
    Ok(())
}

#[test]
fn test_default_metadata_uses_standard_padded_base64() -> VortexResult<()> {
    let metadata = (0..=255).collect::<Vec<u8>>();
    let original = foreign(buffer![1i32].into_array(), &metadata)?;
    let session = ArrowSession::default();
    let field = session.to_arrow_field("opaque", original.dtype())?;
    assert_eq!(field.metadata()[METADATA_ENCODING_KEY], "base64");
    let imported = session.from_arrow_field(&field)?;
    assert_eq!(&imported, original.dtype());

    let single_byte = foreign(buffer![1i32].into_array(), &[0xff])?;
    let field = session.to_arrow_field("opaque", single_byte.dtype())?;
    assert_eq!(field.metadata()[EXTENSION_TYPE_METADATA_KEY], "/w==");
    Ok(())
}

#[rstest]
#[case::json("{\"shape\":[1]}")]
#[case::base64_looking_text("/w==")]
fn test_unmarked_arrow_metadata_is_preserved(#[case] metadata: &str) -> VortexResult<()> {
    let session = ArrowSession::default();
    let field = Field::new("opaque", DataType::Int32, false).with_metadata(
        [
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "test.opaque".to_string(),
            ),
            (
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                metadata.to_string(),
            ),
        ]
        .into(),
    );
    let original = foreign(buffer![42i32].into_array(), metadata.as_bytes())?;
    assert_eq!(&session.from_arrow_field(&field)?, original.dtype());
    let imported = session.from_arrow_array(Arc::new(Int32Array::from(vec![42])), &field)?;
    assert_arrays_eq!(
        imported,
        original,
        &mut array_session().create_execution_ctx()
    );
    Ok(())
}

#[rstest]
#[case::invalid_base64("base64", "{\"shape\":[1]}", "Invalid base64 metadata")]
#[case::missing_padding("base64", "/w", "Invalid base64 metadata")]
#[case::unsupported_encoding("hex", "ff", "Unsupported metadata encoding")]
fn test_invalid_metadata_encoding_is_rejected(
    #[case] encoding: &str,
    #[case] metadata: &str,
    #[case] expected_error: &str,
) {
    let session = ArrowSession::default();
    let field = Field::new("opaque", DataType::Int32, false).with_metadata(
        [
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "test.opaque".to_string(),
            ),
            (
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                metadata.to_string(),
            ),
            (METADATA_ENCODING_KEY.to_string(), encoding.to_string()),
        ]
        .into(),
    );
    let error = session
        .from_arrow_field(&field)
        .expect_err("invalid metadata encoding");
    assert!(error.to_string().contains(expected_error));
    let error = session
        .from_arrow_array(Arc::new(Int32Array::from(vec![42])), &field)
        .expect_err("invalid metadata encoding");
    assert!(error.to_string().contains(expected_error));
}

/// A test plugin mapping opaque binary metadata to Arrow's canonical tensor JSON.
#[derive(Debug)]
struct TensorPlugin;

static ARROW_TENSOR_ID: CachedId = CachedId::new(ArrowTensor::NAME);

impl ArrowExportVTable for TensorPlugin {
    fn arrow_ext_id(&self) -> Id {
        *ARROW_TENSOR_ID
    }

    fn vortex_id(&self) -> Id {
        *OPAQUE_ID
    }

    fn to_arrow_field(
        &self,
        name: &str,
        dtype: &DType,
        session: &ArrowSession,
    ) -> VortexResult<Option<Field>> {
        let DType::Extension(dtype) = dtype else {
            return Ok(None);
        };
        let mut field = session.to_arrow_field(name, dtype.storage_dtype())?;
        field.try_with_extension_type(ArrowTensor::try_new(
            DataType::Int32,
            [1],
            Some(vec!["x".to_string()]),
            Some(vec![0]),
        )?)?;
        Ok(Some(field))
    }

    fn execute_arrow(
        &self,
        array: ArrayRef,
        target: &Field,
        options: &ArrowExportOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowExport> {
        let array = array.execute::<ExtensionArray>(ctx)?;
        ArrowSession::default()
            .exporter(options)
            .execute_arrow(array.storage_array().clone(), Some(target), ctx)
            .map(ArrowExport::Exported)
    }
}

impl ArrowImportVTable for TensorPlugin {
    fn arrow_ext_id(&self) -> Id {
        *ARROW_TENSOR_ID
    }

    fn from_arrow_field(
        &self,
        field: &Field,
        session: &ArrowSession,
    ) -> VortexResult<Option<DType>> {
        field.try_extension_type::<ArrowTensor>()?;
        Ok(Some(DType::Extension(ForeignExtDType::from_parts(
            *OPAQUE_ID,
            vec![0xff],
            session.from_arrow_datatype(field.data_type(), field.is_nullable().into())?,
        )?)))
    }

    fn from_arrow_array(
        &self,
        array: ArrowArrayRef,
        field: &Field,
        _dtype: &DType,
        session: &ArrowSession,
    ) -> VortexResult<ArrowImport> {
        foreign(
            session.from_arrow_array(array, field.is_nullable())?,
            &[0xff],
        )
        .map(ArrowImport::Imported)
    }
}

#[test]
fn test_arrow_plugin_overrides_default_metadata_encoding() -> VortexResult<()> {
    let session = ArrowSession::default();
    session.register_exporter(Arc::new(TensorPlugin));
    session.register_importer(Arc::new(TensorPlugin));
    let storage =
        FixedSizeListArray::try_new(buffer![42i32].into_array(), 1, Validity::NonNullable, 1)?
            .into_array();
    let original = foreign(storage, &[0xff])?;
    let field = session.to_arrow_field("tensor", original.dtype())?;
    assert_eq!(field.metadata()[EXTENSION_TYPE_NAME_KEY], ArrowTensor::NAME);
    assert!(field.metadata()[EXTENSION_TYPE_METADATA_KEY].starts_with('{'));
    assert!(!field.metadata().contains_key(METADATA_ENCODING_KEY));
    assert_eq!(&session.from_arrow_field(&field)?, original.dtype());
    let mut ctx = array_session().create_execution_ctx();
    let arrow = session.execute_arrow(original.clone(), Some(&field), &mut ctx)?;
    let imported = session.from_arrow_array(arrow, &field)?;
    assert_arrays_eq!(imported, original, &mut ctx);
    Ok(())
}
