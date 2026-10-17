// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Concrete text views retain sliced storage and reject incompatible semantic mappings.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::LargeStringArray;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use rowfn::InputBinding;
use rowfn::RowKind;
use rowfn::RowView;
use rowfn::TextBinding;
use rowfn::TextLayout;
use rowfn::TextValue;
use rowfn_arrow::ArrowHost;

fn sliced_view<K>(column: ArrayRef) -> Result<(), ArrowError>
where
    ArrowHost: InputBinding<K>,
    K: for<'a> RowKind<Value<'a>: TextValue>,
{
    let column = column.slice(1, 3);
    let decoded = <ArrowHost as InputBinding<K>>::decode(&column, false, &mut ())?;
    drop(column);
    let view = <ArrowHost as InputBinding<K>>::view(&decoded);
    assert_eq!(view.len(), 3);

    for (index, expected) in ["é", "", "a long string outside a view"].into_iter().enumerate() {
        // SAFETY: this exact retained view has three rows, as checked above.
        let value = unsafe { view.get_unchecked(index) };
        assert_eq!(value.byte_len(), expected.len());
        assert_eq!(value.as_str(), expected);
    }

    Ok(())
}

#[test]
fn raw_text_views_retain_sliced_offsets_and_buffers() -> Result<(), ArrowError> {
    let values = vec![
        "discard",
        "é",
        "",
        "a long string outside a view",
        "discard",
    ];
    sliced_view::<<ArrowHost as TextBinding>::Offset32>(Arc::new(StringArray::from(values.clone())))?;
    sliced_view::<<ArrowHost as TextBinding>::Offset64>(Arc::new(LargeStringArray::from(values.clone())))?;
    sliced_view::<<ArrowHost as TextBinding>::Text>(Arc::new(StringViewArray::from(values)))
}

#[test]
fn layout_selection_rejects_unknown_extensions_and_binding_width_mismatches() -> Result<(), ArrowError> {
    let small = Field::new("text", DataType::Utf8, true);
    let large = Field::new("text", DataType::LargeUtf8, true);
    let view = Field::new("text", DataType::Utf8View, true);
    assert_eq!(ArrowHost::text_layout(&small)?, TextLayout::Offset32);
    assert_eq!(ArrowHost::text_layout(&large)?, TextLayout::Offset64);
    assert_eq!(ArrowHost::text_layout(&view)?, TextLayout::View);
    assert!(
        <ArrowHost as InputBinding<<ArrowHost as TextBinding>::Offset32>>::validate(&large).is_err()
    );
    assert!(
        <ArrowHost as InputBinding<<ArrowHost as TextBinding>::Offset64>>::validate(&small).is_err()
    );
    let extension = small.with_metadata(
        [("ARROW:extension:name".to_owned(), "unknown.text".to_owned())].into(),
    );
    assert!(ArrowHost::text_layout(&extension).is_err());

    Ok(())
}
