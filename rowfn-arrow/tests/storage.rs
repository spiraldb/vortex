// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Arrow-only ownership tests do not add any Vortex dependency to this package.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, StringViewArray};
use arrow_schema::{ArrowError, DataType, Field};
use rowfn::{BatchBinding, Operand, OutputBinding, OutputBuffer};
use rowfn::sink::ElementSink;
use rowfn::sink::{OutputSink, UninitElementSink, Utf8Output, WriteUtf8};
use rowfn_arrow::ArrowHost;

#[test]
fn primitive_publication_preserves_allocation_and_initialized_prefix() -> Result<(), ArrowError> {
    let mut buffer = <ArrowHost as OutputBinding<i64>>::allocate(4, &mut ())?;
    let pointer = buffer.slots().as_ptr().cast::<i64>();
    buffer.slots()[0].write(10);
    buffer.slots()[1].write(20);

    let mut moved = buffer;
    assert_eq!(moved.slots().as_ptr().cast::<i64>(), pointer);
    // SAFETY: only the first two slots are published, and both retain their initialized values.
    let result = unsafe { moved.finish(2) };
    let array = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(array.values().as_ptr(), pointer);
    assert_eq!(array.values().as_ref(), &[10, 20]);
    Ok(())
}

#[test]
fn partially_initialized_scalar_sink_can_be_abandoned() -> Result<(), ArrowError> {
    let mut sink = UninitElementSink::<ArrowHost, i64>::allocate(4, &(), &mut ())?;
    {
        let mut rows = sink.rows();
        // SAFETY: row zero is within the retained four-row view.
        let row = unsafe { UninitElementSink::<ArrowHost, i64>::row(&mut rows, 0) };
        row.write(10);
    }
    drop(sink);
    Ok(())
}

#[test]
fn initialized_scalar_sink_retains_defaults_and_writes_across_views() -> Result<(), ArrowError> {
    let mut sink = ElementSink::<ArrowHost, i64>::allocate(3, &(), &mut ())?;
    {
        let rows = sink.rows();
        assert_eq!(rows, &[0, 0, 0]);
        rows[1] = 42;
    }

    let mut moved = sink;
    assert_eq!(moved.rows(), &[0, 42, 0]);
    // SAFETY: every row remains initialized, including the rows left at their default value.
    let result = unsafe { moved.finish(&mut ()) }?;
    assert_eq!(
        result.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(),
        &[0, 42, 0],
    );
    Ok(())
}

#[test]
fn initialized_scalar_sink_can_publish_zero_rows() -> Result<(), ArrowError> {
    let mut sink = ElementSink::<ArrowHost, i64>::allocate(0, &(), &mut ())?;
    assert!(sink.rows().is_empty());
    // SAFETY: the empty initialized prefix contains no values to read.
    let result = unsafe { sink.finish(&mut ()) }?;
    assert_eq!(result.len(), 0);
    Ok(())
}

#[test]
fn string_parts_preserve_valid_utf8_and_owned_storage() -> Result<(), ArrowError> {
    type Sink = <ArrowHost as Utf8Output>::Sink;
    let owned = String::from("a much longer string allocated by the caller");
    let cases = [vec![], vec!["", "λ"], vec!["123456", "789012"],
        vec!["123456", "789012λ"], vec![owned.as_str(), " λ"]];
    let expected: Vec<_> = cases.iter().map(|parts| parts.concat()).collect();
    let mut sink = Sink::allocate(cases.len() + 1, &(), &mut ())?;
    {
        let mut rows = sink.rows();
        Sink::initialize(&mut rows);
        for (index, parts) in cases.iter().enumerate() {
            // SAFETY: the sink has one row per case plus one initialized, skipped row.
            unsafe { Sink::row(&mut rows, index) }.write_parts(parts);
        }
    }
    drop(cases);
    drop(owned);
    // SAFETY: every view started initialized, and each write retained valid output-owned bytes.
    let result = unsafe { sink.finish(&mut ()) }?;
    result.to_data().validate_full()?;
    let strings = result.as_any().downcast_ref::<StringViewArray>().unwrap();
    assert_eq!(strings.iter().collect::<Vec<_>>(), expected.iter().map(|s| Some(s.as_str()))
        .chain([Some("")]).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn unchecked_publication_rejects_invalid_labels_shapes_and_nullability() -> Result<(), ArrowError> {
    let storage = Field::new("result", DataType::Int64, false);
    let column = Arc::new(Int64Array::from(vec![1, 2]));
    let valid = ArrowHost::validity(&[], 2, &mut ())?;
    let invalid = Field::new("result", DataType::Utf8View, false);
    assert!(ArrowHost::publish(column.clone(), &storage, &invalid, &valid, 2, &mut ()).is_err());
    let input = Operand::<ArrowHost> {
        column: Arc::new(Int64Array::from(vec![None, Some(1)])),
        dtype: storage.clone().with_nullable(true), scalar: false,
    };
    let nullable = ArrowHost::validity(&[input.clone()], 2, &mut ())?;
    assert!(ArrowHost::publish(column.clone(), &storage, &storage, &nullable, 2, &mut ()).is_err());
    assert!(ArrowHost::validity(&[input], 1, &mut ()).is_err());
    assert!(ArrowHost::publish(Arc::new(Int64Array::from(vec![1])), &storage,
        &storage.clone().with_nullable(true), &nullable, 1, &mut ()).is_err());
    let output = ArrowHost::publish(column, &storage, &storage.clone().with_nullable(true),
        &nullable, 2, &mut ())?;
    output.to_data().validate_full()?;
    Ok(())
}

#[test]
fn scalar_broadcast_preserves_owned_views_and_boolean_values() -> Result<(), ArrowError> {
    let text = String::from("a long scalar string stored outside the header");
    let column = Arc::new(StringViewArray::from(vec![text.as_str()]));
    let result = ArrowHost::broadcast(column, 65, &mut ())?;
    drop(text);
    result.to_data().validate_full()?;
    let strings = result.as_any().downcast_ref::<StringViewArray>().unwrap();
    assert!(strings.iter().all(|value| value == Some("a long scalar string stored outside the header")));
    for value in [false, true] {
        let column = Arc::new(BooleanArray::from(vec![value]));
        let result = ArrowHost::broadcast(column, 65, &mut ())?;
        result.to_data().validate_full()?;
        assert!(result.as_any().downcast_ref::<BooleanArray>().unwrap().iter()
            .all(|item| item == Some(value)));
    }
    Ok(())
}
