#![cfg(test)]

use vortex::array::builder::VarBinBuilder;
use vortex::array::{BoolArray, PrimitiveArray};
use vortex::compute::unary::scalar_at;
use vortex::compute::{filter, slice, take};
use vortex::validity::Validity;
use vortex::{Array, ArrayDef, IntoArray, IntoCanonical};
use vortex_dtype::{DType, Nullability};
use vortex_fsst::{fsst_compress, fsst_train_compressor, FSST};

macro_rules! assert_nth_scalar {
    ($arr:expr, $n:expr, $expected:expr) => {
        assert_eq!(scalar_at(&$arr, $n).unwrap(), $expected.try_into().unwrap());
    };
}

// this function is VERY slow on miri, so we only want to run it once
fn build_fsst_array() -> Array {
    let mut input_array = VarBinBuilder::<i32>::with_capacity(3);
    input_array.push_value(b"The Greeks never said that the limit could not he overstepped");
    input_array.push_value(
        b"They said it existed and that whoever dared to exceed it was mercilessly struck down",
    );
    input_array.push_value(b"Nothing in present history can contradict them");
    let input_array = input_array
        .finish(DType::Utf8(Nullability::NonNullable))
        .into_array();

    let compressor = fsst_train_compressor(&input_array).unwrap();

    fsst_compress(&input_array, &compressor)
        .unwrap()
        .into_array()
}

#[test]
fn test_fsst_array_ops() {
    // first test the scalar_at values
    let fsst_array = build_fsst_array();
    assert_nth_scalar!(
        fsst_array,
        0,
        "The Greeks never said that the limit could not he overstepped"
    );
    assert_nth_scalar!(
        fsst_array,
        1,
        "They said it existed and that whoever dared to exceed it was mercilessly struck down"
    );
    assert_nth_scalar!(
        fsst_array,
        2,
        "Nothing in present history can contradict them"
    );

    // test slice
    let fsst_sliced = slice(&fsst_array, 1, 3).unwrap();
    assert_eq!(fsst_sliced.encoding().id(), FSST::ENCODING.id());
    assert_eq!(fsst_sliced.len(), 2);
    assert_nth_scalar!(
        fsst_sliced,
        0,
        "They said it existed and that whoever dared to exceed it was mercilessly struck down"
    );
    assert_nth_scalar!(
        fsst_sliced,
        1,
        "Nothing in present history can contradict them"
    );

    // test take
    let indices = PrimitiveArray::from_vec(vec![0, 2], Validity::NonNullable).into_array();
    let fsst_taken = take(&fsst_array, &indices).unwrap();
    assert_eq!(fsst_taken.len(), 2);
    assert_nth_scalar!(
        fsst_taken,
        0,
        "The Greeks never said that the limit could not he overstepped"
    );
    assert_nth_scalar!(
        fsst_taken,
        1,
        "Nothing in present history can contradict them"
    );

    // test filter
    let predicate =
        BoolArray::from_vec(vec![false, true, false], Validity::NonNullable).into_array();

    let fsst_filtered = filter(&fsst_array, &predicate).unwrap();
    assert_eq!(fsst_filtered.encoding().id(), FSST::ENCODING.id());
    assert_eq!(fsst_filtered.len(), 1);
    assert_nth_scalar!(
        fsst_filtered,
        0,
        "They said it existed and that whoever dared to exceed it was mercilessly struck down"
    );

    // test into_canonical
    let canonical_array = fsst_array
        .clone()
        .into_canonical()
        .unwrap()
        .into_varbin()
        .unwrap()
        .into_array();

    assert_eq!(canonical_array.len(), fsst_array.len());

    for i in 0..fsst_array.len() {
        assert_eq!(
            scalar_at(&fsst_array, i).unwrap(),
            scalar_at(&canonical_array, i).unwrap(),
        );
    }
}
