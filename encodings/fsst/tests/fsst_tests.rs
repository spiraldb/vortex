#![cfg(test)]

use arrow_array::builder::BinaryBuilder;
use fsst::{Compressor, Symbol};
use vortex::array::{BoolArray, PrimitiveArray};
use vortex::arrow::FromArrowArray;
use vortex::compute::unary::scalar_at;
use vortex::compute::{filter, slice, take};
use vortex::validity::Validity;
use vortex::{ArrayDType, ArrayDef, IntoArray};
use vortex_dtype::{DType, Nullability, PType};
use vortex_fsst::{FSSTArray, FSST};

fn build_fsst_utf8_array() -> FSSTArray {
    let compressor = Compressor::train(
        "The Greeks never said that the limit could not he overstepped. \
        They said it existed and that whoever dared to exceed it was mercilessly struck down. \
        Nothing in present history can contradict them.",
    );

    let symbols = compressor.symbol_table();

    // SAFETY: Symbol and u64 have same size, enforced by compiler
    let symbols_u64 = unsafe { std::mem::transmute::<&[Symbol], &[u64]>(symbols) };
    let mut symbols_vec = Vec::new();
    symbols_vec.extend_from_slice(symbols_u64);

    let symbols_array = PrimitiveArray::from_vec(symbols_vec, Validity::NonNullable).into_array();
    assert_eq!(
        symbols_array.dtype(),
        &DType::Primitive(PType::U64, Nullability::NonNullable)
    );

    let mut codes = BinaryBuilder::new();

    codes.append_value(
        compressor
            .compress("The Greeks never said that the limit could not he overstepped".as_bytes()),
    );
    codes.append_value(
        compressor.compress(
            "They said it existed and that whoever dared to exceed it was mercilessly struck down"
                .as_bytes(),
        ),
    );
    codes.append_value(
        compressor.compress("Nothing in present history can contradict them".as_bytes()),
    );

    let codes = codes.finish();
    let codes_array = vortex::Array::from_arrow(&codes, false);

    FSSTArray::try_new(
        DType::Utf8(Nullability::NonNullable),
        symbols_array,
        codes_array,
    )
    .expect("building from parts must succeed")
}

macro_rules! assert_nth_scalar {
    ($arr:expr, $n:expr, $expected:expr) => {
        assert_eq!(scalar_at(&$arr, $n).unwrap(), $expected.try_into().unwrap());
    };
}

#[test]
fn test_compute() {
    let fsst_array = build_fsst_utf8_array().into_array();

    assert_eq!(fsst_array.len(), 3);

    //
    // ScalarAtFn
    //
    {
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
    }

    //
    // SliceFn
    //
    {
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
    }

    //
    // TakeFn
    //
    {
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
    }

    //
    // FilterFn
    //

    {
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
    }
}
