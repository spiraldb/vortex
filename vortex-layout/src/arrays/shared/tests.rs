// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayEq;
use vortex_array::Canonical;
use vortex_array::EqMode;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::DictArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::scalar_fn::fns::like::Like;
use vortex_array::scalar_fn::fns::like::LikeOptions;
use vortex_array::validity::Validity;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_onpair::DEFAULT_CONFIG;
use vortex_onpair::OnPair;
use vortex_onpair::build_token_frequency_index;
use vortex_onpair::onpair_compress;

use super::current_array_ref_for_dispatch;
use crate::arrays::shared::Shared;
use crate::arrays::shared::SharedArrayExt;

#[test]
fn shared_array_caches_on_canonicalize() -> VortexResult<()> {
    let array = PrimitiveArray::new(buffer![1i32, 2, 3], Validity::NonNullable).into_array();
    let shared = Shared::new(array);

    let session = vortex_array::array_session();
    let mut ctx = session.create_execution_ctx();

    let first = shared.get_or_compute(|source| source.clone().execute::<Canonical>(&mut ctx))?;

    // Second call should return cached without invoking the closure.
    let second = shared.get_or_compute(|_| panic!("should not execute twice"))?;

    assert!(first.array_eq(&second, EqMode::Value));

    Ok(())
}

#[test]
fn dispatch_does_not_bypass_cached_error() -> VortexResult<()> {
    let array = VarBinArray::from_iter(
        [Some("needle"), Some("other")],
        DType::Utf8(Nullability::NonNullable),
    )
    .into_array();
    let shared = Shared::new(array);

    assert!(
        shared
            .get_or_compute(|_| Err(vortex_err!("expected failure")))
            .is_err()
    );
    assert!(current_array_ref_for_dispatch(shared.as_view()).is_err());
    let pattern = ConstantArray::new("%needle%", shared.len()).into_array();
    let session = vortex_array::array_session();
    let result = Like::try_new(shared.into_array(), pattern, LikeOptions::default())?
        .into_array()
        .execute::<Canonical>(&mut session.create_execution_ctx());

    assert!(result.is_err_and(|error| error.to_string().contains("expected failure")));
    Ok(())
}

#[test]
fn nested_shared_like_completes() -> VortexResult<()> {
    let source = VarBinArray::from_iter(
        [Some("needle"), Some("other")],
        DType::Utf8(Nullability::NonNullable),
    )
    .into_array();
    let inner = Shared::new(source).into_array();
    let outer = Shared::new(inner).into_array();
    let pattern = ConstantArray::new("%needle%", outer.len()).into_array();

    let session = vortex_array::array_session();
    let mut ctx = session.create_execution_ctx();
    let result = Like::try_new(outer, pattern, LikeOptions::default())?
        .into_array()
        .execute::<Canonical>(&mut ctx)?
        .into_array();

    assert_arrays_eq!(result, BoolArray::from_iter([true, false]), &mut ctx);
    Ok(())
}

#[test]
fn dict_over_shared_onpair_executes_taken_rows() -> VortexResult<()> {
    let values: Vec<_> = (0..1_000)
        .map(|i| format!("https://www.example.com/items/{i:08}"))
        .collect();
    let session = vortex_array::array_session();
    vortex_onpair::initialize(&session);
    let mut ctx = session.create_execution_ctx();
    let input = VarBinViewArray::from_iter_str(&values).into_array();
    let encoded = onpair_compress(&input, DEFAULT_CONFIG, &mut ctx)?
        .try_downcast::<OnPair>()
        .map_err(|_| vortex_err!("expected OnPair"))?;
    let array = build_token_frequency_index(encoded, &mut ctx)?;
    let codes = PrimitiveArray::from_iter([999u16, 1, 500]).into_array();
    let shared = Shared::new(array.into_array());
    // SAFETY: all codes are within the shared dictionary.
    let dict = unsafe { DictArray::new_unchecked(codes, shared.clone().into_array()) }.into_array();
    let expected = VarBinViewArray::from_iter_str([
        values[999].as_str(),
        values[1].as_str(),
        values[500].as_str(),
    ]);
    assert_arrays_eq!(dict, expected, &mut ctx);
    assert!(shared.current_array_ref().is::<OnPair>());
    Ok(())
}
