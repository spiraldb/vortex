use vortex_error::VortexResult;

use crate::{Array, IntoArrayVariant};

pub trait AndFn {
    fn and(&self, array: &Array) -> VortexResult<Array>;
}

pub trait OrFn {
    fn or(&self, array: &Array) -> VortexResult<Array>;
}

pub fn and(lhs: &Array, rhs: &Array) -> VortexResult<Array> {
    // We first want to check if either side implements `AndFn`
    if let Some(selection) = lhs.with_dyn(|lhs| lhs.and().map(|lhs| lhs.and(rhs))) {
        return selection;
    }

    if let Some(selection) = rhs.with_dyn(|rhs| rhs.and().map(|rhs| rhs.and(lhs))) {
        return selection;
    }

    // If neither side implements `AndFn`, we try to expand the left-hand side into a `BoolArray`, which we know does implement it, and call into that implementation.
    let lhs = lhs.clone().into_bool()?;
    lhs.and(rhs)
}

pub fn or(lhs: &Array, rhs: &Array) -> VortexResult<Array> {
    // We first want to check if either side implements `OrFn`
    if let Some(selection) = lhs.with_dyn(|lhs| lhs.or().map(|lhs| lhs.or(rhs))) {
        return selection;
    }

    if let Some(selection) = rhs.with_dyn(|rhs| rhs.or().map(|rhs| rhs.or(lhs))) {
        return selection;
    }

    // If neither side implements `Or`, we try to expand the left-hand side into a `BoolArray`, which we know does implement it, and call into that implementation.
    let lhs = lhs.clone().into_bool()?;
    lhs.or(rhs)
}
