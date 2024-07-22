use vortex_error::VortexResult;

use crate::{Array, IntoArrayVariant};

pub trait AndFn {
    fn and(&self, array: &Array) -> VortexResult<Array>;
}

pub trait OrFn {
    fn or(&self, array: &Array) -> VortexResult<Array>;
}

pub fn and(lhs: &Array, rhs: &Array) -> VortexResult<Array> {
    if let Some(selection) = lhs.with_dyn(|lhs| lhs.and().map(|lhs| lhs.and(rhs))) {
        return selection;
    }

    if let Some(selection) = rhs.with_dyn(|rhs| rhs.and().map(|rhs| rhs.and(lhs))) {
        return selection;
    }

    let lhs = lhs.clone().into_bool()?;

    lhs.and(rhs)
}

pub fn or(lhs: &Array, rhs: &Array) -> VortexResult<Array> {
    if let Some(selection) = lhs.with_dyn(|lhs| lhs.or().map(|lhs| lhs.or(rhs))) {
        return selection;
    }

    if let Some(selection) = rhs.with_dyn(|rhs| rhs.or().map(|rhs| rhs.or(lhs))) {
        return selection;
    }

    let lhs = lhs.clone().into_bool()?;

    lhs.or(rhs)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::array::bool::BoolArray;
    use crate::compute::unary::scalar_at;
    use crate::IntoArray;

    #[rstest]
    #[case(BoolArray::from_iter([Some(true), Some(true), Some(false), Some(false)].into_iter())
    .into_array(), BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter())
    .into_array())]
    #[case(BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter()).into_array(), 
        BoolArray::from_iter([Some(true), Some(true), Some(false), Some(false)].into_iter()).into_array())]
    fn test_or(#[case] lhs: Array, #[case] rhs: Array) {
        let r = or(&lhs, &rhs).unwrap();

        let r = r.into_bool().unwrap().into_array();

        let v0 = scalar_at(&r, 0).unwrap().value().as_bool().unwrap();
        let v1 = scalar_at(&r, 1).unwrap().value().as_bool().unwrap();
        let v2 = scalar_at(&r, 2).unwrap().value().as_bool().unwrap();
        let v3 = scalar_at(&r, 3).unwrap().value().as_bool().unwrap();

        assert!(v0.unwrap());
        assert!(v1.unwrap());
        assert!(v2.unwrap());
        assert!(!v3.unwrap());
    }

    #[rstest]
    #[case(BoolArray::from_iter([Some(true), Some(true), Some(false), Some(false)].into_iter())
    .into_array(), BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter())
    .into_array())]
    #[case(BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter()).into_array(), 
        BoolArray::from_iter([Some(true), Some(true), Some(false), Some(false)].into_iter()).into_array())]
    fn test_and(#[case] lhs: Array, #[case] rhs: Array) {
        let r = and(&lhs, &rhs).unwrap().into_bool().unwrap().into_array();

        let v0 = scalar_at(&r, 0).unwrap().value().as_bool().unwrap();
        let v1 = scalar_at(&r, 1).unwrap().value().as_bool().unwrap();
        let v2 = scalar_at(&r, 2).unwrap().value().as_bool().unwrap();
        let v3 = scalar_at(&r, 3).unwrap().value().as_bool().unwrap();

        assert!(v0.unwrap());
        assert!(!v1.unwrap());
        assert!(!v2.unwrap());
        assert!(!v3.unwrap());
    }
}
