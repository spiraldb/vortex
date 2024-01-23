use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::varbin::varbin_stats;
use crate::array::varbinview::VarBinViewArray;
use crate::array::ArrayEncoding;
use crate::scalar::{BinaryScalar, Utf8Scalar};
use crate::types::DType;
use core::cmp::Ordering;
use std::collections::HashMap;

impl StatsCompute for VarBinViewArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        if self.len() == 0 {
            return StatsSet::new();
        }

        match self.dtype {
            DType::Utf8 => varbin_stats!(string_at, self),
            DType::Binary => varbin_stats!(binary_at, self),
            _ => panic!("Unexpected array dtype"),
        }
    }
}

// TODO(robert): These are duplicated from varbin since eventually these would be views over underlying arrays
fn string_at(arr: &VarBinViewArray, index: usize) -> String {
    arr.scalar_at(index)
        .unwrap()
        .into_any()
        .downcast::<Utf8Scalar>()
        .unwrap()
        .value()
        .to_string()
}

fn binary_at(arr: &VarBinViewArray, index: usize) -> Vec<u8> {
    arr.scalar_at(index)
        .unwrap()
        .into_any()
        .downcast::<BinaryScalar>()
        .unwrap()
        .value()
        .clone()
}
