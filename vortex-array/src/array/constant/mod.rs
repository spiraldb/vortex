use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use vortex_error::{vortex_panic, VortexResult};
use vortex_scalar::Scalar;

use crate::encoding::ids;
use crate::stats::{Stat, StatsSet};
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDef, ArrayTrait};

mod canonical;
mod compute;
mod stats;
mod variants;

impl_encoding!("vortex.constant", ids::CONSTANT, Constant);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantMetadata {
    scalar: Scalar,
    length: usize,
}

impl ConstantArray {
    pub fn new<S>(scalar: S, length: usize) -> Self
    where
        S: Into<Scalar>,
    {
        let scalar = scalar.into();
        // TODO(aduffy): add stats for bools, ideally there should be a
        //  StatsSet::constant(Scalar) constructor that does this for us, like StatsSet::nulls.
        let stats = StatsSet::from(HashMap::from([
            (Stat::Max, scalar.clone()),
            (Stat::Min, scalar.clone()),
            (Stat::IsConstant, true.into()),
            (Stat::IsSorted, true.into()),
            (Stat::RunCount, 1.into()),
        ]));

        Self::try_from_parts(
            scalar.dtype().clone(),
            length,
            ConstantMetadata {
                scalar: scalar.clone(),
                length,
            },
            [].into(),
            stats,
        )
        .unwrap_or_else(|err| {
            vortex_panic!(
                err,
                "Failed to create Constant array of length {} from scalar {}",
                length,
                scalar
            )
        })
    }

    pub fn scalar(&self) -> &Scalar {
        &self.metadata().scalar
    }
}

impl ArrayTrait for ConstantArray {}

impl ArrayValidity for ConstantArray {
    fn is_valid(&self, _index: usize) -> bool {
        match self.metadata().scalar.dtype().is_nullable() {
            true => self.scalar().is_valid(),
            false => true,
        }
    }

    fn logical_validity(&self) -> LogicalValidity {
        match self.scalar().is_null() {
            true => LogicalValidity::AllInvalid(self.len()),
            false => LogicalValidity::AllValid(self.len()),
        }
    }
}

impl AcceptArrayVisitor for ConstantArray {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        Ok(())
    }
}
