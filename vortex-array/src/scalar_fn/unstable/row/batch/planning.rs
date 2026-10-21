// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Inspects borrowed columns before pairing collected inputs with a concrete execution plan.
//!
//! Input lengths, constants, and validity are properties of the batch. Planning derives the output
//! dtype and execution policy from the row signature selected by dispatch.

use smallvec::SmallVec;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;

use super::BatchInputs;
use super::BatchPlan;
use super::BatchProbe;
use super::RowFnExecutionArgs;
use super::RowInputs;
use super::args::BorrowedRowFnArgs;
use crate::ArrayRef;
use crate::arrays::Constant;
use crate::dtype::DType;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::unstable::row::types::is_batch_const;
use crate::validity::Validity;

impl BatchProbe {
    /// Inspect the borrowed inputs: check their row counts, classify batch constants, and conjoin
    /// their validity. Return the probe with the input dtypes.
    ///
    /// This **must not** be used for a nullary function. With no input columns, there is no
    /// validity to propagate and the all-constant check would pass vacuously.
    ///
    /// `id` names the function in the errors this raises. It is called only on an error.
    pub(crate) fn new<I: RowInputs + ?Sized>(
        id: impl Fn() -> ScalarFnId,
        inputs: &I,
    ) -> VortexResult<(Self, SmallVec<[DType; 4]>)> {
        let row_count = inputs.row_count();
        let num_inputs = inputs.num_inputs();
        vortex_ensure!(
            num_inputs <= u32::BITS as usize,
            "the {} function has {num_inputs} inputs, more than a row function supports",
            id(),
        );

        let mut arg_dtypes: SmallVec<[DType; 4]> = SmallVec::with_capacity(num_inputs);
        let mut validity = Validity::NonNullable;
        let mut const_mask = 0u32;
        let mut has_null_const = false;
        for index in 0..num_inputs {
            let input = inputs.input(index);
            vortex_ensure_eq!(
                input.len(),
                row_count,
                "the {} input {index} has the wrong row count",
                id(),
            );

            if is_batch_const(input) {
                const_mask |= 1 << index;
                has_null_const |= input
                    .as_opt::<Constant>()
                    .is_some_and(|constant| constant.scalar().is_null());
            }

            // A non-nullable input has no invalid row, and `NonNullable` is the identity of `and`.
            if input.dtype().is_nullable() {
                validity = validity.and(input.validity()?)?;
            }
            arg_dtypes.push(input.dtype().clone());
        }

        let probe = Self {
            row_count,
            validity,
            const_mask,
            has_null_const,
        };

        Ok((probe, arg_dtypes))
    }
}

impl BatchInputs {
    /// Collect the probed inputs of the function `id`.
    pub(crate) fn collect<I: RowInputs + ?Sized>(
        id: ScalarFnId,
        probe: BatchProbe,
        inputs: &I,
    ) -> Self {
        let BatchProbe {
            row_count,
            validity,
            const_mask,
            has_null_const,
        } = probe;

        Self {
            id,
            row_count,
            inputs: (0..inputs.num_inputs())
                .map(|index| inputs.input(index).clone())
                .collect(),
            validity,
            const_mask,
            has_null_const,
        }
    }

    /// Whether every input is batch-constant.
    pub(super) fn all_batch_const(&self) -> bool {
        self.const_mask.count_ones() as usize == self.inputs.len()
    }

    /// Attach the plan a dispatch selected for these inputs.
    pub(crate) fn with_plan<'a>(
        &'a self,
        arg_dtypes: &'a [DType],
        plan: BatchPlan,
    ) -> RowFnExecutionArgs<'a> {
        let result_dtype = plan.result_dtype(arg_dtypes);

        RowFnExecutionArgs {
            batch: self,
            arg_dtypes,
            result_dtype,
            plan,
        }
    }
}

impl RowFnExecutionArgs<'_> {
    /// Pair an input view with this batch's planning metadata.
    pub(super) fn execution_args<'b>(
        &'b self,
        arrays: &'b [ArrayRef],
        row_count: usize,
    ) -> BorrowedRowFnArgs<'b> {
        BorrowedRowFnArgs::new(arrays, row_count, self.arg_dtypes, &self.plan)
    }
}
