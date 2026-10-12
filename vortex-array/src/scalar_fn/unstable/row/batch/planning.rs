// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use smallvec::SmallVec;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;

use super::BatchPlan;
use super::RowFnExecutionArgs;
use super::args::BorrowedRowFnArgs;
use crate::ArrayRef;
use crate::arrays::Constant;
use crate::dtype::DType;
use crate::scalar_fn::ExecutionArgs;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::unstable::row::types::batch_const;
use crate::validity::Validity;

impl RowFnExecutionArgs {
    /// Collect the inputs and derive their dtypes, validity, and execution policy.
    ///
    /// `plan` receives the collected inputs and returns the batch plan. When
    /// [`PlanningInputs::always_dense`] holds, it can execute the dense kernel in the same dispatch
    /// that selects the plan.
    ///
    /// This constructor **must not** be used for a nullary function. With no input columns, there
    /// is no validity to propagate and the all-constant check would pass vacuously.
    pub(crate) fn new(
        id: ScalarFnId,
        args: &dyn ExecutionArgs,
        plan: impl FnOnce(PlanningInputs<'_>) -> VortexResult<BatchPlan>,
    ) -> VortexResult<Self> {
        let row_count = args.row_count();
        let num_inputs = args.num_inputs();
        let mut inputs: SmallVec<[ArrayRef; 4]> = SmallVec::with_capacity(num_inputs);
        let mut arg_dtypes: SmallVec<[DType; 4]> = SmallVec::with_capacity(num_inputs);
        for index in 0..num_inputs {
            let input = args.get(index)?;
            vortex_ensure_eq!(
                input.len(),
                row_count,
                "the {id} input {index} has the wrong row count",
            );
            arg_dtypes.push(input.dtype().clone());
            inputs.push(input);
        }

        let mut validity = Validity::NonNullable;
        for input in &inputs {
            validity = validity.and(input.validity()?)?;
        }

        let plan = plan(PlanningInputs {
            arrays: &inputs,
            dtypes: &arg_dtypes,
            row_count,
            validity: &validity,
        })?;
        let result_dtype = plan.result_dtype(&arg_dtypes);

        Ok(Self {
            id,
            row_count,
            inputs,
            arg_dtypes,
            validity,
            result_dtype,
            plan,
        })
    }

    /// Pair an input view with this batch's planning metadata.
    pub(super) fn execution_args<'b>(
        &'b self,
        arrays: &'b [ArrayRef],
        row_count: usize,
    ) -> BorrowedRowFnArgs<'b> {
        BorrowedRowFnArgs::new(arrays, row_count, &self.arg_dtypes, &self.plan)
    }
}

/// The collected inputs of a batch, offered to planning before the plan exists.
pub(crate) struct PlanningInputs<'a> {
    /// The input columns.
    arrays: &'a [ArrayRef],

    /// The input dtypes used to select the row implementation.
    dtypes: &'a [DType],

    /// The number of rows in the batch.
    row_count: usize,

    /// The conjoined input validity.
    validity: &'a Validity,
}

impl<'a> PlanningInputs<'a> {
    /// Return the input dtypes used to select the row implementation.
    pub(crate) fn dtypes(&self) -> &'a [DType] {
        self.dtypes
    }

    /// Whether batch execution runs the dense kernel over every row whatever the plan selects.
    ///
    /// This holds when the conjoined validity proves that no row is null and the batch is not an
    /// all-constant batch, which [`RowFnExecutionArgs::execute`] evaluates once and broadcasts.
    /// Planning can then execute the kernel in the same dispatch, and the result
    /// **must** be finished with [`RowFnExecutionArgs::finish_dense`].
    pub(crate) fn always_dense(&self) -> bool {
        self.validity.definitely_no_nulls()
            && !self.arrays.iter().any(is_null_constant)
            && !(self.row_count > 0 && self.arrays.iter().all(|input| batch_const(input).is_some()))
    }
}

impl ExecutionArgs for PlanningInputs<'_> {
    fn get(&self, index: usize) -> VortexResult<ArrayRef> {
        self.arrays.get(index).cloned().ok_or_else(|| {
            vortex_err!(
                "row-function input index must be less than {}, got {index}",
                self.arrays.len(),
            )
        })
    }

    fn num_inputs(&self) -> usize {
        self.arrays.len()
    }

    fn row_count(&self) -> usize {
        self.row_count
    }
}

/// Whether `input` is a literal null constant, which makes every row of the batch null.
pub(super) fn is_null_constant(input: &ArrayRef) -> bool {
    input
        .as_opt::<Constant>()
        .is_some_and(|constant| constant.scalar().is_null())
}
