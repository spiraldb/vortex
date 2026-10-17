// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Lazy validity, native metadata, and final publication for Vortex row calls.

use ::rowfn::{BatchBinding, Operand, Selection, TypeBinding, ValiditySummary};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure};
use vortex_mask::Mask;

use super::VortexHost;
use crate::{ArrayRef, ExecutionCtx, IntoArray};
use crate::arrays::{ConstantArray, ExtensionArray};
use crate::builtins::ArrayBuiltins;
use crate::dtype::{DType, Nullability};
use crate::scalar::Scalar;
use crate::validity::Validity;

impl TypeBinding for VortexHost {
    fn nullable(dtype: &DType) -> bool { dtype.is_nullable() }
    fn with_nullable(dtype: &DType, nullable: bool) -> DType { dtype.with_nullability(Nullability::from(nullable)) }
    fn validate_label(storage: &DType, output: &DType) -> VortexResult<()> {
        if storage == output { return Ok(()); }
        if let DType::Extension(extension) = output {
            vortex_ensure!(extension.storage_dtype() == storage, "output extension must preserve storage type");
            return Ok(());
        }
        vortex_bail!("output label must preserve storage type, got {output}")
    }
}

/// Resolved Vortex validity, retaining bitmap storage and offsets.
pub struct VortexSelection(Mask);
// SAFETY: Mask retains its length and true count. Each traversal visits the immutable mask
// in increasing order and returns immediately if its callback fails.
unsafe impl Selection for VortexSelection {
    fn len(&self) -> usize { self.0.len() }
    fn count(&self) -> usize { self.0.true_count() }
    fn try_for_each<E>(&self, mut visit: impl FnMut(usize) -> Result<(), E>) -> Result<(), E> {
        match &self.0 {
            Mask::AllTrue(rows) => (0..*rows).try_for_each(visit),
            Mask::AllFalse(_) => Ok(()),
            Mask::Values(values) => values.bit_buffer().try_for_each_set_index(|index| visit(index)),
        }
    }
}

impl BatchBinding for VortexHost {
    type Validity = Validity;
    type Selection = VortexSelection;
    fn validate_operand(input: &Operand<Self>, rows: usize) -> VortexResult<()> {
        vortex_ensure!(input.column.dtype() == &input.dtype, "input metadata must match its column");
        vortex_ensure!(input.column.len() == rows || (input.scalar && input.column.len() == 1),
            "input must match logical batch length, got {}", input.column.len());
        if input.scalar {
            vortex_ensure!(super::super::row::constant_input(&input.column).is_some(),
                "a Vortex scalar operand must use a recognized constant encoding");
        }
        Ok(())
    }
    fn validity(inputs: &[Operand<Self>], rows: usize, ctx: &mut ExecutionCtx) -> VortexResult<Validity> {
        let mut validity = Validity::NonNullable;
        for input in inputs {
            let input_validity = if input.scalar && input.column.len() != rows {
                if input.column.all_valid(ctx)? { Validity::AllValid } else { Validity::AllInvalid }
            } else {
                input.column.validity()?
            };
            validity = validity.and(input_validity)?;
        }
        Ok(validity)
    }
    fn validity_summary(validity: &Validity) -> ValiditySummary {
        match validity {
            Validity::NonNullable | Validity::AllValid => ValiditySummary::All,
            Validity::AllInvalid => ValiditySummary::None,
            Validity::Array(_) => ValiditySummary::Unknown,
        }
    }
    fn selection(validity: &Validity, rows: usize, ctx: &mut ExecutionCtx) -> VortexResult<VortexSelection> {
        Ok(VortexSelection(validity.execute_mask(rows, ctx)?))
    }
    fn filter(input: &Operand<Self>, selection: &VortexSelection, _: &mut ExecutionCtx) -> VortexResult<Operand<Self>> {
        let column = if input.scalar && input.column.len() == 1 {
            input.column.clone()
        } else {
            input.column.filter(selection.0.clone())?
        };
        Ok(Operand { column, dtype: input.dtype.clone(), scalar: input.scalar })
    }
    fn all_null(dtype: &DType, rows: usize, _: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        Ok(ConstantArray::new(Scalar::null(dtype.clone()), rows).into_array())
    }
    fn broadcast(column: ArrayRef, rows: usize, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        vortex_ensure!(column.len() == 1 && column.all_valid(ctx)?, "scalar output must contain one valid row");
        Ok(ConstantArray::new(column.execute_scalar(0, ctx)?, rows).into_array())
    }
    fn publish(column: ArrayRef, storage: &DType, output: &DType, validity: &Validity,
        rows: usize, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        vortex_ensure!(column.len() == rows, "row output must match batch length, got {}", column.len());
        vortex_ensure!(column.dtype().with_nullability(Nullability::NonNullable) == *storage,
            "row output must match planned storage, got {}", column.dtype());
        vortex_ensure!(column.all_valid(ctx)?, "row callbacks cannot produce null from valid inputs");
        let mut column = match validity {
            Validity::Array(valid) => column.mask(valid.clone())?,
            Validity::AllInvalid if rows != 0 => return Self::all_null(output, rows, ctx),
            _ => column,
        };
        if let DType::Extension(extension) = output {
            if storage != &output.with_nullability(Nullability::NonNullable) {
                let extension = extension.with_nullability(column.dtype().nullability());
                column = ExtensionArray::try_new(extension, column)?.into_array();
            }
        }
        column.cast(output.clone())
    }
}
