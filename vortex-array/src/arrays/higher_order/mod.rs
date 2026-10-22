// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! An array that applies a higher-order function to its children, with its lambdas bound.

mod rules;

use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use itertools::Itertools;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_panic;
use vortex_session::VortexSession;

use crate::ArrayEq;
use crate::ArrayHash;
use crate::ArrayRef;
use crate::ArraySlots;
use crate::EqMode;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::ProbeState;
use crate::array::Array;
use crate::array::ArrayId;
use crate::array::ArrayParts;
use crate::array::ArrayView;
use crate::array::OperationsVTable;
use crate::array::TypedArrayRef;
use crate::array::VTable;
use crate::array::ValidityVTable;
use crate::array::with_empty_buffers;
use crate::arrays::ConstantArray;
use crate::buffer::BufferHandle;
use crate::columnar::Columnar;
use crate::dtype::DType;
use crate::executor::ExecutionResult;
use crate::expr::BoundLambda;
use crate::higher_order_fn::HigherOrderFnRef;
use crate::scalar::Scalar;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::VecExecutionArgs;
use crate::serde::ArrayChildren;
use crate::validity::Validity;

/// An array that applies a higher-order function to its children.
pub type HigherOrderArray = Array<HigherOrder>;

/// The array vtable of [`HigherOrderArray`]. Its id is the function's.
#[derive(Clone, Debug)]
pub struct HigherOrder {
    id: ScalarFnId,
}

/// The function and its bound lambdas.
#[derive(Clone, Debug)]
pub struct HigherOrderData {
    function: HigherOrderFnRef,
    lambdas: Arc<Vec<BoundLambda>>,
}

impl HigherOrderData {
    /// The function.
    pub fn function(&self) -> &HigherOrderFnRef {
        &self.function
    }

    /// The bound lambdas.
    pub fn lambdas(&self) -> &[BoundLambda] {
        &self.lambdas
    }
}

impl Display for HigherOrderData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "higher_order_fn: {} {}",
            self.function,
            self.lambdas.iter().format(", ")
        )
    }
}

impl ArrayHash for HigherOrderData {
    fn array_hash<H: Hasher>(&self, state: &mut H, _accuracy: EqMode) {
        self.function.hash(state);
        self.lambdas.hash(state);
    }
}

impl ArrayEq for HigherOrderData {
    fn array_eq(&self, other: &Self, _accuracy: EqMode) -> bool {
        self.function == other.function && self.lambdas == other.lambdas
    }
}

impl Array<HigherOrder> {
    /// Apply a function, with its lambdas bound, to these children of `len` rows.
    pub fn try_new(
        function: HigherOrderFnRef,
        lambdas: Arc<Vec<BoundLambda>>,
        children: impl IntoIterator<Item = ArrayRef>,
        len: usize,
    ) -> VortexResult<Self> {
        let slots: ArraySlots = children.into_iter().map(Some).collect();
        vortex_ensure!(
            function.arity().matches(slots.len()),
            "{} requires {} children, got {}",
            function.id(),
            function.arity(),
            slots.len()
        );
        vortex_ensure!(
            slots.iter().flatten().all(|child| child.len() == len),
            "HigherOrderArray requires children of length {len}"
        );
        let arg_dtypes = slots
            .iter()
            .flatten()
            .map(|child| child.dtype().clone())
            .collect_vec();
        let dtype = function.return_dtype(&arg_dtypes, &lambdas)?;
        let vtable = HigherOrder { id: function.id() };
        let data = HigherOrderData { function, lambdas };
        Ok(
            unsafe {
                Array::from_parts_unchecked(ArrayParts::new(vtable, dtype, len, data, slots))
            },
        )
    }
}

/// Accessors for a [`HigherOrderArray`].
pub trait HigherOrderArrayExt: TypedArrayRef<HigherOrder> {
    /// The value children.
    fn children(&self) -> Vec<ArrayRef> {
        self.slots()
            .iter()
            .map(|slot| {
                slot.as_ref()
                    .vortex_expect("HigherOrderArray child slot")
                    .clone()
            })
            .collect()
    }
}

impl<T: TypedArrayRef<HigherOrder>> HigherOrderArrayExt for T {}

impl VTable for HigherOrder {
    type TypedArrayData = HigherOrderData;
    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        self.id
    }

    fn validate(
        &self,
        data: &HigherOrderData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        vortex_ensure_eq!(data.function.id(), self.id);
        vortex_ensure!(slots.iter().all(Option::is_some));
        vortex_ensure!(slots.iter().flatten().all(|child| child.len() == len));
        let arg_dtypes = slots
            .iter()
            .flatten()
            .map(|child| child.dtype().clone())
            .collect_vec();
        vortex_ensure_eq!(
            data.function.return_dtype(&arg_dtypes, &data.lambdas)?,
            *dtype
        );
        Ok(())
    }

    fn nbuffers(_array: ArrayView<'_, Self>) -> usize {
        0
    }

    fn buffer(_array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        vortex_panic!("HigherOrderArray buffer index {idx} out of bounds")
    }

    fn buffer_name(_array: ArrayView<'_, Self>, _idx: usize) -> Option<String> {
        None
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        with_empty_buffers(self, array, buffers)
    }

    fn serialize(
        _array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn deserialize(
        &self,
        _dtype: &DType,
        _len: usize,
        _metadata: &[u8],
        _buffers: &[BufferHandle],
        _children: &dyn ArrayChildren,
        _session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_bail!("HigherOrderArray is not serializable")
    }

    fn slot_name(array: ArrayView<'_, Self>, idx: usize) -> String {
        array.function.child_name(idx).as_ref().to_string()
    }

    fn execute(array: Array<Self>, ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        let args = VecExecutionArgs::new(array.children(), array.len());
        array
            .function
            .execute(&args, &array.lambdas, ctx)
            .map(ExecutionResult::done)
    }

    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        rules::PARENT_RULES.evaluate(array, parent, child_idx)
    }
}

impl OperationsVTable<HigherOrder> for HigherOrder {
    type ProbeState = ();

    fn probe_scalar(
        state: &mut ProbeState<'_, HigherOrder>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        Self::scalar_at(state.array(), index, ctx)
    }

    fn scalar_at(
        array: ArrayView<'_, HigherOrder>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let inputs: Vec<_> = array
            .children()
            .iter()
            .map(|child| Ok(ConstantArray::new(child.execute_scalar(index, ctx)?, 1).into_array()))
            .collect::<VortexResult<_>>()?;
        let result =
            array
                .function
                .execute(&VecExecutionArgs::new(inputs, 1), &array.lambdas, ctx)?;
        Ok(match result.execute::<Columnar>(ctx)? {
            Columnar::Canonical(canonical) => canonical.into_array().execute_scalar(0, ctx)?,
            Columnar::Constant(constant) => constant.scalar().clone(),
        })
    }
}

impl ValidityVTable<HigherOrder> for HigherOrder {
    fn validity(view: ArrayView<'_, HigherOrder>) -> VortexResult<Validity> {
        if view.function.is_strict() {
            // Valid where every child is.
            return view
                .children()
                .iter()
                .try_fold(Validity::from(view.dtype().nullability()), |acc, child| {
                    acc.and(child.validity()?)
                });
        }
        // Prototype: a function that isn't strict would need a lazy validity, like the one
        // scalar function arrays build.
        vortex_bail!(
            "validity of a non-strict {} is not supported yet",
            view.function.id()
        )
    }
}
