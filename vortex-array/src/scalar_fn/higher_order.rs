// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;

use crate::dtype::DType;
use crate::expr::BoundLambda;
use crate::scalar_fn::ScalarFnVTable;

/// The lambda hooks of a scalar function that takes lambda arguments.
///
/// A higher-order function implements this alongside [`ScalarFnVTable`] and returns itself from
/// [`ScalarFnVTable::higher_order`]. Binding an
/// [`Expression::HigherOrder`](crate::expr::Expression::HigherOrder) node asks the function for each
/// lambda's parameter dtypes, binds the lambdas, and stores them in the function's options. The
/// bound node is an ordinary scalar node whose children are the value arguments followed by every
/// lambda's captures.
pub trait HigherOrderVTable<V: ScalarFnVTable> {
    /// The parameter dtypes of the `lambda_index`th lambda, given the dtypes of the value
    /// arguments.
    ///
    /// `param_count` is the number of parameters the lambda declares, so a function can offer an
    /// optional parameter.
    fn lambda_params(
        &self,
        options: &V::Options,
        lambda_index: usize,
        param_count: usize,
        arg_dtypes: &[DType],
    ) -> VortexResult<Vec<DType>>;

    /// These options holding the bound lambdas, in argument order.
    fn with_lambdas(
        &self,
        options: &V::Options,
        lambdas: Vec<BoundLambda>,
    ) -> VortexResult<V::Options>;
}
