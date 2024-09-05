#![allow(unsafe_op_in_unsafe_fn)]

use array::PyArray;
use expr::PyExpr;
use pyo3::prelude::*;

mod array;
mod compress;
mod dtype;
mod encode;
mod error;
mod expr;
mod io;
mod python_repr;

/// Vortex is an Apache Arrow-compatible toolkit for working with compressed array data.
#[pymodule]
fn _lib(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();

    let dtype = PyModule::new_bound(py, "dtype")?;
    m.add_submodule(&dtype)?;

    dtype.add_class::<dtype::PyDType>()?;
    dtype.add_function(wrap_pyfunction!(dtype::dtype_null, m)?)?;
    dtype.add_function(wrap_pyfunction!(dtype::dtype_bool, m)?)?;
    dtype.add_function(wrap_pyfunction!(dtype::dtype_int, m)?)?;
    dtype.add_function(wrap_pyfunction!(dtype::dtype_uint, m)?)?;
    dtype.add_function(wrap_pyfunction!(dtype::dtype_float, m)?)?;
    dtype.add_function(wrap_pyfunction!(dtype::dtype_utf8, m)?)?;
    dtype.add_function(wrap_pyfunction!(dtype::dtype_binary, m)?)?;

    let encoding = PyModule::new_bound(py, "encoding")?;
    m.add_submodule(&encoding)?;

    encoding.add_function(wrap_pyfunction!(encode::_encode, m)?)?;
    encoding.add_function(wrap_pyfunction!(compress::compress, m)?)?;

    encoding.add_class::<PyArray>()?;

    let io = PyModule::new_bound(py, "io")?;
    m.add_submodule(&io)?;

    io.add_function(wrap_pyfunction!(io::read, m)?)?;
    io.add_function(wrap_pyfunction!(io::write, m)?)?;

    let expr = PyModule::new_bound(py, "expr")?;
    m.add_submodule(&expr)?;

    expr.add_function(wrap_pyfunction!(expr::column, m)?)?;
    expr.add_class::<PyExpr>()?;

    Ok(())
}
