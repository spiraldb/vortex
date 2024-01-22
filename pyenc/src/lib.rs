use pyo3::prelude::*;

use dtype::PyDType;
use enc::types::DType;

use crate::array::*;

mod array;
mod dtype;
mod enc_arrow;
mod encode;
mod error;

/// A Python module implemented in Rust.
#[pymodule]
fn _lib(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();

    m.add_function(wrap_pyfunction!(encode::encode, m)?)?;

    m.add_class::<PyArray>()?;
    m.add_class::<PyBoolArray>()?;
    m.add_class::<PyChunkedArray>()?;
    m.add_class::<PyConstantArray>()?;
    m.add_class::<PyPrimitiveArray>()?;
    m.add_class::<PyREEArray>()?;
    m.add_class::<PyStructArray>()?;
    m.add_class::<PyTypedArray>()?;
    m.add_class::<PyVarBinArray>()?;
    m.add_class::<PyVarBinViewArray>()?;

    m.add_class::<PyDType>()?;

    m.add_function(wrap_pyfunction!(dtype_int, m)?)?;
    m.add_function(wrap_pyfunction!(dtype_float, m)?)?;
    m.add_function(wrap_pyfunction!(dtype_bool, m)?)?;
    m.add_function(wrap_pyfunction!(dtype_utf8, m)?)?;

    Ok(())
}

#[pyfunction(name = "bool")]
#[pyo3(signature = (nullable = false))]
fn dtype_bool(py: Python<'_>, nullable: bool) -> PyResult<Py<PyDType>> {
    let mut dtype = DType::Bool;
    if nullable {
        dtype = DType::Nullable(Box::new(dtype));
    }
    PyDType::wrap(py, dtype)
}

#[pyfunction(name = "int")]
#[pyo3(signature = (width = None, signed = true, nullable = false))]
fn dtype_int(
    py: Python<'_>,
    width: Option<i8>,
    signed: bool,
    nullable: bool,
) -> PyResult<Py<PyDType>> {
    let mut dtype = DType::Int(width.unwrap_or(0).into(), signed.into());
    if nullable {
        dtype = DType::Nullable(Box::new(dtype));
    }
    PyDType::wrap(py, dtype)
}

#[pyfunction(name = "float")]
#[pyo3(signature = (width = None, nullable = false))]
fn dtype_float(py: Python<'_>, width: Option<i8>, nullable: bool) -> PyResult<Py<PyDType>> {
    let mut dtype = DType::Float(width.unwrap_or(0).into());
    if nullable {
        dtype = DType::Nullable(Box::new(dtype));
    }
    PyDType::wrap(py, dtype)
}

#[pyfunction(name = "utf8")]
#[pyo3(signature = (nullable = false))]
fn dtype_utf8(py: Python<'_>, nullable: bool) -> PyResult<Py<PyDType>> {
    let mut dtype = DType::Utf8;
    if nullable {
        dtype = DType::Nullable(Box::new(dtype));
    }
    PyDType::wrap(py, dtype)
}
