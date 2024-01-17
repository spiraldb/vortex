use pyo3::prelude::*;

use dtype::PyDType;

use crate::array::*;

mod array;
mod dtype;
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
    Ok(())
}
