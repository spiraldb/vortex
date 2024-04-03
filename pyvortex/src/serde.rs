use std::io;
use std::io::{ErrorKind, Read, Write};

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::{ffi, pyfunction, FromPyPointer, IntoPy, Py, PyAny, PyResult, Python};
use vortex::serde::{ReadCtx, WriteCtx};

use crate::array::PyArray;
use crate::dtype::PyDType;

#[pyfunction]
pub fn read(py: Python<'_>, schema: &PyDType, read: &PyAny) -> PyResult<Py<PyArray>> {
    if !read.hasattr("readinto")? {
        return Err(PyTypeError::new_err(
            "reader has to support `readinto` method",
        ));
    }
    let read_no_gil: Py<PyAny> = read.into_py(py);
    let mut pyread = PyRead::new(read_no_gil);
    let mut ctx = ReadCtx::new(schema.unwrap(), &mut pyread);
    ctx.read()
        .map_err(|e| PyValueError::new_err(e.to_string()))
        .and_then(|arr| PyArray::wrap(py, arr))
}

struct PyRead {
    pyref: Py<PyAny>,
}

impl PyRead {
    pub fn new(pyref: Py<PyAny>) -> Self {
        Self { pyref }
    }
}

impl Read for PyRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Python::with_gil(|py| {
            let view = unsafe {
                let v = ffi::PyMemoryView_FromMemory(
                    buf.as_mut_ptr() as _,
                    buf.len() as ffi::Py_ssize_t,
                    ffi::PyBUF_WRITE,
                );
                PyAny::from_owned_ptr(py, v)
            };
            self.pyref
                .call_method(py, "readinto", (view,), None)
                .and_then(|v| v.extract(py))
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
        })
    }
}

#[pyfunction]
pub fn write(py: Python<'_>, arr: &PyArray, write: &PyAny) -> PyResult<()> {
    if !write.hasattr("write")? && !write.hasattr("flush")? {
        return Err(PyTypeError::new_err(
            "writer has to support `write` and `flush` methods",
        ));
    }
    let write_no_gil: Py<PyAny> = write.into_py(py);
    let mut pywrite = PyWrite::new(write_no_gil);
    let mut ctx = WriteCtx::new(&mut pywrite);
    ctx.write(arr.unwrap())
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

struct PyWrite {
    pyref: Py<PyAny>,
}

impl PyWrite {
    pub fn new(pyref: Py<PyAny>) -> Self {
        Self { pyref }
    }
}

impl Write for PyWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Python::with_gil(|py| {
            let view = unsafe {
                let v = ffi::PyMemoryView_FromMemory(
                    buf.as_ptr() as _,
                    buf.len() as ffi::Py_ssize_t,
                    ffi::PyBUF_READ,
                );
                PyAny::from_owned_ptr(py, v)
            };
            self.pyref
                .call_method(py, "write", (view,), None)
                .and_then(|v| v.extract(py))
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Python::with_gil(|py| {
            self.pyref
                .call_method0(py, "flush")
                .map(|_| ())
                .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))
        })
    }
}
