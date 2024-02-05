use paste::paste;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use enc::array::bool::BoolArray;
use enc::array::chunked::ChunkedArray;
use enc::array::constant::ConstantArray;
use enc::array::patched::PatchedArray;
use enc::array::primitive::PrimitiveArray;
use enc::array::ree::REEArray;
use enc::array::struct_::StructArray;
use enc::array::typed::TypedArray;
use enc::array::varbin::VarBinArray;
use enc::array::varbinview::VarBinViewArray;
use enc::array::{Array, ArrayKind, ArrayRef};
use enc_zigzag::{ZigZagArray, ZIGZAG_ENCODING};

use crate::dtype::PyDType;
use crate::enc_arrow;
use crate::error::PyEncError;

#[pyclass(name = "Array", module = "enc", sequence, subclass)]
pub struct PyArray {
    inner: ArrayRef,
}

macro_rules! pyarray {
    ($T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "enc", extends = PyArray, sequence, subclass)]
            pub struct [<Py $T>] {
                inner: $T,
            }

           impl [<Py $T>] {
               pub fn wrap(py: Python<'_>, inner: Box<$T>) -> PyResult<Py<Self>> {
                   let init = PyClassInitializer::from(PyArray { inner: inner.clone() as ArrayRef })
                        .add_subclass([<Py $T>] { inner: *inner });
                   Py::new(py, init)
               }

               pub fn unwrap(&self) -> &$T {
                   &self.inner
               }
           }
        }
    };
}

pyarray!(BoolArray, "BoolArray");
pyarray!(ChunkedArray, "ChunkedArray");
pyarray!(PatchedArray, "PatchedArray");
pyarray!(ConstantArray, "ConstantArray");
pyarray!(PrimitiveArray, "PrimitiveArray");
pyarray!(REEArray, "REEArray");
pyarray!(StructArray, "StructArray");
pyarray!(TypedArray, "TypedArray");
pyarray!(VarBinArray, "VarBinArray");
pyarray!(VarBinViewArray, "VarBinViewArray");
pyarray!(ZigZagArray, "ZigZagArray");

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: ArrayRef) -> PyResult<Py<Self>> {
        // This is the one place where we'd want to have owned kind enum but there's no other place this is used
        match ArrayKind::from(inner.as_ref()) {
            ArrayKind::Bool(_) => {
                PyBoolArray::wrap(py, inner.into_any().downcast::<BoolArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Chunked(_) => {
                PyChunkedArray::wrap(py, inner.into_any().downcast::<ChunkedArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Patched(_) => {
                PyPatchedArray::wrap(py, inner.into_any().downcast::<PatchedArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Constant(_) => {
                PyConstantArray::wrap(py, inner.into_any().downcast::<ConstantArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Primitive(_) => {
                PyPrimitiveArray::wrap(py, inner.into_any().downcast::<PrimitiveArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::REE(_) => {
                PyREEArray::wrap(py, inner.into_any().downcast::<REEArray>().unwrap())?.extract(py)
            }
            ArrayKind::Struct(_) => {
                PyStructArray::wrap(py, inner.into_any().downcast::<StructArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Typed(_) => {
                PyTypedArray::wrap(py, inner.into_any().downcast::<TypedArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::VarBin(_) => {
                PyVarBinArray::wrap(py, inner.into_any().downcast::<VarBinArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::VarBinView(_) => PyVarBinViewArray::wrap(
                py,
                inner.into_any().downcast::<VarBinViewArray>().unwrap(),
            )?
            .extract(py),
            ArrayKind::Other(other) => match other.encoding().id() {
                // PyEnc chooses to expose certain encodings as first-class objects.
                // For the remainder, we should have a generic EncArray implementation that supports basic functions.
                &ZIGZAG_ENCODING => {
                    PyZigZagArray::wrap(py, inner.into_any().downcast::<ZigZagArray>().unwrap())?
                        .extract(py)
                }
                _ => Err(PyValueError::new_err(format!(
                    "Cannot convert {:?} to enc array",
                    inner
                ))),
            },
        }
    }

    pub fn unwrap(&self) -> &dyn Array {
        self.inner.as_ref()
    }
}

#[pymethods]
impl PyArray {
    fn to_pyarrow(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        enc_arrow::export_array(self_.py(), &self_.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.inner.nbytes()
    }

    #[getter]
    fn dtype(self_: PyRef<Self>) -> PyResult<Py<PyDType>> {
        PyDType::wrap(self_.py(), self_.inner.dtype().clone())
    }
}

#[pymethods]
impl PyZigZagArray {
    #[staticmethod]
    fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        ZigZagArray::encode(array.unwrap())
            .map_err(PyEncError::map_err)
            .and_then(|zarray| PyArray::wrap(array.py(), zarray))
    }
}
