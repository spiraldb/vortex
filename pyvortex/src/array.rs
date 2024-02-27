use paste::paste;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use vortex::array::bool::BoolArray;
use vortex::array::chunked::ChunkedArray;
use vortex::array::constant::ConstantArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::struct_::StructArray;
use vortex::array::typed::TypedArray;
use vortex::array::varbin::VarBinArray;
use vortex::array::varbinview::VarBinViewArray;
use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex_alp::{ALPArray, ALP_ENCODING};
use vortex_dict::{DictArray, DICT_ENCODING};
use vortex_ffor::{FFORArray, FFOR_ENCODING};
use vortex_ree::{REEArray, REE_ENCODING};
use vortex_roaring::{RoaringBoolArray, RoaringIntArray, ROARING_BOOL_ENCODING, ROARING_INT_ENCODING};
use vortex_zigzag::{ZigZagArray, ZIGZAG_ENCODING};

use crate::dtype::PyDType;
use crate::vortex_arrow;
use crate::error::PyVortexError;

#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
pub struct PyArray {
    inner: ArrayRef,
}

macro_rules! pyarray {
    ($T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "vortex", extends = PyArray, sequence, subclass)]
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
pyarray!(ConstantArray, "ConstantArray");
pyarray!(PrimitiveArray, "PrimitiveArray");
pyarray!(SparseArray, "SparseArray");
pyarray!(StructArray, "StructArray");
pyarray!(TypedArray, "TypedArray");
pyarray!(VarBinArray, "VarBinArray");
pyarray!(VarBinViewArray, "VarBinViewArray");

pyarray!(ALPArray, "ALPArray");
pyarray!(DictArray, "DictArray");
pyarray!(FFORArray, "FFORArray");
pyarray!(REEArray, "REEArray");
pyarray!(RoaringBoolArray, "RoaringBoolArray");
pyarray!(RoaringIntArray, "RoaringIntArray");
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
            ArrayKind::Constant(_) => {
                PyConstantArray::wrap(py, inner.into_any().downcast::<ConstantArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Primitive(_) => {
                PyPrimitiveArray::wrap(py, inner.into_any().downcast::<PrimitiveArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Sparse(_) => {
                PySparseArray::wrap(py, inner.into_any().downcast::<SparseArray>().unwrap())?
                    .extract(py)
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
            ArrayKind::Other(other) => match *other.encoding().id() {
                // PyEnc chooses to expose certain encodings as first-class objects.
                // For the remainder, we should have a generic EncArray implementation that supports basic functions.
                ALP_ENCODING => {
                    PyALPArray::wrap(py, inner.into_any().downcast::<ALPArray>().unwrap())?
                        .extract(py)
                }
                DICT_ENCODING => {
                    PyDictArray::wrap(py, inner.into_any().downcast::<DictArray>().unwrap())?
                        .extract(py)
                }
                FFOR_ENCODING => {
                    PyFFORArray::wrap(py, inner.into_any().downcast::<FFORArray>().unwrap())?
                        .extract(py)
                }
                REE_ENCODING => {
                    PyREEArray::wrap(py, inner.into_any().downcast::<REEArray>().unwrap())?
                        .extract(py)
                }
                ROARING_BOOL_ENCODING => PyRoaringBoolArray::wrap(
                    py,
                    inner.into_any().downcast::<RoaringBoolArray>().unwrap(),
                )?
                .extract(py),
                ROARING_INT_ENCODING => PyRoaringIntArray::wrap(
                    py,
                    inner.into_any().downcast::<RoaringIntArray>().unwrap(),
                )?
                .extract(py),
                ZIGZAG_ENCODING => {
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
        vortex_arrow::export_array(self_.py(), &self_.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    #[getter]
    fn encoding(&self) -> String {
        self.inner.encoding().id().to_string()
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
impl PyRoaringBoolArray {
    #[staticmethod]
    fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        RoaringBoolArray::encode(array.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|zarray| PyArray::wrap(array.py(), zarray.boxed()))
    }
}

#[pymethods]
impl PyRoaringIntArray {
    #[staticmethod]
    fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        RoaringIntArray::encode(array.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|zarray| PyArray::wrap(array.py(), zarray.boxed()))
    }
}

#[pymethods]
impl PyZigZagArray {
    #[staticmethod]
    fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        ZigZagArray::encode(array.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|zarray| PyArray::wrap(array.py(), zarray))
    }
}
