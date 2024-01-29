use paste::paste;
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
use enc::array::{ArrayKind, ArrayRef};

use crate::dtype::PyDType;
use crate::enc_arrow;

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

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: ArrayRef) -> PyResult<Py<Self>> {
        // This is the one place where we'd want to have owned kind enum but there's no other place this is used
        match inner.kind() {
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
            ArrayKind::Other(_) => panic!("Can't convert array to python"),
        }
    }
}

#[pymethods]
impl PyArray {
    fn to_pyarrow(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        enc_arrow::export_array_array(self_.py(), &self_.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn dtype(self_: PyRef<Self>) -> PyResult<Py<PyDType>> {
        PyDType::wrap(self_.py(), self_.inner.dtype().clone())
    }
}
