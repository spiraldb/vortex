use paste::paste;
use pyo3::prelude::*;

use enc::array::bool::BoolArray;
use enc::array::chunked::ChunkedArray;
use enc::array::constant::ConstantArray;
use enc::array::primitive::PrimitiveArray;
use enc::array::ree::REEArray;
use enc::array::struct_::StructArray;
use enc::array::typed::TypedArray;
use enc::array::varbin::VarBinArray;
use enc::array::varbinview::VarBinViewArray;
use enc::array::{Array, ArrayEncoding};

use crate::dtype::PyDType;
use crate::enc_arrow;

#[pyclass(name = "Array", module = "enc", sequence, subclass)]
pub struct PyArray {
    inner: Array,
}

macro_rules! pyarray {
    ($T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "enc", extends = PyArray, sequence, subclass)]
            pub struct [<Py $T>] {
                inner: $T,
            }

           impl [<Py $T>] {
               pub fn wrap(py: Python<'_>, inner: $T) -> PyResult<Py<Self>> {
                   let init = PyClassInitializer::from(PyArray { inner: inner.clone().into() })
                        .add_subclass([<Py $T>] { inner });
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
pyarray!(REEArray, "REEArray");
pyarray!(StructArray, "StructArray");
pyarray!(TypedArray, "TypedArray");
pyarray!(VarBinArray, "VarBinArray");
pyarray!(VarBinViewArray, "VarBinViewArray");

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: Array) -> PyResult<Py<Self>> {
        match inner {
            Array::Bool(a) => PyBoolArray::wrap(py, a)?.extract(py),
            Array::Chunked(a) => PyChunkedArray::wrap(py, a)?.extract(py),
            Array::Constant(a) => PyConstantArray::wrap(py, a)?.extract(py),
            Array::Primitive(a) => PyPrimitiveArray::wrap(py, a)?.extract(py),
            Array::REE(a) => PyREEArray::wrap(py, a)?.extract(py),
            Array::Struct(a) => PyStructArray::wrap(py, a)?.extract(py),
            Array::Typed(a) => PyTypedArray::wrap(py, a)?.extract(py),
            Array::VarBin(a) => PyVarBinArray::wrap(py, a)?.extract(py),
            Array::VarBinView(a) => PyVarBinViewArray::wrap(py, a)?.extract(py),
        }
    }
}

#[pymethods]
impl PyArray {
    fn to_pyarrow(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
        // NOTE(robert): Return RecordBatchStreamReader always?
        enc_arrow::export_array_array(self_.py(), &self_.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn dtype(self_: PyRef<Self>) -> PyResult<Py<PyDType>> {
        PyDType::wrap(self_.py(), self_.inner.dtype())
    }
}
