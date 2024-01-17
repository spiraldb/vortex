use arrow2::datatypes::{DataType as ArrowDataType, Field};
use enc::array::binary::{VarBinArray, VarBinViewArray};
use enc::array::bool::BoolArray;
use enc::array::chunked::ChunkedArray;
use enc::array::constant::ConstantArray;
use enc::array::primitive::PrimitiveArray;
use enc::array::ree::REEArray;
use enc::array::struct_::StructArray;
use enc::array::typed::TypedArray;
use enc::array::{Array, ArrayEncoding};
use enc::types::DType;
use paste::paste;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};

use crate::dtype::PyDType;

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

        // Export the schema once
        let data_type: ArrowDataType = self_.inner.dtype().into();
        let field = Field::new(
            "array",
            data_type,
            matches!(self_.inner.dtype(), DType::Nullable(_)),
        );
        let schema_struct = arrow2::ffi::export_field_to_c(&field);

        // Import pyarrow and its Array class
        let mod_pyarrow = PyModule::import(self_.py(), "pyarrow")?;
        let cls_array = mod_pyarrow.getattr("Array")?;

        // Iterate each chunk, export it to Arrow FFI, then import as a pyarrow array
        let chunks: PyResult<Vec<&PyAny>> = self_
            .inner
            .iter_arrow()
            .map(|arrow_array| {
                let array_struct = arrow2::ffi::export_array_to_c(arrow_array);
                cls_array.call_method1(
                    "_import_from_c",
                    (
                        (&array_struct as *const arrow2::ffi::ArrowArray) as usize,
                        (&schema_struct as *const arrow2::ffi::ArrowSchema) as usize,
                    ),
                )
            })
            .collect();

        let dtype_array = mod_pyarrow.getattr("DataType")?;
        let dtype_struct = arrow2::ffi::export_field_to_c(&field);
        let pa_data_dtype = dtype_array.call_method1(
            "_import_from_c",
            ((&dtype_struct as *const arrow2::ffi::ArrowSchema) as usize,),
        )?;
        // Combine into a chunked array
        mod_pyarrow.call_method(
            "chunked_array",
            (PyList::new(self_.py(), chunks?),),
            Some([("type", pa_data_dtype)].into_py_dict(self_.py())),
        )
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn dtype(&self) -> PyDType {
        self.inner.dtype().into()
    }
}
