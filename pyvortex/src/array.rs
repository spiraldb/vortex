use arrow::array::{Array as ArrowArray, ArrayRef};
use arrow::pyarrow::ToPyArrow;
use paste::paste;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};
use vortex::array::{
    Bool, BoolArray, BoolEncoding, Chunked, ChunkedArray, ChunkedEncoding, Constant, ConstantArray,
    ConstantEncoding, Primitive, PrimitiveArray, PrimitiveEncoding, Sparse, SparseArray,
    SparseEncoding, Struct, StructArray, StructEncoding, VarBin, VarBinArray, VarBinEncoding,
    VarBinView, VarBinViewArray, VarBinViewEncoding,
};
use vortex::compute::take;
use vortex::encoding::EncodingRef;
use vortex::{Array, ArrayDType, ArrayData, ArrayDef, IntoCanonical, ToArray};
use vortex_alp::{ALPArray, ALPEncoding, ALP};
use vortex_dict::{Dict, DictArray, DictEncoding};
use vortex_fastlanes::{
    BitPacked, BitPackedArray, BitPackedEncoding, Delta, DeltaArray, DeltaEncoding, FoR, FoRArray,
    FoREncoding,
};
use vortex_roaring::{
    RoaringBool, RoaringBoolArray, RoaringBoolEncoding, RoaringInt, RoaringIntArray,
    RoaringIntEncoding,
};
use vortex_runend::{RunEnd, RunEndArray, RunEndEncoding};
use vortex_zigzag::{ZigZag, ZigZagArray, ZigZagEncoding};

use crate::dtype::PyDType;
use crate::error::PyVortexError;

#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
pub struct PyArray {
    inner: Array,
}

macro_rules! pyarray {
    ($E:ident, $T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "vortex", extends = PyArray, sequence, subclass)]
            pub struct [<Py $T>] {
                inner: $T,
                #[allow(dead_code)]
                encoding: EncodingRef,
            }

           impl [<Py $T>] {
               pub fn wrap(py: Python<'_>, inner: $T) -> PyResult<Py<Self>> {
                   let init = PyClassInitializer::from(PyArray { inner: inner.to_array().clone() })
                        .add_subclass([<Py $T>] { inner, encoding: &$E });
                   Py::new(py, init)
               }

               pub fn unwrap(&self) -> &$T {
                   &self.inner
               }
           }
        }
    };
}

pyarray!(BoolEncoding, BoolArray, "BoolArray");
pyarray!(ChunkedEncoding, ChunkedArray, "ChunkedArray");
pyarray!(ConstantEncoding, ConstantArray, "ConstantArray");
pyarray!(PrimitiveEncoding, PrimitiveArray, "PrimitiveArray");
pyarray!(SparseEncoding, SparseArray, "SparseArray");
pyarray!(StructEncoding, StructArray, "StructArray");
pyarray!(VarBinEncoding, VarBinArray, "VarBinArray");
pyarray!(VarBinViewEncoding, VarBinViewArray, "VarBinViewArray");

pyarray!(ALPEncoding, ALPArray, "ALPArray");
pyarray!(BitPackedEncoding, BitPackedArray, "BitPackedArray");
pyarray!(FoREncoding, FoRArray, "FoRArray");
pyarray!(DeltaEncoding, DeltaArray, "DeltaArray");
pyarray!(DictEncoding, DictArray, "DictArray");
pyarray!(RunEndEncoding, RunEndArray, "RunEndArray");
pyarray!(RoaringBoolEncoding, RoaringBoolArray, "RoaringBoolArray");
pyarray!(RoaringIntEncoding, RoaringIntArray, "RoaringIntArray");
pyarray!(ZigZagEncoding, ZigZagArray, "ZigZagArray");

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: ArrayData) -> PyResult<Py<Self>> {
        let encoding_id = inner.encoding().id();
        let array = Array::from(inner);
        // This is the one place where we'd want to have owned kind enum but there's no other place this is used
        match encoding_id {
            Bool::ID => PyBoolArray::wrap(
                py,
                BoolArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Chunked::ID => PyChunkedArray::wrap(
                py,
                ChunkedArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Constant::ID => PyConstantArray::wrap(
                py,
                ConstantArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Primitive::ID => PyPrimitiveArray::wrap(
                py,
                PrimitiveArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Sparse::ID => PySparseArray::wrap(
                py,
                SparseArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Struct::ID => PyStructArray::wrap(
                py,
                StructArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBin::ID => PyVarBinArray::wrap(
                py,
                VarBinArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBinView::ID => PyVarBinViewArray::wrap(
                py,
                VarBinViewArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Dict::ID => PyDictArray::wrap(
                py,
                DictArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RunEnd::ID => PyRunEndArray::wrap(
                py,
                RunEndArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Delta::ID => PyDeltaArray::wrap(
                py,
                DeltaArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            FoR::ID => PyFoRArray::wrap(
                py,
                FoRArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            BitPacked::ID => PyBitPackedArray::wrap(
                py,
                BitPackedArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),

            ALP::ID => PyALPArray::wrap(
                py,
                ALPArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RoaringBool::ID => PyBitPackedArray::wrap(
                py,
                BitPackedArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RoaringInt::ID => PyBitPackedArray::wrap(
                py,
                BitPackedArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            ZigZag::ID => PyZigZagArray::wrap(
                py,
                ZigZagArray::try_from(array).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            _ => Py::new(py, Self { inner: array }),
        }
    }

    pub fn unwrap(&self) -> &Array {
        &self.inner
    }
}

#[pymethods]
impl PyArray {
    fn to_arrow(self_: PyRef<'_, Self>) -> PyResult<Bound<PyAny>> {
        // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
        // NOTE(robert): Return RecordBatchStreamReader always?
        let py = self_.py();
        let vortex = &self_.inner;

        let chunks: Vec<ArrayRef> = if let Ok(chunked_array) = ChunkedArray::try_from(vortex) {
            chunked_array
                .chunks()
                .map(|chunk| -> PyResult<ArrayRef> {
                    Ok(chunk
                        .into_canonical()
                        .map_err(PyVortexError::map_err)?
                        .into_arrow())
                })
                .collect::<PyResult<Vec<ArrayRef>>>()?
        } else {
            vec![vortex
                .clone()
                .into_canonical()
                .map_err(PyVortexError::map_err)?
                .into_arrow()]
        };
        if chunks.is_empty() {
            return Err(PyValueError::new_err("No chunks in array"));
        }

        // Export the schema once
        let data_type = chunks[0].data_type().clone();
        let pa_data_type = data_type.to_pyarrow(py)?;

        // Iterate each chunk, export it to Arrow FFI, then import as a pyarrow array
        let chunks: PyResult<Vec<PyObject>> = chunks
            .iter()
            .map(|arrow_array| arrow_array.into_data().to_pyarrow(py))
            .collect();

        // Import pyarrow and its Array class
        let mod_pyarrow = PyModule::import_bound(py, "pyarrow")?;

        // Combine into a chunked array
        mod_pyarrow.call_method(
            "chunked_array",
            (PyList::new_bound(py, chunks?),),
            Some(&[("type", pa_data_type)].into_py_dict_bound(py)),
        )
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

    fn take(&self, indices: PyRef<'_, Self>) -> PyResult<Py<Self>> {
        take(&self.inner, indices.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|arr| Self::wrap(indices.py(), arr.into()))
    }
}
