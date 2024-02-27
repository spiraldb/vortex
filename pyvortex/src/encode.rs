use arrow::array::{make_array, ArrayData};
use arrow::datatypes::DataType;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatchReader;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use vortex::array::chunked::ChunkedArray;
use vortex::array::{Array, ArrayRef};
use vortex::arrow::convert::TryIntoDType;
use vortex::dtype::DType;

use crate::array::PyArray;
use crate::vortex_arrow::map_arrow_err;
use crate::error::PyVortexError;

/// The main entry point for creating enc arrays from other Python objects.
///
#[pyfunction]
pub fn encode(obj: &PyAny) -> PyResult<Py<PyArray>> {
    let pa = obj.py().import("pyarrow")?;
    let pa_array = pa.getattr("Array")?;
    let chunked_array = pa.getattr("ChunkedArray")?;
    let table = pa.getattr("Table")?;

    if obj.is_instance(pa_array)? {
        let arrow_array = ArrayData::from_pyarrow(obj).map(make_array)?;
        let enc_array: vortex::array::ArrayRef = arrow_array.into();
        PyArray::wrap(obj.py(), enc_array)
    } else if obj.is_instance(chunked_array)? {
        let chunks: Vec<&PyAny> = obj.getattr("chunks")?.extract()?;
        let encoded_chunks = chunks
            .iter()
            .map(|a| {
                ArrayData::from_pyarrow(a)
                    .map(make_array)
                    .map(ArrayRef::from)
            })
            .collect::<PyResult<Vec<ArrayRef>>>()?;
        let null_count: usize = obj.getattr("null_count")?.extract()?;
        let dtype: DType = obj
            .getattr("type")
            .and_then(DataType::from_pyarrow)?
            .try_into_dtype(null_count > 0)
            .map_err(PyVortexError::map_err)?;
        PyArray::wrap(obj.py(), ChunkedArray::new(encoded_chunks, dtype).boxed())
    } else if obj.is_instance(table)? {
        let array_stream = ArrowArrayStreamReader::from_pyarrow(obj)?;
        let dtype = DType::try_from(array_stream.schema()).map_err(PyVortexError::map_err)?;
        let chunks = array_stream
            .into_iter()
            .map(|b| b.map(ArrayRef::from).map_err(map_arrow_err))
            .collect::<PyResult<Vec<ArrayRef>>>()?;
        PyArray::wrap(obj.py(), ChunkedArray::new(chunks, dtype).boxed())
    } else {
        Err(PyValueError::new_err("Cannot convert object to enc array"))
    }
}
