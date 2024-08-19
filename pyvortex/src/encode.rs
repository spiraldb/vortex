use arrow::array::{make_array, ArrayData as ArrowArrayData};
use arrow::datatypes::{DataType, Field};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatchReader;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vortex::array::ChunkedArray;
use vortex::arrow::{FromArrowArray, FromArrowType};
use vortex::{Array, ToArrayData};
use vortex_dtype::DType;
use vortex_error::VortexError;

use crate::array::PyArray;
use crate::error::PyVortexError;
use crate::vortex_arrow::map_to_pyerr;

/// The main entry point for creating enc arrays from other Python objects.
///
#[pyfunction]
pub fn encode(obj: &Bound<PyAny>) -> PyResult<Py<PyArray>> {
    let pa = obj.py().import_bound("pyarrow")?;
    let pa_array = pa.getattr("Array")?;
    let chunked_array = pa.getattr("ChunkedArray")?;
    let table = pa.getattr("Table")?;

    if obj.is_instance(&pa_array)? {
        let arrow_array = ArrowArrayData::from_pyarrow_bound(obj).map(make_array)?;
        let enc_array = Array::from_arrow(arrow_array, false);
        PyArray::wrap(obj.py(), enc_array.into())
    } else if obj.is_instance(&chunked_array)? {
        let chunks: Vec<Bound<PyAny>> = obj.getattr("chunks")?.extract()?;
        let encoded_chunks = chunks
            .iter()
            .map(|a| {
                ArrowArrayData::from_pyarrow_bound(a)
                    .map(make_array)
                    .map(|a| Array::from_arrow(a, false))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let dtype: DType = obj
            .getattr("type")
            .and_then(|v| DataType::from_pyarrow_bound(&v))
            .map(|dt| DType::from_arrow(&Field::new("_", dt, false)))?;
        PyArray::wrap(
            obj.py(),
            ChunkedArray::try_new(encoded_chunks, dtype)
                .map_err(PyVortexError::map_err)?
                .to_array_data(),
        )
    } else if obj.is_instance(&table)? {
        let array_stream = ArrowArrayStreamReader::from_pyarrow_bound(obj)?;
        let dtype = DType::from_arrow(array_stream.schema());
        let chunks = array_stream
            .into_iter()
            .map(|b| b.map_err(VortexError::ArrowError))
            .map(|b| b.and_then(Array::try_from).map_err(map_to_pyerr))
            .collect::<PyResult<Vec<_>>>()?;
        PyArray::wrap(
            obj.py(),
            ChunkedArray::try_new(chunks, dtype)
                .map_err(PyVortexError::map_err)?
                .to_array_data(),
        )
    } else {
        Err(PyValueError::new_err("Cannot convert object to enc array"))
    }
}
