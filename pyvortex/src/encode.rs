use arrow::array::{make_array, ArrayData as ArrowArrayData};
use arrow::datatypes::{DataType, Field};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatchReader;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::{FromArrowArray, FromArrowType};
use vortex::{ArrayData, IntoArray, IntoArrayData, ToArrayData};
use vortex_dtype::DType;

use crate::array::PyArray;
use crate::error::PyVortexError;
use crate::vortex_arrow::map_arrow_err;

/// The main entry point for creating enc arrays from other Python objects.
///
#[pyfunction]
pub fn encode(obj: &PyAny) -> PyResult<Py<PyArray>> {
    let pa = obj.py().import("pyarrow")?;
    let pa_array = pa.getattr("Array")?;
    let chunked_array = pa.getattr("ChunkedArray")?;
    let table = pa.getattr("Table")?;

    if obj.is_instance(pa_array)? {
        let arrow_array = ArrowArrayData::from_pyarrow(obj).map(make_array)?;
        let enc_array = ArrayData::from_arrow(arrow_array, false);
        PyArray::wrap(obj.py(), enc_array)
    } else if obj.is_instance(chunked_array)? {
        let chunks: Vec<&PyAny> = obj.getattr("chunks")?.extract()?;
        let encoded_chunks = chunks
            .iter()
            .map(|a| {
                ArrowArrayData::from_pyarrow(a)
                    .map(make_array)
                    .map(|a| ArrayData::from_arrow(a, false).into_array())
            })
            .collect::<PyResult<Vec<_>>>()?;
        let dtype: DType = obj
            .getattr("type")
            .and_then(DataType::from_pyarrow)
            .map(|dt| DType::from_arrow(&Field::new("_", dt, false)))?;
        PyArray::wrap(
            obj.py(),
            ChunkedArray::try_new(encoded_chunks, dtype)
                .map_err(PyVortexError::map_err)?
                .into_array_data(),
        )
    } else if obj.is_instance(table)? {
        let array_stream = ArrowArrayStreamReader::from_pyarrow(obj)?;
        let dtype = DType::from_arrow(array_stream.schema());
        let chunks = array_stream
            .into_iter()
            .map(|b| {
                b.map(|bb| bb.to_array_data().into_array())
                    .map_err(map_arrow_err)
            })
            .collect::<PyResult<Vec<_>>>()?;
        PyArray::wrap(
            obj.py(),
            ChunkedArray::try_new(chunks, dtype)
                .map_err(PyVortexError::map_err)?
                .into_array_data(),
        )
    } else {
        Err(PyValueError::new_err("Cannot convert object to enc array"))
    }
}
