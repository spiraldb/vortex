use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::pyarrow::{IntoPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::pyfunction;
use pyo3::types::PyString;
use tokio::fs::File;
use vortex::arrow::infer_schema;
use vortex_dtype::field::Field;
use vortex_error::VortexResult;
use vortex_serde::io::{ObjectStoreReadAt, VortexReadAt};
use vortex_serde::layouts::{Projection, RowFilter, VortexRecordBatchReader};

use crate::expr::PyExpr;
use crate::io::{layout_stream_from_reader, read_array, read_dtype, FileOrUrl};
use crate::{PyArray, TOKIO_RUNTIME};

#[pyclass(name = "Dataset", module = "io", sequence, subclass)]
/// An on-disk Vortex dataset for use with an Arrow-compatible query engine.
pub struct PyDataset {
    file_or_url: FileOrUrl,
    schema: SchemaRef,
}

impl PyDataset {
    async fn new(file_or_url: FileOrUrl) -> VortexResult<PyDataset> {
        let dtype = read_dtype(&file_or_url).await?;
        let schema = infer_schema(&dtype)?;

        Ok(PyDataset {
            file_or_url,
            schema: Arc::new(schema),
        })
    }
}

impl PyDataset {
    fn reader_to_pyarrow_record_batch_reader<T: VortexReadAt + Unpin + 'static>(
        self_: PyRef<Self>,
        reader: T,
        projection: Projection,
        batch_size: Option<usize>,
        row_filter: Option<RowFilter>,
    ) -> PyResult<PyObject> {
        let layout_reader = TOKIO_RUNTIME.block_on(layout_stream_from_reader(
            reader, projection, batch_size, row_filter,
        ))?;

        let record_batch_reader: Box<dyn RecordBatchReader + Send> =
            Box::new(VortexRecordBatchReader::new(layout_reader, &TOKIO_RUNTIME)?);

        record_batch_reader.into_pyarrow(self_.py())
    }
}

#[pymethods]
impl PyDataset {
    pub fn schema(self_: PyRef<Self>) -> PyResult<PyObject> {
        self_.schema.to_pyarrow(self_.py())
    }

    #[pyo3(signature = (columns, batch_size, row_filter))]
    pub fn to_array(
        &self,
        columns: Option<Vec<String>>,
        batch_size: Option<usize>,
        row_filter: Option<&Bound<PyExpr>>,
    ) -> PyResult<PyArray> {
        let projection = match columns {
            None => Projection::All,
            Some(columns) => {
                Projection::Flat(columns.into_iter().map(Field::Name).collect::<Vec<_>>())
            }
        };
        let row_filter = row_filter.map(|x| RowFilter::new(x.borrow().unwrap().clone()));
        let inner = TOKIO_RUNTIME.block_on(read_array(
            &self.file_or_url,
            projection,
            batch_size,
            row_filter,
        ))?;
        Ok(PyArray::new(inner))
    }

    #[pyo3(signature = (columns, batch_size, row_filter))]
    pub fn to_record_batch_reader(
        self_: PyRef<Self>,
        columns: Option<Vec<String>>,
        batch_size: Option<usize>,
        row_filter: Option<&Bound<PyExpr>>,
    ) -> PyResult<PyObject> {
        let projection = match columns {
            None => Projection::All,
            Some(columns) => {
                Projection::Flat(columns.into_iter().map(Field::Name).collect::<Vec<_>>())
            }
        };

        let row_filter = row_filter.map(|x| RowFilter::new(x.borrow().unwrap().clone()));

        match &self_.file_or_url {
            FileOrUrl::File(file) => {
                let reader = TOKIO_RUNTIME.block_on(File::open(Path::new(file)))?;
                Self::reader_to_pyarrow_record_batch_reader(
                    self_, reader, projection, batch_size, row_filter,
                )
            }
            FileOrUrl::Url(url) => {
                let reader = TOKIO_RUNTIME.block_on(ObjectStoreReadAt::try_new_from_url(url))?;
                Self::reader_to_pyarrow_record_batch_reader(
                    self_, reader, projection, batch_size, row_filter,
                )
            }
        }
    }
}

#[pyfunction]
#[pyo3(signature = (*, file = None, url = None))]
pub fn dataset(
    file: Option<&Bound<PyString>>,
    url: Option<&Bound<PyString>>,
) -> PyResult<PyDataset> {
    let file = file.map(|x| x.extract()).transpose()?;
    let url = url.map(|x| x.extract()).transpose()?;

    Ok(TOKIO_RUNTIME.block_on(PyDataset::new(FileOrUrl::try_new(file, url)?))?)
}
