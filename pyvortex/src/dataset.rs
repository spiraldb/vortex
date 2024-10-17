use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use pyo3::pyfunction;
use pyo3::types::PyString;
use tokio::fs::File;
use vortex::arrow::infer_schema;
use vortex::Array;
use vortex_dtype::field::Field;
use vortex_error::VortexResult;
use vortex_sampling_compressor::ALL_COMPRESSORS_CONTEXT;
use vortex_serde::layouts::{
    LayoutContext, LayoutDeserializer, LayoutReaderBuilder, Projection, RowFilter,
};

use crate::error::PyVortexError;
use crate::expr::PyExpr;
use crate::{io, PyArray};

#[pyclass(name = "Dataset", module = "io", sequence, subclass)]
/// An on-disk Vortex dataset for use with an Arrow-compatible query engine.
pub struct PyDataset {
    fname: String,
    schema: SchemaRef,
}

impl PyDataset {
    async fn new(fname: &str) -> VortexResult<PyDataset> {
        let file = File::open(Path::new(fname)).await?;
        let mut reader_builder = LayoutReaderBuilder::new(
            file,
            LayoutDeserializer::new(
                ALL_COMPRESSORS_CONTEXT.clone(),
                LayoutContext::default().into(),
            ),
        );

        let footer = reader_builder.read_footer().await?;
        let schema = infer_schema(&footer.dtype()?)?;

        Ok(PyDataset {
            fname: fname.to_string(),
            schema: Arc::new(schema),
        })
    }

    async fn read(
        &self,
        fname: &str,
        projection: Projection,
        batch_size: Option<usize>,
        row_filter: Option<RowFilter>,
    ) -> VortexResult<Array> {
        io::async_read(fname, projection, batch_size, row_filter).await
    }
}

#[pymethods]
impl PyDataset {
    pub fn fname(&self) -> &String {
        &self.fname
    }

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

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(self.read(self.fname(), projection, batch_size, row_filter))
            .map_err(PyVortexError::map_err)
            .map(PyArray::new)
    }
}

#[pyfunction]
pub fn dataset(fname: &Bound<'_, PyString>) -> PyResult<PyDataset> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(PyDataset::new(fname.to_str()?))
        .map_err(PyVortexError::map_err)
}
