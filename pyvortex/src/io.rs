use std::path::Path;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::pyfunction;
use pyo3::types::{PyList, PyLong, PyString};
use tokio::fs::File;
use vortex::Array;
use vortex_dtype::field::Field;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_sampling_compressor::ALL_COMPRESSORS_CONTEXT;
use vortex_serde::io::{ObjectStoreReadAt, VortexReadAt};
use vortex_serde::layouts::{
    LayoutBatchStream, LayoutContext, LayoutDescriptorReader, LayoutDeserializer,
    LayoutReaderBuilder, LayoutWriter, Projection, RowFilter,
};

use crate::expr::PyExpr;
use crate::{PyArray, TOKIO_RUNTIME};

/// Read a vortex struct array from the local filesystem.
///
/// Parameters
/// ----------
/// file : :class:`str`, optional
///     The file path to read from. Only one of `url` and `file` may be specified.
/// url : :class:`str`, optional
///     The URL to read from. Only one of `url` and `file` may be specified.
/// projection : :class:`list`[:class:`str` ``|`` :class:`int`]
///     The columns to read identified either by their index or name.
/// row_filter : :class:`.Expr`
///     Keep only the rows for which this expression evaluates to true.
///
/// Examples
/// --------
///
/// Read an array with a structured column and nulls at multiple levels and in multiple columns.
///
/// >>> a = vortex.encoding.array([
/// ...     {'name': 'Joseph', 'age': 25},
/// ...     {'name': None, 'age': 31},
/// ...     {'name': 'Angela', 'age': None},
/// ...     {'name': 'Mikhail', 'age': 57},
/// ...     {'name': None, 'age': None},
/// ... ])
/// >>> vortex.io.write(a, "a.vortex")
/// >>> b = vortex.io.read("a.vortex")
/// >>> b.to_arrow_array()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: int64
///   [
///     25,
///     31,
///     null,
///     57,
///     null
///   ]
/// -- child 1 type: string_view
///   [
///     "Joseph",
///     null,
///     "Angela",
///     "Mikhail",
///     null
///   ]
///
/// Read just the age column:
///
/// >>> c = vortex.io.read("a.vortex", projection = ["age"])
/// >>> c.to_arrow_array()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: int64
///   [
///     25,
///     31,
///     null,
///     57,
///     null
///   ]
///
/// Read just the name column, by its index:
///
/// >>> d = vortex.io.read("a.vortex", projection = [1])
/// >>> d.to_arrow_array()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: string_view
///   [
///     "Joseph",
///     null,
///     "Angela",
///     "Mikhail",
///     null
///   ]
///
///
/// Keep rows with an age above 35. This will read O(N_KEPT) rows, when the file format allows.
///
/// >>> e = vortex.io.read("a.vortex", row_filter = vortex.expr.column("age") > 35)
/// >>> e.to_arrow_array()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: int64
///   [
///     57
///   ]
/// -- child 1 type: string_view
///   [
///     "Mikhail"
///   ]
///
/// TODO(DK): Repeating a column in a projection does not work
///
/// Read the age column by name, twice, and the name column by index, once:
///
/// >>> # e = vortex.io.read("a.vortex", projection = ["age", 1, "age"])
/// >>> # e.to_arrow_array()
///
/// TODO(DK): Top-level nullness does not work.
///
/// >>> a = vortex.encoding.array([
/// ...     {'name': 'Joseph', 'age': 25},
/// ...     {'name': None, 'age': 31},
/// ...     {'name': 'Angela', 'age': None},
/// ...     None,
/// ...     {'name': 'Mikhail', 'age': 57},
/// ...     {'name': None, 'age': None},
/// ... ])
/// >>> vortex.io.write(a, "a.vortex")
/// >>> b = vortex.io.read("a.vortex")
/// >>> # b.to_arrow_array()
///
#[pyfunction]
#[pyo3(signature = (*, file = None, url = None, projection = None, row_filter = None))]
pub fn read(
    file: Option<&Bound<PyString>>,
    url: Option<&Bound<PyString>>,
    projection: Option<&Bound<PyAny>>,
    row_filter: Option<&Bound<PyExpr>>,
) -> PyResult<PyArray> {
    let projection = match projection {
        None => Projection::All,
        Some(projection) => {
            let list: &Bound<PyList> = projection.downcast()?;
            Projection::Flat(
                list.iter()
                    .map(|field| -> PyResult<Field> {
                        if field.clone().is_instance_of::<PyString>() {
                            Ok(Field::Name(
                                field.downcast::<PyString>()?.to_str()?.to_string(),
                            ))
                        } else if field.is_instance_of::<PyLong>() {
                            Ok(Field::Index(field.extract()?))
                        } else {
                            Err(PyTypeError::new_err(format!(
                                "projection: expected list of string, int, and None, but found: {}.",
                                field,
                            )))
                        }
                    })
                    .collect::<PyResult<Vec<Field>>>()?,
            )
        }
    };

    let row_filter = row_filter.map(|x| RowFilter::new(x.borrow().unwrap().clone()));

    let file = file.map(|x| x.extract()).transpose()?;
    let url = url.map(|x| x.extract()).transpose()?;

    let inner = TOKIO_RUNTIME.block_on(read_array(
        &FileOrUrl::try_new(file, url)?,
        projection,
        None,
        row_filter,
    ))?;
    Ok(PyArray::new(inner))
}

pub(crate) async fn layout_stream_from_reader<T: VortexReadAt + Unpin>(
    reader: T,
    projection: Projection,
    batch_size: Option<usize>,
    row_filter: Option<RowFilter>,
) -> VortexResult<LayoutBatchStream<T>> {
    let mut builder = LayoutReaderBuilder::new(
        reader,
        LayoutDeserializer::new(
            ALL_COMPRESSORS_CONTEXT.clone(),
            LayoutContext::default().into(),
        ),
    )
    .with_projection(projection);

    if let Some(batch_size) = batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    if let Some(row_filter) = row_filter {
        builder = builder.with_row_filter(row_filter);
    }

    builder.build().await
}

pub(crate) async fn read_array_from_reader<T: VortexReadAt + Unpin + 'static>(
    reader: T,
    projection: Projection,
    batch_size: Option<usize>,
    row_filter: Option<RowFilter>,
) -> VortexResult<Array> {
    layout_stream_from_reader(reader, projection, batch_size, row_filter)
        .await?
        .read_all()
        .await
}

pub(crate) async fn read_dtype_from_reader<T: VortexReadAt + Unpin + 'static>(
    reader: T,
) -> VortexResult<DType> {
    LayoutDescriptorReader::new(LayoutDeserializer::new(
        ALL_COMPRESSORS_CONTEXT.clone(),
        LayoutContext::default().into(),
    ))
    .read_footer(&reader, reader.size().await)
    .await?
    .dtype()
}

pub enum FileOrUrl {
    File(String),
    Url(String),
}

impl FileOrUrl {
    pub fn try_new(file: Option<&str>, url: Option<&str>) -> VortexResult<FileOrUrl> {
        match (file, url) {
            (None, None) | (Some(_), Some(_)) => {
                vortex_bail!("Exactly one of file and url must be specified.",)
            }
            (Some(file), _) => Ok(FileOrUrl::File(file.to_string())),
            (_, Some(url)) => Ok(FileOrUrl::Url(url.to_string())),
        }
    }
}

pub(crate) async fn read_dtype(file_or_url: &FileOrUrl) -> VortexResult<DType> {
    match file_or_url {
        FileOrUrl::File(file) => read_dtype_from_reader(File::open(Path::new(file)).await?).await,
        FileOrUrl::Url(url) => {
            read_dtype_from_reader(ObjectStoreReadAt::try_new_from_url(url).await?).await
        }
    }
}

pub(crate) async fn read_array(
    file_or_url: &FileOrUrl,
    projection: Projection,
    batch_size: Option<usize>,
    row_filter: Option<RowFilter>,
) -> VortexResult<Array> {
    match file_or_url {
        FileOrUrl::File(file) => {
            let reader = File::open(Path::new(file)).await?;
            read_array_from_reader(reader, projection, batch_size, row_filter).await
        }
        FileOrUrl::Url(url) => {
            let reader = ObjectStoreReadAt::try_new_from_url(url).await?;
            read_array_from_reader(reader, projection, batch_size, row_filter).await
        }
    }
}

#[pyfunction]
/// Write a vortex struct array to the local filesystem.
///
/// Parameters
/// ----------
/// array : :class:`vortex.encoding.Array`
///     The array. Must be an array of structures.
///
/// f : :class:`str`
///     The file path.
///
/// Examples
/// --------
///
/// Write the array `a` to the local file `a.vortex`.
///
/// >>> a = vortex.encoding.array([
/// ...     {'x': 1},
/// ...     {'x': 2},
/// ...     {'x': 10},
/// ...     {'x': 11},
/// ...     {'x': None},
/// ... ])
/// >>> vortex.io.write(a, "a.vortex")
///
pub fn write(array: &Bound<'_, PyArray>, f: &Bound<'_, PyString>) -> PyResult<()> {
    async fn run(array: &Array, fname: &str) -> PyResult<()> {
        let file = File::create(Path::new(fname)).await?;
        let mut writer = LayoutWriter::new(file);

        writer = writer.write_array_columns(array.clone()).await?;
        writer.finalize().await?;
        Ok(())
    }

    let fname = f.to_str()?; // TODO(dk): support file objects
    let array = array.borrow().unwrap().clone();

    TOKIO_RUNTIME.block_on(run(&array, fname))
}
