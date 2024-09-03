use std::path::Path;

use futures::StreamExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::pyfunction;
use pyo3::types::{PyList, PyLong, PyString};
use tokio::fs::File;
use vortex::array::{ChunkedArray, StructArray};
use vortex::validity::Validity;
use vortex::variants::StructArrayTrait;
use vortex::{Array, Context, IntoArray};
use vortex_dtype::field::Field;
use vortex_error::{vortex_err, VortexResult};
use vortex_serde::io::TokioAdapter;
use vortex_serde::layouts::{
    LayoutBatchStream, LayoutContext, LayoutDeserializer, LayoutReaderBuilder, LayoutWriter,
    Projection, RowFilter,
};

use crate::error::PyVortexError;
use crate::expr::PyExpr;
use crate::PyArray;

/// Read a vortex array from the local filesystem.
///
/// Parameters
/// ----------
/// f : :class:`str`
///
///     The file path.
///
/// Examples
/// --------
///
/// Read an array that was written to the local file `a.vortex`.
///
/// >>> a = vortex.encode([1,2,3])
/// >>> vortex.io.write_vortex_array(a, "a.vortex")
/// >>> b = vortex.io.read_vortex_array("a.vortex")
/// >>> b.to_arrow()
/// <pyarrow.lib.Int64Array object at ...>
/// [
///   1,
///   2,
///   3
/// ]
///
/// Read an array with a structured column and nulls at multiple levels and in multiple columns.
///
/// >>> a = vortex.encode([
/// ...     {'name': 'Joseph', 'age': 25},
/// ...     {'name': None, 'age': 31},
/// ...     {'name': 'Angela', 'age': None},
/// ...     {'name': 'Mikhail', 'age': 57},
/// ...     None,
/// ...     {'name': None, 'age': None},
/// ... ])
/// >>> vortex.io.write_vortex_array(a, "a.vortex")
/// >>> b = vortex.io.read_vortex_array("a.vortex")
/// >>> b.to_arrow()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid:
///   [
///     true,
///     true,
///     true,
///     true,
///     false,
///     true
///   ]
/// -- child 0 type: int64
///   [
///     25,
///     31,
///     null,
///     57,
///     0,
///     null
///   ]
/// -- child 1 type: string
///   [
///     "Joseph",
///     null,
///     "Angela",
///     "Mikhail",
///     "",
///     null
///   ]
///
#[pyfunction]
pub fn read_vortex_array<'py>(f: &Bound<'py, PyString>) -> PyResult<Bound<'py, PyArray>> {
    async fn run(fname: &str) -> VortexResult<Array> {
        let file = File::open(Path::new(fname)).await?;

        let value: LayoutBatchStream<TokioAdapter<File>> = LayoutReaderBuilder::new(
            TokioAdapter(file), // TODO(dk): Why didn't we implement this on File directly?
            LayoutDeserializer::new(Context::default().into(), LayoutContext::default().into()),
        )
        .build()
        .await?;

        let dtype = value.schema().into_dtype();

        let vecs = value
            .map(|a| {
                StructArray::try_from(a?)?
                    .field_by_name("_")
                    .ok_or_else(|| {
                        vortex_err!("Python can only read files written by write_vortex_array")
                    })
            })
            .collect::<Vec<VortexResult<Array>>>()
            .await
            .into_iter() // TODO(dk) unclear why I need two collects to pacify the compiler
            .collect::<VortexResult<Vec<Array>>>()?;

        if vecs.len() == 1 {
            Ok(vecs.into_iter().next().unwrap())
        } else {
            ChunkedArray::try_new(vecs, dtype).map(|e| e.into())
        }
    }

    let fname = f.to_str()?; // TODO(dk): support file objects

    let inner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(fname))
        .map_err(PyVortexError::new)?;

    Bound::new(f.py(), PyArray::new(inner))
}

#[pyfunction]
/// Write a vortex array to the local filesystem.
///
/// Parameters
/// ----------
/// array : :class:`vortex.encoding.Array`
///
///     The array.
///
/// f : :class:`str`
///
///     The file path.
///
/// Examples
/// --------
///
/// Write the array `a` to the local file `a.vortex`.
///
/// >>> a = vortex.encode([1,2,3])
/// >>> vortex.io.write_vortex_array(a, "a.vortex")
///
pub fn write_vortex_array(array: &Bound<'_, PyArray>, f: &Bound<'_, PyString>) -> PyResult<()> {
    async fn run(array: &Array, fname: &str) -> VortexResult<()> {
        let file = File::create(Path::new(fname)).await?;
        let mut writer = LayoutWriter::new(file);

        writer = writer.write_array_columns(array.clone()).await?;
        writer.finalize().await?;
        Ok(())
    }

    let fname = f.to_str()?; // TODO(dk): support file objects
    let array = array.borrow().unwrap().clone();

    let wrapper = StructArray::try_new(
        vec!["_".into()].into(),
        vec![array.clone()],
        array.len(),
        Validity::AllValid,
    )
    .map_err(PyVortexError::map_err)?
    .into_array();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(&wrapper, fname))
        .map_err(PyVortexError::map_err)
}

/// Read a vortex struct array from the local filesystem.
///
/// Parameters
/// ----------
/// f : :class:`str`
///
///     The file path.
///
/// Examples
/// --------
///
/// TODO(DK): Top-level nullness does not work.
///
/// Read an array with a structured column and nulls at multiple levels and in multiple columns.
///
/// >>> a = vortex.encode([
/// ...     {'name': 'Joseph', 'age': 25},
/// ...     {'name': None, 'age': 31},
/// ...     {'name': 'Angela', 'age': None},
/// ...     {'name': 'Mikhail', 'age': 57},
/// ...     {'name': None, 'age': None},
/// ... ])
/// >>> vortex.io.write_vortex_struct_array(a, "a.vortex")
/// >>> b = vortex.io.read_vortex_struct_array("a.vortex")
/// >>> b.to_arrow()
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
/// -- child 1 type: string
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
/// >>> c = vortex.io.read_vortex_struct_array("a.vortex", projection = ["age"])
/// >>> c.to_arrow()
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
/// >>> d = vortex.io.read_vortex_struct_array("a.vortex", projection = [1])
/// >>> d.to_arrow()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: string
///   [
///     "Joseph",
///     null,
///     "Angela",
///     "Mikhail",
///     null
///   ]
///
///
/// TODO(DK): This panics
/// Read the age column by name, twice, and the name column by index, once:
///
/// >>> # e = vortex.io.read_vortex_struct_array("a.vortex", projection = ["age", 1, "age"])
/// >>> # e.to_arrow()
///
/// Keep rows with an age above 35. This will read O(N_KEPT) rows, when the file format allows.
///
/// >>> e = vortex.io.read_vortex_struct_array("a.vortex", row_filter = vortex.expr.column("age") > 35)
/// >>> e.to_arrow()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: int64
///   [
///     57
///   ]
/// -- child 1 type: string
///   [
///     "Mikhail"
///   ]
#[pyfunction]
#[pyo3(signature = (f, projection = None, row_filter = None))]
pub fn read_vortex_struct_array<'py>(
    f: &Bound<'py, PyString>,
    projection: Option<&Bound<'py, PyAny>>,
    row_filter: Option<&Bound<'py, PyExpr>>,
) -> PyResult<Bound<'py, PyArray>> {
    async fn run(
        fname: &str,
        projection: Projection,
        row_filter: Option<RowFilter>,
    ) -> VortexResult<Array> {
        let file = File::open(Path::new(fname)).await?;

        let mut builder: LayoutReaderBuilder<TokioAdapter<File>> = LayoutReaderBuilder::new(
            TokioAdapter(file), // TODO(dk): Why didn't we implement this on File directly?
            LayoutDeserializer::new(Context::default().into(), LayoutContext::default().into()),
        )
        .with_projection(projection);

        if let Some(row_filter) = row_filter {
            builder = builder.with_row_filter(row_filter);
        }

        let stream = builder.build().await?;

        let dtype = stream.schema().into_dtype();

        let vecs = stream
            .collect::<Vec<VortexResult<Array>>>()
            .await
            .into_iter() // TODO(dk) unclear why I need two collects to pacify the compiler
            .collect::<VortexResult<Vec<Array>>>()?;

        if vecs.len() == 1 {
            Ok(vecs.into_iter().next().unwrap())
        } else {
            ChunkedArray::try_new(vecs, dtype).map(|e| e.into())
        }
    }

    let fname = f.to_str()?; // TODO(dk): support file objects

    let projection = match projection {
        None => Projection::All,
        Some(projection) => {
            let list: &Bound<'py, PyList> = projection.downcast()?;
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

    let inner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(fname, projection, row_filter))
        .map_err(PyVortexError::new)?;

    Bound::new(f.py(), PyArray::new(inner))
}

#[pyfunction]
/// Write a vortex struct array to the local filesystem.
///
/// Parameters
/// ----------
/// array : :class:`vortex.encoding.Array`
///
///     The array. Must be an array of structures. For scalar typed arrays see :func:`write_vortex_array`.
///
/// f : :class:`str`
///
///     The file path.
///
/// Examples
/// --------
///
/// Write the array `a` to the local file `a.vortex`.
///
/// >>> a = vortex.encode([
/// ...     {'x': 1},
/// ...     {'x': 2},
/// ...     {'x': 10},
/// ...     {'x': 11},
/// ...     {'x': None},
/// ... ])
/// >>> vortex.io.write_vortex_array(a, "a.vortex")
///
pub fn write_vortex_struct_array(
    array: &Bound<'_, PyArray>,
    f: &Bound<'_, PyString>,
) -> PyResult<()> {
    async fn run(array: &Array, fname: &str) -> VortexResult<()> {
        let file = File::create(Path::new(fname)).await?;
        let mut writer = LayoutWriter::new(file);

        writer = writer.write_array_columns(array.clone()).await?;
        writer.finalize().await?;
        Ok(())
    }

    let fname = f.to_str()?; // TODO(dk): support file objects
    let array = array.borrow().unwrap().clone();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(&array, fname))
        .map_err(PyVortexError::map_err)
}
