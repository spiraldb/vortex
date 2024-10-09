use std::path::Path;
use std::sync::Arc;

use futures::TryStreamExt;
use lazy_static::lazy_static;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::pyfunction;
use pyo3::types::{PyList, PyLong, PyString};
use tokio::fs::File;
use vortex::array::ChunkedArray;
use vortex::encoding::EncodingRef;
use vortex::{Array, Context};
use vortex_alp::{ALPEncoding, ALPRDEncoding};
use vortex_bytebool::ByteBoolEncoding;
use vortex_datetime_parts::DateTimePartsEncoding;
use vortex_dict::DictEncoding;
use vortex_dtype::field::Field;
use vortex_error::{vortex_panic, VortexResult};
use vortex_fastlanes::{BitPackedEncoding, DeltaEncoding, FoREncoding};
use vortex_fsst::FSSTEncoding;
use vortex_roaring::{RoaringBoolEncoding, RoaringIntEncoding};
use vortex_runend::RunEndEncoding;
use vortex_runend_bool::RunEndBoolEncoding;
use vortex_serde::layouts::{
    LayoutContext, LayoutDeserializer, LayoutReaderBuilder, LayoutWriter, Projection, RowFilter,
};
use vortex_zigzag::ZigZagEncoding;

use crate::error::PyVortexError;
use crate::expr::PyExpr;
use crate::PyArray;

lazy_static! {
    pub static ref MAXIMAL_CTX: Arc<Context> = Arc::new(Context::default().with_encodings([
        &ALPEncoding as EncodingRef,
        &ByteBoolEncoding,
        &DateTimePartsEncoding,
        &DictEncoding,
        &BitPackedEncoding,
        &DeltaEncoding,
        &FoREncoding,
        &FSSTEncoding,
        &RoaringBoolEncoding,
        &RoaringIntEncoding,
        &RunEndEncoding,
        &RunEndBoolEncoding,
        &ZigZagEncoding,
        &ALPRDEncoding,
    ]));
}

/// Read a vortex struct array from the local filesystem.
///
/// Parameters
/// ----------
/// f : :class:`str`
///     The file path.
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
/// >>> c = vortex.io.read("a.vortex", projection = ["age"])
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
/// >>> d = vortex.io.read("a.vortex", projection = [1])
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
/// Keep rows with an age above 35. This will read O(N_KEPT) rows, when the file format allows.
///
/// >>> e = vortex.io.read("a.vortex", row_filter = vortex.expr.column("age") > 35)
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
///
/// TODO(DK): Repeating a column in a projection does not work
///
/// Read the age column by name, twice, and the name column by index, once:
///
/// >>> # e = vortex.io.read("a.vortex", projection = ["age", 1, "age"])
/// >>> # e.to_arrow()
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
/// >>> # b.to_arrow()
///
#[pyfunction]
#[pyo3(signature = (f, projection = None, row_filter = None))]
pub fn read<'py>(
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

        let mut builder: LayoutReaderBuilder<File> = LayoutReaderBuilder::new(
            file,
            LayoutDeserializer::new(MAXIMAL_CTX.clone(), LayoutContext::default().into()),
        )
        .with_projection(projection);

        if let Some(row_filter) = row_filter {
            builder = builder.with_row_filter(row_filter);
        }

        let stream = builder.build().await?;
        let dtype = stream.schema().clone().into();

        let vecs: Vec<Array> = stream.try_collect().await?;

        if vecs.len() == 1 {
            vecs.into_iter().next().ok_or_else(|| {
                vortex_panic!(
                    "Should be impossible: vecs.len() == 1 but couldn't get first element"
                )
            })
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
