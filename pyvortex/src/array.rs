use arrow::array::{Array as ArrowArray, ArrayRef};
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};
use vortex::array::ChunkedArray;
use vortex::compute::unary::fill_forward;
use vortex::compute::{slice, take};
use vortex::{Array, ArrayDType, IntoCanonical};

use crate::dtype::PyDType;
use crate::error::PyVortexError;
use crate::python_repr::PythonRepr;

#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
/// An array of zero or more *rows* each with the same set of *columns*.
pub struct PyArray {
    inner: Array,
}

impl PyArray {
    pub fn new(inner: Array) -> PyArray {
        PyArray { inner }
    }

    pub fn unwrap(&self) -> &Array {
        &self.inner
    }
}

#[pymethods]
impl PyArray {
    /// Convert this array to an Arrow array.
    ///
    /// .. seealso::
    ///     :meth:`.to_arrow_table`
    ///
    /// Returns
    /// -------
    /// :class:`pyarrow.Array`
    ///
    /// Examples
    /// --------
    ///
    /// Round-trip an Arrow array through a Vortex array:
    ///
    ///     >>> vortex.encoding.array([1, 2, 3]).to_arrow_array()
    ///     <pyarrow.lib.Int64Array object at ...>
    ///     [
    ///       1,
    ///       2,
    ///       3
    ///     ]
    fn to_arrow_array(self_: PyRef<'_, Self>) -> PyResult<Bound<PyAny>> {
        // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
        let py = self_.py();
        let vortex = &self_.inner;

        if let Ok(chunked_array) = ChunkedArray::try_from(vortex) {
            let chunks: Vec<ArrayRef> = chunked_array
                .chunks()
                .map(|chunk| -> PyResult<ArrayRef> {
                    chunk
                        .into_canonical()
                        .and_then(|arr| arr.into_arrow())
                        .map_err(PyVortexError::map_err)
                })
                .collect::<PyResult<Vec<ArrayRef>>>()?;
            if chunks.is_empty() {
                return Err(PyValueError::new_err("No chunks in array"));
            }
            let pa_data_type = chunks[0].data_type().clone().to_pyarrow(py)?;
            let chunks: PyResult<Vec<PyObject>> = chunks
                .iter()
                .map(|arrow_array| arrow_array.into_data().to_pyarrow(py))
                .collect();

            // Combine into a chunked array
            PyModule::import_bound(py, "pyarrow")?.call_method(
                "chunked_array",
                (PyList::new_bound(py, chunks?),),
                Some(&[("type", pa_data_type)].into_py_dict_bound(py)),
            )
        } else {
            Ok(vortex
                .clone()
                .into_canonical()
                .and_then(|arr| arr.into_arrow())
                .map_err(PyVortexError::map_err)?
                .into_data()
                .to_pyarrow(py)?
                .into_bound(py))
        }
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

    /// The data type of this array.
    ///
    /// Returns
    /// -------
    /// :class:`vortex.dtype.DType`
    ///
    /// Examples
    /// --------
    ///
    /// By default, :func:`vortex.encoding.array` uses the largest available bit-width:
    ///
    ///     >>> vortex.encoding.array([1, 2, 3]).dtype
    ///     int(64, False)
    ///
    /// Including a :obj:`None` forces a nullable type:
    ///
    ///     >>> vortex.encoding.array([1, None, 2, 3]).dtype
    ///     int(64, True)
    ///
    /// A UTF-8 string array:
    ///
    ///     >>> vortex.encoding.array(['hello, ', 'is', 'it', 'me?']).dtype
    ///     utf8(False)
    #[getter]
    fn dtype(self_: PyRef<Self>) -> PyResult<Py<PyDType>> {
        PyDType::wrap(self_.py(), self_.inner.dtype().clone())
    }

    /// Filter an Array by another Boolean array.
    ///
    /// Parameters
    /// ----------
    /// filter : :class:`vortex.encoding.Array`
    ///     Keep all the rows in ``self`` for which the correspondingly indexed row in `filter` is True.
    ///
    /// Returns
    /// -------
    /// :class:`vortex.encoding.Array`
    ///
    /// Examples
    /// --------
    ///
    /// Keep only the single digit positive integers.
    ///
    /// >>> a = vortex.encoding.array([0, 42, 1_000, -23, 10, 9, 5])
    /// >>> filter = vortex.array([True, False, False, False, False, True, True])
    /// >>> a.filter(filter).to_arrow_array()
    /// <pyarrow.lib.Int64Array object at ...>
    /// [
    ///   0,
    ///   9,
    ///   5
    /// ]
    fn filter(&self, filter: &Bound<PyArray>) -> PyResult<PyArray> {
        let filter = filter.borrow();

        vortex::compute::filter(&self.inner, &filter.inner)
            .map_err(PyVortexError::map_err)
            .map(|arr| PyArray { inner: arr })
    }

    /// Fill forward non-null values over runs of nulls.
    ///
    /// Leading nulls are replaced with the "zero" for that type. For integral and floating-point
    /// types, this is zero. For the Boolean type, this is `:obj:`False`.
    ///
    /// Fill forward sensor values over intermediate missing values. Note that leading nulls are
    /// replaced with 0.0:
    ///
    /// >>> a = vortex.encoding.array([
    /// ...      None,  None, 30.29, 30.30, 30.30,  None,  None, 30.27, 30.25,
    /// ...     30.22,  None,  None,  None,  None, 30.12, 30.11, 30.11, 30.11,
    /// ...     30.10, 30.08,  None, 30.21, 30.03, 30.03, 30.05, 30.07, 30.07,
    /// ... ])
    /// >>> a.fill_forward().to_arrow_array()
    /// <pyarrow.lib.DoubleArray object at ...>
    /// [
    ///   0,
    ///   0,
    ///   30.29,
    ///   30.3,
    ///   30.3,
    ///   30.3,
    ///   30.3,
    ///   30.27,
    ///   30.25,
    ///   30.22,
    ///   ...
    ///   30.11,
    ///   30.1,
    ///   30.08,
    ///   30.08,
    ///   30.21,
    ///   30.03,
    ///   30.03,
    ///   30.05,
    ///   30.07,
    ///   30.07
    /// ]
    fn fill_forward(&self) -> PyResult<PyArray> {
        fill_forward(&self.inner)
            .map_err(PyVortexError::map_err)
            .map(|arr| PyArray { inner: arr })
    }

    /// Filter, permute, and/or repeat elements by their index.
    ///
    /// Parameters
    /// ----------
    /// indices : :class:`vortex.encoding.Array`
    ///     An array of indices to keep.
    ///
    /// Returns
    /// -------
    /// :class:`vortex.encoding.Array`
    ///
    /// Examples
    /// --------
    ///
    /// Keep only the first and third elements:
    ///
    ///     >>> a = vortex.encoding.array(['a', 'b', 'c', 'd'])
    ///     >>> indices = vortex.encoding.array([0, 2])
    ///     >>> a.take(indices).to_arrow_array()
    ///     <pyarrow.lib.StringArray object at ...>
    ///     [
    ///       "a",
    ///       "c"
    ///     ]
    ///
    /// Permute and repeat the first and second elements:
    ///
    ///     >>> a = vortex.encoding.array(['a', 'b', 'c', 'd'])
    ///     >>> indices = vortex.encoding.array([0, 1, 1, 0])
    ///     >>> a.take(indices).to_arrow_array()
    ///     <pyarrow.lib.StringArray object at ...>
    ///     [
    ///       "a",
    ///       "b",
    ///       "b",
    ///       "a"
    ///     ]
    fn take<'py>(&self, indices: &Bound<'py, PyArray>) -> PyResult<Bound<'py, PyArray>> {
        let py = indices.py();
        let indices = &indices.borrow().inner;

        if !indices.dtype().is_int() {
            return Err(PyValueError::new_err(format!(
                "indices: expected int or uint array, but found: {}",
                indices.dtype().python_repr()
            )));
        }

        take(&self.inner, indices)
            .map_err(PyVortexError::map_err)
            .and_then(|arr| Bound::new(py, PyArray { inner: arr }))
    }

    /// Keep only a contiguous subset of elements.
    ///
    /// Parameters
    /// ----------
    /// start : :class:`int`
    ///     The start index of the range to keep, inclusive.
    ///
    /// end : :class:`int`
    ///     The end index, exclusive.
    ///
    /// Returns
    /// -------
    /// :class:`vortex.encoding.Array`
    ///
    /// Examples
    /// --------
    ///
    /// Keep only the second through third elements:
    ///
    ///     >>> a = vortex.encoding.array(['a', 'b', 'c', 'd'])
    ///     >>> a.slice(1, 3).to_arrow_array()
    ///     <pyarrow.lib.StringArray object at ...>
    ///     [
    ///       "b",
    ///       "c"
    ///     ]
    ///
    /// Keep none of the elements:
    ///
    ///     >>> a = vortex.encoding.array(['a', 'b', 'c', 'd'])
    ///     >>> a.slice(3, 3).to_arrow_array()
    ///     <pyarrow.lib.StringArray object at ...>
    ///     []
    ///
    /// Unlike Python, it is an error to slice outside the bounds of the array:
    ///
    ///     >>> a = vortex.encoding.array(['a', 'b', 'c', 'd'])
    ///     >>> a.slice(2, 10).to_arrow_array()
    ///     Traceback (most recent call last):
    ///     ...
    ///     ValueError: index 10 out of bounds from 0 to 4
    ///
    /// Or to slice with a negative value:
    ///
    ///     >>> a = vortex.encoding.array(['a', 'b', 'c', 'd'])
    ///     >>> a.slice(-2, -1).to_arrow_array()
    ///     Traceback (most recent call last):
    ///     ...
    ///     OverflowError: can't convert negative int to unsigned
    ///
    #[pyo3(signature = (start, end, *))]
    fn slice(&self, start: usize, end: usize) -> PyResult<PyArray> {
        slice(&self.inner, start, end)
            .map(PyArray::new)
            .map_err(PyVortexError::map_err)
    }

    /// Internal technical details about the encoding of this Array.
    ///
    /// Warnings
    /// --------
    /// The format of the returned string may change without notice.
    ///
    /// Returns
    /// -------
    /// :class:`.str`
    ///
    /// Examples
    /// --------
    ///
    /// Uncompressed arrays have straightforward encodings:
    ///
    ///     >>> arr = vortex.encoding.array([1, 2, None, 3])
    ///     >>> print(arr.tree_display())
    ///     root: vortex.primitive(0x03)(i64?, len=4) nbytes=33 B (100.00%)
    ///       metadata: PrimitiveMetadata { validity: Array }
    ///       buffer: 32 B
    ///       validity: vortex.bool(0x02)(bool, len=4) nbytes=1 B (3.03%)
    ///         metadata: BoolMetadata { validity: NonNullable, first_byte_bit_offset: 0 }
    ///         buffer: 1 B
    ///     <BLANKLINE>
    ///
    /// Compressed arrays often have more complex, deeply nested encoding trees.
    fn tree_display(&self) -> String {
        self.inner.tree_display().to_string()
    }
}
