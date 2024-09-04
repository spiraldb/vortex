use pyo3::prelude::*;
use vortex_sampling_compressor::SamplingCompressor;

use crate::array::PyArray;
use crate::error::PyVortexError;

#[pyfunction]
/// Attempt to compress a vortex array.
///
/// Parameters
/// ----------
/// array : :class:`vortex.encoding.Array`
///
///     The array.
///
/// Examples
/// --------
///
/// Compress a very sparse array of integers:
///
/// >>> a = vortex.encoding.array([42 for _ in range(1000)])
/// >>> str(vortex.encoding.compress(a))
/// 'vortex.constant(0x0a)(i64, len=1000)'
///
/// Compress an array of increasing integers:
///
/// >>> a = vortex.encoding.array(list(range(1000)))
/// >>> str(vortex.encoding.compress(a))
/// 'fastlanes.for(0x0f)(i64, len=1000)'
///
/// Compress an array of increasing floating-point numbers and a few nulls:
///
/// >>> a = vortex.encoding.array([
/// ...     float(x) if x % 20 != 0 else None
/// ...     for x in range(1000)
/// ... ])
/// >>> str(vortex.encoding.compress(a))
/// 'vortex.alp(0x0d)(f64?, len=1000)'
pub fn compress<'py>(array: &Bound<'py, PyArray>) -> PyResult<Bound<'py, PyArray>> {
    let compressor = SamplingCompressor::default();
    let inner = compressor
        .compress(&array.borrow().unwrap(), None)
        .map_err(PyVortexError::new)?
        .into_array();
    Bound::new(array.py(), PyArray::new(inner))
}
