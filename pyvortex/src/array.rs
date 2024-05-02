use paste::paste;
use pyo3::prelude::*;
use vortex::array::bool::{Bool, BoolArray, BoolEncoding, OwnedBoolArray};
use vortex::array::chunked::{Chunked, ChunkedArray, ChunkedEncoding, OwnedChunkedArray};
use vortex::array::constant::{Constant, ConstantArray, ConstantEncoding, OwnedConstantArray};
use vortex::array::primitive::{OwnedPrimitiveArray, Primitive, PrimitiveArray, PrimitiveEncoding};
use vortex::array::r#struct::{OwnedStructArray, Struct, StructArray, StructEncoding};
use vortex::array::sparse::{OwnedSparseArray, Sparse, SparseArray, SparseEncoding};
use vortex::array::varbin::{OwnedVarBinArray, VarBin, VarBinArray, VarBinEncoding};
use vortex::array::varbinview::{
    OwnedVarBinViewArray, VarBinView, VarBinViewArray, VarBinViewEncoding,
};
use vortex::compute::take::take;
use vortex::encoding::EncodingRef;
use vortex::ArrayDef;
use vortex::ToStatic;
use vortex::{ArrayDType, OwnedArray};
use vortex_alp::{ALPArray, ALPEncoding, OwnedALPArray, ALP};
use vortex_dict::{Dict, DictArray, DictEncoding, OwnedDictArray};
use vortex_fastlanes::{
    BitPacked, BitPackedArray, BitPackedEncoding, Delta, DeltaArray, DeltaEncoding, FoR, FoRArray,
    FoREncoding, OwnedBitPackedArray, OwnedDeltaArray, OwnedFoRArray,
};
use vortex_ree::{OwnedREEArray, REEArray, REEEncoding, REE};
use vortex_roaring::{
    OwnedRoaringBoolArray, OwnedRoaringIntArray, RoaringBool, RoaringBoolArray,
    RoaringBoolEncoding, RoaringInt, RoaringIntArray, RoaringIntEncoding,
};
use vortex_zigzag::{OwnedZigZagArray, ZigZag, ZigZagArray, ZigZagEncoding};

use crate::dtype::PyDType;
use crate::error::PyVortexError;
use crate::vortex_arrow;

#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
pub struct PyArray {
    inner: OwnedArray,
}

macro_rules! pyarray {
    ($E:ident, $T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "vortex", extends = PyArray, sequence, subclass)]
            pub struct [<Py $T>] {
                inner: [<Owned $T>],
                #[allow(dead_code)]
                encoding: EncodingRef,
            }

           impl [<Py $T>] {
               pub fn wrap(py: Python<'_>, inner: [<Owned $T>]) -> PyResult<Py<Self>> {
                   let init = PyClassInitializer::from(PyArray { inner: inner.array().to_static() })
                        .add_subclass([<Py $T>] { inner, encoding: &$E });
                   Py::new(py, init)
               }

               pub fn unwrap(&self) -> &$T {
                   &self.inner
               }
           }
        }
    };
}

pyarray!(BoolEncoding, BoolArray, "BoolArray");
pyarray!(ChunkedEncoding, ChunkedArray, "ChunkedArray");
pyarray!(ConstantEncoding, ConstantArray, "ConstantArray");
pyarray!(PrimitiveEncoding, PrimitiveArray, "PrimitiveArray");
pyarray!(SparseEncoding, SparseArray, "SparseArray");
pyarray!(StructEncoding, StructArray, "StructArray");
pyarray!(VarBinEncoding, VarBinArray, "VarBinArray");
pyarray!(VarBinViewEncoding, VarBinViewArray, "VarBinViewArray");

pyarray!(ALPEncoding, ALPArray, "ALPArray");
pyarray!(BitPackedEncoding, BitPackedArray, "BitPackedArray");
pyarray!(FoREncoding, FoRArray, "FoRArray");
pyarray!(DeltaEncoding, DeltaArray, "DeltaArray");
pyarray!(DictEncoding, DictArray, "DictArray");
pyarray!(REEEncoding, REEArray, "REEArray");
pyarray!(RoaringBoolEncoding, RoaringBoolArray, "RoaringBoolArray");
pyarray!(RoaringIntEncoding, RoaringIntArray, "RoaringIntArray");
pyarray!(ZigZagEncoding, ZigZagArray, "ZigZagArray");

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: OwnedArray) -> PyResult<Py<Self>> {
        match inner.encoding().id().as_ref() {
            Bool::ID_STR => PyBoolArray::wrap(
                py,
                OwnedBoolArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Chunked::ID_STR => PyChunkedArray::wrap(
                py,
                OwnedChunkedArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Constant::ID_STR => PyConstantArray::wrap(
                py,
                OwnedConstantArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Primitive::ID_STR => PyPrimitiveArray::wrap(
                py,
                OwnedPrimitiveArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Sparse::ID_STR => PySparseArray::wrap(
                py,
                OwnedSparseArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Struct::ID_STR => PyStructArray::wrap(
                py,
                OwnedStructArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBin::ID_STR => PyVarBinArray::wrap(
                py,
                OwnedVarBinArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBinView::ID_STR => PyVarBinViewArray::wrap(
                py,
                OwnedVarBinViewArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Dict::ID_STR => PyDictArray::wrap(
                py,
                OwnedDictArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            REE::ID_STR => PyREEArray::wrap(
                py,
                OwnedREEArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Delta::ID_STR => PyDeltaArray::wrap(
                py,
                OwnedDeltaArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            FoR::ID_STR => PyFoRArray::wrap(
                py,
                OwnedFoRArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            BitPacked::ID_STR => PyBitPackedArray::wrap(
                py,
                OwnedBitPackedArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),

            ALP::ID_STR => PyALPArray::wrap(
                py,
                OwnedALPArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RoaringBool::ID_STR => PyBitPackedArray::wrap(
                py,
                OwnedBitPackedArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RoaringInt::ID_STR => PyBitPackedArray::wrap(
                py,
                OwnedBitPackedArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            ZigZag::ID_STR => PyZigZagArray::wrap(
                py,
                OwnedZigZagArray::try_from(inner).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            _ => Py::new(py, Self { inner }),
            // ArrayKind::Other(other) => match other.encoding().id() {
            //     // PyEnc chooses to expose certain encodings as first-class objects.
            //     // For the remainder, we should have a generic EncArray implementation that supports basic functions.
            //     ALPEncoding::ID_STR => {
            //         PyALPArray::wrap(py, inner.into_any().downcast::<ALPArray>().unwrap())?
            //             .extract(py)
            //     }
            //     RoaringBoolEncoding::ID_STR => PyRoaringBoolArray::wrap(
            //         py,
            //         inner.into_any().downcast::<RoaringBoolArray>().unwrap(),
            //     )?
            //     .extract(py),
            //     RoaringIntEncoding::ID_STR => PyRoaringIntArray::wrap(
            //         py,
            //         inner.into_any().downcast::<RoaringIntArray>().unwrap(),
            //     )?
            //     .extract(py),
            //     ZigZagEncoding::ID_STR => {
            //         PyZigZagArray::wrap(py, inner.into_any().downcast::<ZigZagArray>().unwrap())?
            //             .extract(py)
            //     }
            //     _ => Py::new(py, Self { inner }),
            //},
        }
    }

    pub fn unwrap(&self) -> &OwnedArray {
        &self.inner
    }
}

#[pymethods]
impl PyArray {
    fn to_pyarrow(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        vortex_arrow::export_array(self_.py(), &self_.inner)
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

    #[getter]
    fn dtype(self_: PyRef<Self>) -> PyResult<Py<PyDType>> {
        PyDType::wrap(self_.py(), self_.inner.dtype().clone())
    }

    fn take(&self, indices: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        take(&self.inner, indices.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|arr| PyArray::wrap(indices.py(), arr))
    }
}
//
// #[pymethods]
// impl PyRoaringBoolArray {
//     #[staticmethod]
//     fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
//         RoaringBoolArray::encode(array.unwrap())
//             .map_err(PyVortexError::map_err)
//             .and_then(|zarray| PyArray::wrap(array.py(), zarray.into_array()))
//     }
// }
//
// #[pymethods]
// impl PyRoaringIntArray {
//     #[staticmethod]
//     fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
//         RoaringIntArray::encode(array.unwrap())
//             .map_err(PyVortexError::map_err)
//             .and_then(|zarray| PyArray::wrap(array.py(), zarray.into_array()))
//     }
// }
//
// #[pymethods]
// impl PyZigZagArray {
//     #[staticmethod]
//     fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
//         ZigZagArray::encode(array.unwrap())
//             .map_err(PyVortexError::map_err)
//             .and_then(|zarray| PyArray::wrap(array.py(), zarray))
//     }
// }
