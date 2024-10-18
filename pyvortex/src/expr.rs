use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;
use vortex_dtype::field::Field;
use vortex_dtype::half::f16;
use vortex_dtype::{DType, Nullability, PType};
use vortex_expr::{BinaryExpr, Column, Literal, Operator, VortexExpr};
use vortex_scalar::{PValue, Scalar, ScalarValue};

use crate::dtype::PyDType;

/// An expression describes how to filter rows when reading an array from a file.
///
/// Examples
/// ========
///
/// All the examples read the following file.
///
/// >>> a = vortex.encoding.array([
/// ...     {'name': 'Joseph', 'age': 25},
/// ...     {'name': None, 'age': 31},
/// ...     {'name': 'Angela', 'age': None},
/// ...     {'name': 'Mikhail', 'age': 57},
/// ...     {'name': None, 'age': None},
/// ... ])
/// >>> vortex.io.write(a, "a.vortex")
///
/// Read only those rows whose age column is greater than 35:
///
/// >>> e = vortex.io.read("a.vortex", row_filter = vortex.expr.column("age") > 35)
/// >>> e.to_arrow_array()
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
/// Read only those rows whose age column lies in (21, 33]. Notice that we must use parentheses
/// because of the Python precedence rules for ``&``:
///
/// >>> age = vortex.expr.column("age")
/// >>> e = vortex.io.read("a.vortex", row_filter = (age > 21) & (age <= 33))
/// >>> e.to_arrow_array()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: int64
///   [
///     25,
///     31
///   ]
/// -- child 1 type: string_view
///   [
///     "Joseph",
///     null
///   ]
///
/// Read only those rows whose name is `Joseph`:
///
/// >>> name = vortex.expr.column("name")
/// >>> e = vortex.io.read("a.vortex", row_filter = name == "Joseph")
/// >>> e.to_arrow_array()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: int64
///   [
///     25
///   ]
/// -- child 1 type: string_view
///   [
///     "Joseph"
///   ]
///
/// Read rows whose name is `Angela` or whose age is between 20 and 30, inclusive. Notice that the
/// Angela row is excluded because its age is null. The entire row filtering expression therefore
/// evaluates to null which is interpreted as false:
///
/// >>> name = vortex.expr.column("name")
/// >>> e = vortex.io.read("a.vortex", row_filter = (name == "Angela") | ((age >= 20) & (age <= 30)))
/// >>> e.to_arrow_array()
/// <pyarrow.lib.StructArray object at ...>
/// -- is_valid: all not null
/// -- child 0 type: int64
///   [
///     25
///   ]
/// -- child 1 type: string_view
///   [
///     "Joseph"
///   ]
#[pyclass(name = "Expr", module = "vortex")]
pub struct PyExpr {
    inner: Arc<dyn VortexExpr>,
}

impl PyExpr {
    pub fn unwrap(&self) -> &Arc<dyn VortexExpr> {
        &self.inner
    }
}

fn py_binary_opeartor<'py>(
    left: PyRef<'py, PyExpr>,
    operator: Operator,
    right: Bound<'py, PyExpr>,
) -> PyResult<Bound<'py, PyExpr>> {
    Bound::new(
        left.py(),
        PyExpr {
            inner: Arc::new(BinaryExpr::new(
                left.inner.clone(),
                operator,
                right.borrow().inner.clone(),
            )),
        },
    )
}

fn coerce_expr<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyExpr>> {
    let nonnull = Nullability::NonNullable;
    if let Ok(value) = value.downcast::<PyExpr>() {
        Ok(value.clone())
    } else if let Ok(value) = value.downcast::<PyNone>() {
        scalar(DType::Null, value)
    } else if let Ok(value) = value.downcast::<PyLong>() {
        scalar(DType::Primitive(PType::I64, nonnull), value)
    } else if let Ok(value) = value.downcast::<PyFloat>() {
        scalar(DType::Primitive(PType::F64, nonnull), value)
    } else if let Ok(value) = value.downcast::<PyString>() {
        scalar(DType::Utf8(nonnull), value)
    } else if let Ok(value) = value.downcast::<PyBytes>() {
        scalar(DType::Binary(nonnull), value)
    } else {
        Err(PyValueError::new_err(format!(
            "expected None, int, float, str, or bytes but found: {}",
            value
        )))
    }
}

#[pymethods]
impl PyExpr {
    fn __eq__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::Eq, coerce_expr(right)?)
    }

    fn __neq__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::NotEq, coerce_expr(right)?)
    }

    fn __gt__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::Gt, coerce_expr(right)?)
    }

    fn __ge__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::Gte, coerce_expr(right)?)
    }

    fn __lt__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::Lt, coerce_expr(right)?)
    }

    fn __le__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::Lte, coerce_expr(right)?)
    }

    fn __and__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::And, coerce_expr(right)?)
    }

    fn __or__<'py>(
        self_: PyRef<'py, Self>,
        right: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyExpr>> {
        py_binary_opeartor(self_, Operator::Or, coerce_expr(right)?)
    }
}

/// A named column.
///
/// See :class:`.Expr` for more examples.
///
/// Example
/// =======
///
/// A filter that selects only those rows whose name is `Joseph`:
///
/// >>> name = vortex.expr.column("name")
/// >>> filter = name == "Joseph"
///
#[pyfunction]
pub fn column<'py>(name: &Bound<'py, PyString>) -> PyResult<Bound<'py, PyExpr>> {
    let py = name.py();
    let name: String = name.extract()?;
    Bound::new(
        py,
        PyExpr {
            inner: Arc::new(Column::new(Field::Name(name))),
        },
    )
}

#[pyfunction]
pub fn _literal<'py>(
    dtype: &Bound<'py, PyDType>,
    value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyExpr>> {
    scalar(dtype.borrow().unwrap().clone(), value)
}

pub fn scalar<'py>(dtype: DType, value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyExpr>> {
    let py = value.py();
    Bound::new(
        py,
        PyExpr {
            inner: Arc::new(Literal::new(Scalar::new(
                dtype.clone(),
                scalar_value(dtype, value)?,
            ))),
        },
    )
}

pub fn scalar_value(dtype: DType, value: &Bound<'_, PyAny>) -> PyResult<ScalarValue> {
    match dtype {
        DType::Null => {
            value.downcast::<PyNone>()?;
            Ok(ScalarValue::Null)
        }
        DType::Bool(_) => {
            let value = value.downcast::<PyBool>()?;
            Ok(ScalarValue::Bool(value.extract()?))
        }
        DType::Primitive(ptype, _) => {
            let pvalue = match ptype {
                PType::I8 => PValue::I8(value.extract()?),
                PType::I16 => PValue::I16(value.extract()?),
                PType::I32 => PValue::I32(value.extract()?),
                PType::I64 => PValue::I64(value.extract()?),
                PType::U8 => PValue::U8(value.extract()?),
                PType::U16 => PValue::U16(value.extract()?),
                PType::U32 => PValue::U32(value.extract()?),
                PType::U64 => PValue::U64(value.extract()?),
                PType::F16 => {
                    let float = value.extract::<f32>()?;
                    PValue::F16(f16::from_f32(float))
                }
                PType::F32 => PValue::F32(value.extract()?),
                PType::F64 => PValue::F64(value.extract()?),
            };
            Ok(ScalarValue::Primitive(pvalue))
        }
        DType::Utf8(_) => Ok(ScalarValue::BufferString(value.extract::<String>()?.into())),
        DType::Binary(_) => Ok(ScalarValue::Buffer(value.extract::<&[u8]>()?.into())),
        DType::Struct(..) => todo!(),
        DType::List(element_type, _) => {
            let list = value.downcast::<PyList>();
            let values: Vec<ScalarValue> = list
                .iter()
                .map(|element| scalar_value(element_type.as_ref().clone(), element))
                .collect::<PyResult<Vec<ScalarValue>>>()?;
            Ok(ScalarValue::List(Arc::from(values.into_boxed_slice())))
        }
        DType::Extension(..) => todo!(),
    }
}
