use std::borrow::Cow;
use std::env;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::array::EncodingId;
use crate::dtype::DType;
use crate::ptype::PType;

#[derive(Debug, PartialEq)]
pub struct ErrString(Cow<'static, str>);

impl<T> From<T> for ErrString
where
    T: Into<Cow<'static, str>>,
{
    fn from(msg: T) -> Self {
        if env::var("ENC_PANIC_ON_ERR").as_deref().unwrap_or("") == "1" {
            panic!("{}", msg.into())
        } else {
            ErrString(msg.into())
        }
    }
}

impl AsRef<str> for ErrString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for ErrString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for ErrString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EncError {
    #[error("index {0} out of bounds from {1} to {2}")]
    OutOfBounds(usize, usize, usize),
    #[error("arguments have different lengths")]
    LengthMismatch,
    #[error("{0}")]
    ComputeError(ErrString),
    #[error("invalid data type: {0}")]
    InvalidDType(DType),
    #[error("invalid physical type: {0:?}")]
    InvalidPType(PType),
    #[error("invalid array encoding: {0:?}")]
    InvalidEncoding(EncodingId),
    #[error("can't convert type {0} into {1}")]
    IncompatibleTypes(DType, DType),
    #[error("Expected type {0} but found type {1}")]
    MismatchedTypes(DType, DType),
    #[error("unexpected arrow data type: {0:?}")]
    InvalidArrowDataType(arrow::datatypes::DataType),
    #[error("polars error: {0:?}")]
    PolarsError(PolarsError),
    #[error("arrow error: {0:?}")]
    ArrowError(ArrowError),
    #[error("patch values may not be null for base dtype {0}")]
    NullPatchValuesNotAllowed(DType),
    #[error("unsupported DType {0} for data array")]
    UnsupportedDataArrayDType(DType),
    #[error("unsupported DType {0} for offsets array")]
    UnsupportedOffsetsArrayDType(DType),
    #[error("array containing indices or run ends must be strictly monotonically increasing")]
    IndexArrayMustBeStrictSorted,
}

pub type EncResult<T> = Result<T, EncError>;

// Wrap up external errors so that we can implement a dumb PartialEq
#[derive(Debug)]
pub struct ArrowError(pub arrow::error::ArrowError);

impl PartialEq for ArrowError {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl From<arrow::error::ArrowError> for EncError {
    fn from(err: arrow::error::ArrowError) -> Self {
        EncError::ArrowError(ArrowError(err))
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PolarsError(polars_core::error::PolarsError);

impl PartialEq for PolarsError {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl From<polars_core::error::PolarsError> for EncError {
    fn from(err: polars_core::error::PolarsError) -> Self {
        EncError::PolarsError(PolarsError(err))
    }
}
