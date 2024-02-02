use std::borrow::Cow;
use std::env;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::types::DType;

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
    #[error("invalid dtype: {0}")]
    InvalidDType(DType),
    #[error("can't convert type {0} into {1}")]
    IncompatibleTypes(DType, DType),
    #[error("Expected both arrays to have the same type, found {0} and {1}")]
    MismatchedTypes(DType, DType),
    #[error("unexpected arrow data type: {0:?}")]
    InvalidArrowDataType(arrow::datatypes::DataType),
    #[error("polars error: {0:?}")]
    PolarsError(PolarsError),
    #[error("Malformed patch values, patch index had entry for index {0} but there was no corresponding patch value")]
    MalformedPatches(usize),
}

pub type EncResult<T> = Result<T, EncError>;

// Wrap up PolarsError so that we can implement a dumb PartialEq

#[derive(Debug)]
pub struct PolarsError {
    inner: polars_core::error::PolarsError,
}

impl PolarsError {
    pub fn inner(&self) -> &polars_core::error::PolarsError {
        &self.inner
    }
}

impl PartialEq for PolarsError {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl From<polars_core::error::PolarsError> for EncError {
    fn from(err: polars_core::error::PolarsError) -> Self {
        EncError::PolarsError(PolarsError { inner: err })
    }
}
