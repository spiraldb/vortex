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
    #[error("invalid dtype: {0:?}")]
    InvalidDType(DType),

    #[error("unexpected arrow data type: {0:?}")]
    InvalidArrowDataType(arrow2::datatypes::DataType),
}

pub type EncResult<T> = Result<T, EncError>;
