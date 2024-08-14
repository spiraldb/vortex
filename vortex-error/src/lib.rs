#![feature(error_generic_member_access)]

use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::{env, fmt, io};

#[derive(Debug)]
pub struct ErrString(Cow<'static, str>);

#[allow(clippy::fallible_impl_from)]
impl<T> From<T> for ErrString
where
    T: Into<Cow<'static, str>>,
{
    fn from(msg: T) -> Self {
        if env::var("VORTEX_PANIC_ON_ERR").as_deref().unwrap_or("") == "1" {
            panic!("{}", msg.into())
        } else {
            Self(msg.into())
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
        Display::fmt(&self.0, f)
    }
}

#[derive(thiserror::Error)]
pub enum VortexError {
    #[error("index {0} out of bounds from {1} to {2}\nBacktrace:\n{3}")]
    OutOfBounds(usize, usize, usize, Backtrace),
    #[error("{0}\nBacktrace:\n{1}")]
    ComputeError(ErrString, Backtrace),
    #[error("{0}\nBacktrace:\n{1}")]
    InvalidArgument(ErrString, Backtrace),
    #[error("{0}\nBacktrace:\n{1}")]
    InvalidSerde(ErrString, Backtrace),
    #[error("function {0} not implemented for {1}\nBacktrace:\n{2}")]
    NotImplemented(ErrString, ErrString, Backtrace),
    #[error("expected type: {0} but instead got {1}\nBacktrace:\n{2}")]
    MismatchedTypes(ErrString, ErrString, Backtrace),
    #[error("{0}: {1}")]
    Context(ErrString, Box<VortexError>),
    #[error(transparent)]
    ArrowError(
        #[from]
        #[backtrace]
        arrow_schema::ArrowError,
    ),
    #[cfg(feature = "flatbuffers")]
    #[error(transparent)]
    FlatBuffersError(
        #[from]
        #[backtrace]
        flatbuffers::InvalidFlatbuffer,
    ),
    #[cfg(feature = "flexbuffers")]
    #[error(transparent)]
    FlexBuffersReaderError(
        #[from]
        #[backtrace]
        flexbuffers::ReaderError,
    ),
    #[cfg(feature = "flexbuffers")]
    #[error(transparent)]
    FlexBuffersDeError(
        #[from]
        #[backtrace]
        flexbuffers::DeserializationError,
    ),
    #[cfg(feature = "flexbuffers")]
    #[error(transparent)]
    FlexBuffersSerError(
        #[from]
        #[backtrace]
        flexbuffers::SerializationError,
    ),
    #[error(transparent)]
    FmtError(
        #[from]
        #[backtrace]
        std::fmt::Error,
    ),
    #[error(transparent)]
    IOError(
        #[from]
        #[backtrace]
        io::Error,
    ),
    #[error(transparent)]
    Utf8Error(
        #[from]
        #[backtrace]
        std::str::Utf8Error,
    ),
    #[cfg(feature = "parquet")]
    #[error(transparent)]
    ParquetError(
        #[from]
        #[backtrace]
        parquet::errors::ParquetError,
    ),
    #[error(transparent)]
    TryFromSliceError(
        #[from]
        #[backtrace]
        std::array::TryFromSliceError,
    ),
    #[cfg(feature = "worker")]
    #[error(transparent)]
    WorkerError(
        #[from]
        #[backtrace]
        worker::Error,
    ),
    #[cfg(feature = "object_store")]
    #[error(transparent)]
    ObjectStore(
        #[from]
        #[backtrace]
        object_store::Error,
    ),
    #[cfg(feature = "datafusion")]
    #[error(transparent)]
    DataFusion(
        #[from]
        #[backtrace]
        datafusion_common::DataFusionError,
    ),
}

pub type VortexResult<T> = Result<T, VortexError>;

impl Debug for VortexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[macro_export]
macro_rules! vortex_err {
    (OutOfBounds: $idx:expr, $start:expr, $stop:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::OutOfBounds($idx, $start, $stop, Backtrace::capture())
        )
    }};
    (NotImplemented: $func:expr, $by_whom:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::NotImplemented($func.into(), format!("{}", $by_whom).into(), Backtrace::capture())
        )
    }};
    (MismatchedTypes: $expected:literal, $actual:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::MismatchedTypes($expected.into(), $actual.to_string().into(), Backtrace::capture())
        )
    }};
    (MismatchedTypes: $expected:expr, $actual:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::MismatchedTypes($expected.to_string().into(), $actual.to_string().into(), Backtrace::capture())
        )
    }};
    (Context: $fmt:literal $(, $arg:expr)*, $err:expr $(,)?) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::Context(format!($fmt, $($arg),*).into(), Box::new($err))
        )
    }};
    ($variant:ident: $fmt:literal $(, $arg:expr)* $(,)?) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::$variant(format!($fmt, $($arg),*).into(), Backtrace::capture())
        )
    }};
    ($variant:ident: $err:expr $(,)?) => {
        $crate::__private::must_use(
            $crate::VortexError::$variant($err)
        )
    };
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::vortex_err!(InvalidArgument: $fmt, $($arg),*)
    };
}

#[macro_export]
macro_rules! vortex_bail {
    ($($tt:tt)+) => {
        return Err($crate::vortex_err!($($tt)+))
    };
}

#[cfg(feature = "datafusion")]
impl From<VortexError> for datafusion_common::DataFusionError {
    fn from(value: VortexError) -> Self {
        Self::External(Box::new(value))
    }
}

#[cfg(feature = "datafusion")]
impl From<VortexError> for datafusion_common::arrow::error::ArrowError {
    fn from(value: VortexError) -> Self {
        match value {
            VortexError::ArrowError(e) => e,
            _ => Self::from_external_error(Box::new(value)),
        }
    }
}

// Not public, referenced by macros only.
#[doc(hidden)]
pub mod __private {
    #[doc(hidden)]
    #[inline]
    #[cold]
    #[must_use]
    pub const fn must_use(error: crate::VortexError) -> crate::VortexError {
        error
    }
}

#[cfg(feature = "worker")]
impl From<VortexError> for worker::Error {
    fn from(value: VortexError) -> Self {
        Self::RustError(value.to_string())
    }
}
