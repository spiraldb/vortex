use super::{Codec, CodecFunction};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CodecError {
    #[error("Invalid input in {0}::{1}")]
    InvalidInput(Codec, CodecFunction),
    #[error("Invalid alignment in {0}::{1}")]
    IncorrectAlignment(Codec, CodecFunction),
    #[error("Encoding failed in {0}::{1}")]
    EncodingFailed(Codec, CodecFunction),
    #[error("Provided output buffer is too small in {0}::{1}")]
    OutputBufferTooSmall(Codec, CodecFunction),
    #[error("Out of memory in {0}::{1}")]
    OutOfMemory(Codec, CodecFunction),
    #[error("Unknown codec error in {0}::{1}")]
    Unknown(Codec, CodecFunction),
}

impl CodecError {
    pub fn parse_error(
        status: codecz_sys::ResultStatus_t,
        codec: Codec,
        func: CodecFunction,
    ) -> Option<CodecError> {
        match status {
            codecz_sys::ResultStatus_t_Ok => None,
            codecz_sys::ResultStatus_t_InvalidInput => Some(CodecError::InvalidInput(codec, func)),
            codecz_sys::ResultStatus_t_IncorrectAlignment => {
                Some(CodecError::IncorrectAlignment(codec, func))
            }
            codecz_sys::ResultStatus_t_EncodingFailed => {
                Some(CodecError::EncodingFailed(codec, func))
            }
            codecz_sys::ResultStatus_t_OutputBufferTooSmall => {
                Some(CodecError::OutputBufferTooSmall(codec, func))
            }
            codecz_sys::ResultStatus_t_OutOfMemory => Some(CodecError::OutOfMemory(codec, func)),
            _ => Some(CodecError::Unknown(codec, func)),
        }
    }
}
