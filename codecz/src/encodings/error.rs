// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{Codec, CodecFunction};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CodecError {
    #[error("Invalid input in {0}::{1}")]
    InvalidInput(Codec, CodecFunction),
    #[error("Invalid encoding parameter in {0}::{1}")]
    InvalidEncodingParameter(Codec, CodecFunction),
    #[error("Invalid alignment in {0}::{1}")]
    IncorrectAlignment(Codec, CodecFunction),
    #[error("Encoding failed in {0}::{1}")]
    EncodingFailed(Codec, CodecFunction),
    #[error("Provided output buffer is too small in {0}::{1}")]
    OutputBufferTooSmall(Codec, CodecFunction),
    #[error("Out of memory in {0}::{1}")]
    OutOfMemory(Codec, CodecFunction),
    #[error("Out of memory in {0}::{1}")]
    ShouldBeUnreachable(Codec, CodecFunction),
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
            codecz_sys::ResultStatus_t_InvalidEncodingParameter => {
                Some(CodecError::InvalidEncodingParameter(codec, func))
            }
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
            codecz_sys::ResultStatus_t_ShouldBeUnreachable => {
                Some(CodecError::ShouldBeUnreachable(codec, func))
            }
            _ => Some(CodecError::Unknown(codec, func)),
        }
    }
}
