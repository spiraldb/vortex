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

use enum_display::EnumDisplay;

mod error;
pub use error::CodecError;
pub mod alp;
pub mod ffor;
pub mod ree;

pub use vortex_alloc::{AlignedAllocator, AlignedVec, ALIGNED_ALLOCATOR, VORTEX_ALIGNMENT};

pub(crate) type ByteBuffer = codecz_sys::ByteBuffer_t;
pub(crate) type WrittenBuffer = codecz_sys::WrittenBuffer_t;
pub(crate) type OneBufferResult = codecz_sys::OneBufferResult_t;
pub(crate) type TwoBufferResult = codecz_sys::TwoBufferResult_t;

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum Codec {
    ALP,
    FFoR,
    REE,
    ZigZag,
}

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum CodecFunction {
    Prelude,
    Encode,
    Decode,
    CollectExceptions,
    EncodeSingle,
    DecodeSingle,
}

mod test {
    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_alignment() {
        assert_eq!(
            codecz_sys::VORTEX_ALIGNMENT as usize,
            vortex_alloc::VORTEX_ALIGNMENT,
        );
        assert_eq!(
            codecz_sys::VORTEX_ALIGNMENT as usize,
            super::ALIGNED_ALLOCATOR.min_alignment(),
        );
        assert!(arrow_buffer::alloc::ALIGNMENT >= 64);
    }
}
