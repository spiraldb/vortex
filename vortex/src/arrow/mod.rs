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

use arrow::array::ArrayRef;
use itertools::Itertools;

use crate::array::ArrowIterator;

pub mod aligned_iter;
pub mod compute;
pub mod convert;

pub trait CombineChunks {
    fn combine_chunks(self) -> ArrayRef;
}

impl CombineChunks for Box<ArrowIterator> {
    fn combine_chunks(self) -> ArrayRef {
        let chunks = self.collect_vec();
        let chunk_refs = chunks.iter().map(|a| a.as_ref()).collect_vec();
        arrow::compute::concat(&chunk_refs).unwrap()
    }
}

#[macro_export]
macro_rules! match_arrow_numeric_type {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::dtype::DType::*;
        use $crate::dtype::IntWidth::*;
        use $crate::dtype::Signedness::*;
        use $crate::dtype::FloatWidth;
        use arrow::datatypes::*;
        match $self {
            Int(_8, Unsigned, _) => __with__! {UInt8Type},
            Int(_16, Unsigned, _) => __with__!{UInt16Type},
            Int(_32, Unsigned, _) => __with__!{UInt32Type},
            Int(_64, Unsigned, _) => __with__!{UInt64Type},
            Int(_8, Signed, _) => __with__! {Int8Type},
            Int(_16, Signed, _) => __with__!{Int16Type},
            Int(_32, Signed, _) => __with__!{Int32Type},
            Int(_64, Signed, _) => __with__!{Int64Type},
            Float(FloatWidth::_16, _) => __with__!{Float16Type},
            Float(FloatWidth::_32, _) => __with__!{Float32Type},
            Float(FloatWidth::_64, _) => __with__!{Float64Type},
            _ => unimplemented!("Convert this DType to ArrowPrimitiveType")
        }
    })
}

pub use match_arrow_numeric_type;
