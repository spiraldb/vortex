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

use std::any::Any;
use std::fmt::{Display, Formatter};

use crate::dtype::DType;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar};

#[derive(Debug, Clone, PartialEq)]
pub struct NullScalar;

impl Default for NullScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl NullScalar {
    #[inline]
    pub fn new() -> Self {
        Self {}
    }
}

impl Scalar for NullScalar {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    #[inline]
    fn as_nonnull(&self) -> Option<&dyn Scalar> {
        None
    }

    #[inline]
    fn into_nonnull(self: Box<Self>) -> Option<Box<dyn Scalar>> {
        None
    }

    #[inline]
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Null
    }

    fn cast(&self, dtype: &DType) -> VortexResult<Box<dyn Scalar>> {
        Ok(NullableScalar::none(dtype.clone()).boxed())
    }

    fn nbytes(&self) -> usize {
        1
    }
}

impl Display for NullScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}
