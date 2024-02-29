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

use crate::dtype::{DType, Nullability};
use crate::error::{VortexError, VortexResult};
use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Utf8Scalar {
    value: String,
}

impl Utf8Scalar {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &str {
        self.value.as_str()
    }
}

impl Scalar for Utf8Scalar {
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
        Some(self)
    }

    #[inline]
    fn into_nonnull(self: Box<Self>) -> Option<Box<dyn Scalar>> {
        Some(self)
    }

    #[inline]
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Utf8(Nullability::NonNullable)
    }

    fn cast(&self, _dtype: &DType) -> VortexResult<Box<dyn Scalar>> {
        todo!()
    }

    fn nbytes(&self) -> usize {
        self.value.len()
    }
}

impl From<String> for Box<dyn Scalar> {
    fn from(value: String) -> Self {
        Utf8Scalar::new(value).boxed()
    }
}

impl From<&str> for Box<dyn Scalar> {
    fn from(value: &str) -> Self {
        Utf8Scalar::new(value.to_string()).boxed()
    }
}

impl TryFrom<Box<dyn Scalar>> for String {
    type Error = VortexError;

    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        let dtype = value.dtype().clone();
        let scalar = value
            .into_any()
            .downcast::<Utf8Scalar>()
            .map_err(|_| VortexError::InvalidDType(dtype))?;
        Ok(scalar.value)
    }
}

impl TryFrom<&dyn Scalar> for String {
    type Error = VortexError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        if let Some(scalar) = value.as_any().downcast_ref::<Utf8Scalar>() {
            Ok(scalar.value().to_string())
        } else {
            Err(VortexError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl Display for Utf8Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}
