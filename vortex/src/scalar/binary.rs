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

use crate::dtype::{DType, Nullability};
use crate::error::{VortexError, VortexResult};
use crate::scalar::Scalar;
use std::any::Any;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct BinaryScalar {
    value: Vec<u8>,
}

impl BinaryScalar {
    pub fn new(value: Vec<u8>) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.value
    }
}

impl Scalar for BinaryScalar {
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
        &DType::Binary(Nullability::NonNullable)
    }

    fn cast(&self, _dtype: &DType) -> VortexResult<Box<dyn Scalar>> {
        todo!()
    }

    fn nbytes(&self) -> usize {
        self.value.len()
    }
}

impl From<Vec<u8>> for Box<dyn Scalar> {
    fn from(value: Vec<u8>) -> Self {
        BinaryScalar::new(value).boxed()
    }
}

impl TryFrom<Box<dyn Scalar>> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        let dtype = value.dtype().clone();
        let scalar = value
            .into_any()
            .downcast::<BinaryScalar>()
            .map_err(|_| VortexError::InvalidDType(dtype))?;
        Ok(scalar.value)
    }
}

impl TryFrom<&dyn Scalar> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        if let Some(scalar) = value.as_any().downcast_ref::<BinaryScalar>() {
            Ok(scalar.value.clone())
        } else {
            Err(VortexError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl Display for BinaryScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "bytes[{}]", self.value.len())
    }
}
