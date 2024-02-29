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

use crate::dtype::{DType, Nullability, TimeUnit};
use crate::error::VortexResult;
use crate::scalar::{PScalar, Scalar};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct LocalTimeScalar {
    value: PScalar,
    dtype: DType,
}

impl LocalTimeScalar {
    pub fn new(value: PScalar, unit: TimeUnit) -> Self {
        Self {
            value,
            dtype: DType::LocalTime(unit, Nullability::NonNullable),
        }
    }

    pub fn value(&self) -> &PScalar {
        &self.value
    }

    pub fn time_unit(&self) -> TimeUnit {
        let DType::LocalTime(u, _) = self.dtype else {
            unreachable!("unexpected dtype")
        };
        u
    }
}

impl Scalar for LocalTimeScalar {
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
        &self.dtype
    }

    fn cast(&self, _dtype: &DType) -> VortexResult<Box<dyn Scalar>> {
        todo!()
    }

    fn nbytes(&self) -> usize {
        self.value.nbytes()
    }
}

impl PartialOrd for LocalTimeScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.dtype() != other.dtype() {
            None
        } else {
            self.value.partial_cmp(&other.value)
        }
    }
}

impl Display for LocalTimeScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let DType::LocalTime(u, _) = self.dtype() else {
            unreachable!()
        };
        write!(f, "localtime[{}, unit={}]", self.value, u)
    }
}
