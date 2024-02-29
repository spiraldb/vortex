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

use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::scalar::Scalar;

pub fn repeat(scalar: &dyn Scalar, n: usize) -> ArrayRef {
    ConstantArray::new(dyn_clone::clone_box(scalar), n).boxed()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_repeat() {
        let scalar: Box<dyn Scalar> = 47.into();
        let array = repeat(scalar.as_ref(), 100);
        assert_eq!(array.len(), 100);
    }
}
