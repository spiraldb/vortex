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

use cast::CastPrimitiveFn;
use patch::PatchFn;
use take::TakeFn;

pub mod add;
pub mod as_contiguous;
pub mod cast;
pub mod patch;
pub mod repeat;
pub mod search_sorted;
pub mod take;

pub trait ArrayCompute {
    fn cast_primitive(&self) -> Option<&dyn CastPrimitiveFn> {
        None
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        None
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }
}
