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

use std::fmt::Formatter;

use humansize::{format_size, DECIMAL};

use crate::array::Array;

pub trait ArrayDisplay {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result;
}

pub struct ArrayFormatter<'a, 'b: 'a> {
    fmt: &'a mut Formatter<'b>,
    indent: String,
    total_size: usize,
}

impl<'a, 'b: 'a> ArrayFormatter<'a, 'b> {
    pub fn new(
        fmt: &'a mut Formatter<'b>,
        indent: String,
        total_size: usize,
    ) -> ArrayFormatter<'a, 'b> {
        ArrayFormatter {
            fmt,
            indent,
            total_size,
        }
    }

    pub fn array(&mut self, array: &dyn Array) -> std::fmt::Result {
        self.writeln(format!(
            "{}({}), len={}, nbytes={} ({:.2}%)",
            array.encoding().id(),
            array.dtype(),
            array.len(),
            format_size(array.nbytes(), DECIMAL),
            100f64 * array.nbytes() as f64 / self.total_size as f64
        ))?;
        ArrayDisplay::fmt(array, self)
    }

    pub fn writeln<T: AsRef<str>>(&mut self, str: T) -> std::fmt::Result {
        writeln!(self.fmt, "{}{}", self.indent, str.as_ref())
    }

    pub fn indent<F>(&mut self, indented: F) -> std::fmt::Result
    where
        F: FnOnce(&mut ArrayFormatter) -> std::fmt::Result,
    {
        let original_ident = self.indent.clone();
        self.indent += "  ";
        let res = indented(self);
        self.indent = original_ident;
        res
    }

    pub fn new_total_size<F>(&mut self, total: usize, new_total: F) -> std::fmt::Result
    where
        F: FnOnce(&mut ArrayFormatter) -> std::fmt::Result,
    {
        let original_total = self.total_size;
        self.total_size = total;
        let res = new_total(self);
        self.total_size = original_total;
        res
    }
}

#[cfg(test)]
mod test {
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;

    #[test]
    fn primitive_array() {
        let arr = PrimitiveArray::from_vec((0..100).collect()).boxed();
        assert_eq!(format!("{}", arr), "vortex.primitive(signed_int(32)), len=100, nbytes=400 B (100.00%)\n[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]...\n")
    }
}
