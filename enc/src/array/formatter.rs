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
            "{}({}), len={}, nbytes={} ({})",
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
}
