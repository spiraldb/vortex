use std::fmt;

use humansize::{format_size, DECIMAL};
use serde::ser::Error;
use vortex_error::{VortexError, VortexResult};

use crate::buffer::Buffer;
use crate::visitor::ArrayVisitor;
use crate::{Array, ToArrayData};

impl Array<'_> {
    pub fn tree_display(&self) -> TreeDisplayWrapper {
        TreeDisplayWrapper(self)
    }
}

pub struct TreeDisplayWrapper<'a>(&'a Array<'a>);
impl<'a> TreeDisplayWrapper<'a> {
    pub fn new(array: &'a Array<'a>) -> Self {
        Self(array)
    }
}

impl<'a, 'fmt: 'a> fmt::Display for TreeDisplayWrapper<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let array = self.0;
        let nbytes = array.with_dyn(|a| a.nbytes());
        let mut array_fmt = TreeFormatter::new(f, "".to_string(), nbytes);
        array_fmt
            .visit_child("root", array)
            .map_err(fmt::Error::custom)
    }
}

pub struct TreeFormatter<'a, 'b: 'a> {
    fmt: &'a mut fmt::Formatter<'b>,
    indent: String,
    total_size: usize,
}

/// TODO(ngates): I think we want to go back to the old explicit style. It gives arrays more
///  control over how their metadata etc is displayed.
impl<'a, 'b: 'a> ArrayVisitor for TreeFormatter<'a, 'b> {
    fn visit_child(&mut self, name: &str, array: &Array) -> VortexResult<()> {
        array.with_dyn(|a| {
            let nbytes = a.nbytes();
            writeln!(
                self.fmt,
                "{}{}: {} nbytes={} ({:.2}%)",
                self.indent,
                name,
                array,
                format_size(nbytes, DECIMAL),
                100f64 * nbytes as f64 / self.total_size as f64
            )?;
            self.indent(|i| {
                writeln!(
                    i.fmt,
                    // TODO(ngates): use Display for metadata
                    "{}metadata: {:?}",
                    i.indent,
                    array.to_array_data().metadata()
                )
            })?;
            self.indent(|i| a.accept(i).map_err(fmt::Error::custom))
                .map_err(VortexError::from)
        })
    }

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
        Ok(writeln!(
            self.fmt,
            "{}buffer: {}",
            self.indent,
            format_size(buffer.len(), DECIMAL)
        )?)
    }
}

impl<'a, 'b: 'a> TreeFormatter<'a, 'b> {
    fn new(
        fmt: &'a mut fmt::Formatter<'b>,
        indent: String,
        total_size: usize,
    ) -> TreeFormatter<'a, 'b> {
        TreeFormatter {
            fmt,
            indent,
            total_size,
        }
    }

    fn indent<F>(&mut self, indented: F) -> fmt::Result
    where
        F: FnOnce(&mut TreeFormatter) -> fmt::Result,
    {
        let original_ident = self.indent.clone();
        self.indent += "  ";
        let res = indented(self);
        self.indent = original_ident;
        res
    }

    #[allow(dead_code)]
    pub fn new_total_size<F>(&mut self, total: usize, new_total: F) -> fmt::Result
    where
        F: FnOnce(&mut TreeFormatter) -> fmt::Result,
    {
        let original_total = self.total_size;
        self.total_size = total;
        let res = new_total(self);
        self.total_size = original_total;
        res
    }
}
