use std::mem;
use std::sync::Arc;

use vortex::array::{BoolArray, StructArray};
use vortex::compute::and;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_error::{vortex_err, VortexResult};

use crate::layouts::read::{LayoutReader, ReadResult};

#[derive(Debug)]
pub struct ColumnsReader {
    names: Arc<[Arc<str>]>,
    children: Vec<Box<dyn LayoutReader>>,
    arrays: Vec<Option<Array>>,
    selections: Vec<Option<BoolArray>>,
}

impl ColumnsReader {
    pub fn new(names: Arc<[Arc<str>]>, children: Vec<Box<dyn LayoutReader>>) -> Self {
        let arrays = vec![None; children.len()];
        let selections = vec![None; children.len()];

        Self {
            names,
            children,
            arrays,
            selections,
        }
    }

    pub(crate) fn read(&mut self) -> VortexResult<Option<ReadResult>> {
        let mut messages = Vec::new();
        for (i, child_array) in self
            .arrays
            .iter_mut()
            .enumerate()
            .filter(|(_, a)| a.is_none())
        {
            match self.children[i].read_next()? {
                Some(rr) => match rr {
                    ReadResult::ReadMore(message) => {
                        messages.extend(message);
                    }
                    ReadResult::Batch(a) => *child_array = Some(a),
                    ReadResult::Selection(_) => unreachable!(""),
                },
                None => {
                    debug_assert!(
                        self.arrays.iter().all(Option::is_none),
                        "Expected layout to produce an array but it was empty"
                    );
                    return Ok(None);
                }
            }
        }

        if messages.is_empty() {
            let child_arrays = mem::replace(&mut self.arrays, vec![None; self.children.len()])
                .into_iter()
                .enumerate()
                .map(|(i, a)| a.ok_or_else(|| vortex_err!("Missing child array at index {}", i)))
                .collect::<VortexResult<Vec<_>>>()?;

            return Ok(Some(ReadResult::Batch(
                StructArray::from_fields(&self.names.iter().zip(child_arrays).collect::<Vec<_>>())
                    .into_array(),
            )));
        } else {
            Ok(Some(ReadResult::ReadMore(messages)))
        }
    }

    pub(crate) fn eval_selection(&mut self) -> VortexResult<Option<ReadResult>> {
        let mut messages = Vec::new();
        for (i, child_selection) in self
            .selections
            .iter_mut()
            .enumerate()
            .filter(|(_, a)| a.is_none())
        {
            match self.children[i].eval_selection(None)? {
                Some(rr) => match rr {
                    ReadResult::ReadMore(message) => {
                        messages.extend(message);
                    }
                    ReadResult::Selection(selection) => *child_selection = Some(selection),
                    ReadResult::Batch(_) => unreachable!(""),
                },
                None => {
                    debug_assert!(
                        self.selections.iter().all(Option::is_none),
                        "Expected layout to produce an array but it was empty"
                    );
                    return Ok(None);
                }
            }
        }

        if messages.is_empty() {
            let child_arrays = mem::replace(&mut self.selections, vec![None; self.children.len()])
                .into_iter()
                .enumerate()
                .map(|(i, a)| {
                    a.ok_or_else(|| vortex_err!("Missing child selection at index {}", i))
                })
                .collect::<VortexResult<Vec<_>>>()?;

            let len = child_arrays.first().map(|a| a.len()).unwrap();
            let mut base = BoolArray::from(vec![true; len]);

            for arr in child_arrays.into_iter() {
                base = and(&base, arr)?.into_bool()?;
            }

            Ok(Some(ReadResult::Selection(base)))
        } else {
            Ok(Some(ReadResult::ReadMore(messages)))
        }
    }
}
