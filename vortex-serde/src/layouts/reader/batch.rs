use std::mem;
use std::sync::Arc;

use vortex::{Array, IntoArray};
use vortex::array::StructArray;
use vortex_error::VortexResult;

use crate::layouts::reader::{Layout, ReadResult};

#[derive(Debug)]
pub struct BatchReader {
    names: Arc<[Arc<str>]>,
    children: Vec<Box<dyn Layout>>,
    arrays: Vec<Option<Array>>,
}

impl BatchReader {
    pub fn new(names: Arc<[Arc<str>]>, children: Vec<Box<dyn Layout>>) -> Self {
        let arrays = vec![None; children.len()];
        Self {
            names,
            children,
            arrays,
        }
    }

    pub fn read(&mut self) -> VortexResult<Option<ReadResult>> {
        let mut rr1 = Vec::new();
        for (i, child_array) in self
            .arrays
            .iter_mut()
            .enumerate()
            .filter(|(_, a)| a.is_none())
        {
            match self.children[i].read() {
                Ok(Some(rr)) => match rr {
                    ReadResult::GetMsgs(r1) => {
                        rr1.extend(r1);
                    }
                    ReadResult::Batch(a) => *child_array = Some(a),
                },
                Ok(None) => {
                    debug_assert!(
                        self.arrays.iter().all(|a| a.is_none()),
                        "Expected layout to produce an array but it was empty"
                    );
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }

        if rr1.is_empty() {
            let child_arrays = mem::replace(&mut self.arrays, vec![None; self.children.len()])
                .into_iter()
                .map(|a| a.unwrap());
            return Ok(Some(ReadResult::Batch(
                StructArray::from_fields(&self.names.iter().zip(child_arrays).collect::<Vec<_>>())
                    .into_array(),
            )));
        } else {
            Ok(Some(ReadResult::GetMsgs(rr1)))
        }
    }
}
