use std::mem;
use std::sync::Arc;

use vortex::{Array, IntoArray};
use vortex::array::StructArray;
use vortex_error::VortexResult;

use crate::layouts::{Layout, ReadResult};

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
        let mut rr2 = Vec::new();
        for (i, column_reader) in self.children.iter_mut().enumerate() {
            if self.arrays[i].is_none() {
                match column_reader.read() {
                    Ok(Some(rr)) => match rr {
                        ReadResult::GetMsgs(r1, r2) => {
                            // rewrite the path here
                            rr1.extend(r1);
                            rr2.extend(r2);
                        }
                        ReadResult::Batch(a) => self.arrays[i] = Some(a),
                    },
                    Ok(None) => {
                        if self.arrays.iter().all(|a| a.is_none()) {
                            return Ok(None);
                        }
                        debug_assert!(
                            self.arrays[i].is_some(),
                            "Expected layout to produce an array but it was empty"
                        );
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        if self.arrays.iter().all(|a| a.is_some()) {
            debug_assert!(
                rr1.is_empty() && rr2.is_empty(),
                "Expected read only if there's arrays missing"
            );
            let child_arrays = mem::replace(&mut self.arrays, vec![None; self.children.len()])
                .into_iter()
                .map(|a| a.unwrap());
            return Ok(Some(ReadResult::Batch(
                StructArray::from_fields(&self.names.iter().zip(child_arrays).collect::<Vec<_>>())
                    .into_array(),
            )));
        } else if rr1.is_empty() && rr2.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ReadResult::GetMsgs(rr1, rr2)))
        }
    }
}
