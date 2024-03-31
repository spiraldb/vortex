use crate::encoding::{EncodingId, EncodingRef, ENCODINGS};
use itertools::Itertools;
use std::sync::Arc;

// TODO(ngates): come up with a better name
pub struct ViewContext {
    encodings: Arc<[EncodingRef]>,
}

impl ViewContext {
    pub fn new(encodings: Arc<[EncodingRef]>) -> Self {
        Self { encodings }
    }

    pub fn encodings(&self) -> &[EncodingRef] {
        self.encodings.as_ref()
    }

    pub fn find_encoding(&self, encoding_id: u16) -> Option<EncodingRef> {
        self.encodings.get(encoding_id as usize).cloned()
    }

    pub fn encoding_idx(&self, encoding_id: EncodingId) -> Option<u16> {
        self.encodings
            .iter()
            .position(|e| e.id() == encoding_id)
            .map(|i| i as u16)
    }
}

impl Default for ViewContext {
    fn default() -> Self {
        Self {
            encodings: ENCODINGS.iter().cloned().collect_vec().into(),
        }
    }
}
