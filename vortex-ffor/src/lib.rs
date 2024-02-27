pub use ffor::*;
use linkme::distributed_slice;
use vortex::array::{EncodingRef, ENCODINGS};

mod compress;
mod downcast;
mod ffor;
mod serde;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FFOR: EncodingRef = &FFoREncoding;
