use linkme::distributed_slice;
use vortex::array::{EncodingRef, ENCODINGS};

pub use ree::*;

mod compress;
mod downcast;
mod ree;
mod serde;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_REE: EncodingRef = &REEEncoding;
