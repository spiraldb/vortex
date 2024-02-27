use vortex::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;

pub use zigzag::*;

mod compress;
mod downcast;
mod serde;
mod stats;
mod zigzag;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ZIGZAG: EncodingRef = &ZigZagEncoding;
