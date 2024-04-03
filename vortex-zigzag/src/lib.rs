use linkme::distributed_slice;
use vortex::encoding::{EncodingRef, ENCODINGS};

pub use zigzag::*;

mod compress;
mod compute;
mod downcast;
mod serde;
mod stats;
mod zigzag;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ZIGZAG: EncodingRef = &ZigZagEncoding;
