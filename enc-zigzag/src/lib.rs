use enc::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;

pub use zigzag::*;

mod compress;
mod stats;
mod zigzag;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ROARING_BOOL: EncodingRef = &ZigZagEncoding;
