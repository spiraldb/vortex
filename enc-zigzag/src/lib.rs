use enc::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;

pub use zigzag::*;

mod compress;
mod serde;
mod stats;
mod zigzag;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ZIGZAG: EncodingRef = &ZigZagEncoding;
