pub use alp::*;
use linkme::distributed_slice;
use vortex::array::{EncodingRef, ENCODINGS};

mod alp;
mod compress;
mod downcast;
mod serde;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ALP: EncodingRef = &ALPEncoding;
