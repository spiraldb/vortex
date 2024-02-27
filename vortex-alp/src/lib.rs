pub use alp::*;
use vortex::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;

mod alp;
mod compress;
mod downcast;
mod serde;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ALP: EncodingRef = &ALPEncoding;
