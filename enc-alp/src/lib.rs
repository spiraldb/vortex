pub use alp::*;
use enc::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;

mod alp;
mod compress;
mod helpers;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ALP: EncodingRef = &ALPEncoding;
