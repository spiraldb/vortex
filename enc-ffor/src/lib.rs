use enc::array::{EncodingRef, ENCODINGS};
pub use ffor::*;
use linkme::distributed_slice;

mod compress;
mod ffor;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FFOR: EncodingRef = &FFoREncoding;
