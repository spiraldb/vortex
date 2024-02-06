use enc::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;

pub use ree::*;

mod compress;
mod ree;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_REE: EncodingRef = &REEEncoding;
