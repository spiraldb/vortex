use enc::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;

pub use dict::*;

mod compress;
mod dict;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_DICT: EncodingRef = &DictEncoding;
