use linkme::distributed_slice;
use vortex::array::{EncodingRef, ENCODINGS};

pub use compress::*;
pub use dict::*;

mod compress;
mod dict;
mod downcast;
mod serde;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_DICT: EncodingRef = &DictEncoding;
