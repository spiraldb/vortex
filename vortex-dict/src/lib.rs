pub use compress::*;
pub use dict::*;
use linkme::distributed_slice;
use vortex::encoding::{EncodingRef, ENCODINGS};

mod compress;
mod compute;
mod dict;
mod downcast;
mod serde;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_DICT: EncodingRef = &DictEncoding;
