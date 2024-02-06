use enc::array::{EncodingRef, ENCODINGS};
use linkme::distributed_slice;
pub use patched::*;

mod compress;
mod patched;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_PATCHED: EncodingRef = &PatchedEncoding;
