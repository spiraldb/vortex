use linkme::distributed_slice;

pub use datetime::*;
use vortex::encoding::{EncodingRef, ENCODINGS};

mod compress;
mod compute;
mod datetime;
mod serde;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_DATETIME: EncodingRef = &DateTimeEncoding;
