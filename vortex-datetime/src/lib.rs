#![feature(trait_upcasting)]
pub use datetime::*;
use linkme::distributed_slice;
use vortex::encoding::{EncodingRef, ENCODINGS};

mod compress;
mod compute;
mod datetime;
mod serde;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_DATETIME: EncodingRef = &DateTimeEncoding;
