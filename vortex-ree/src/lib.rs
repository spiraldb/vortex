use linkme::distributed_slice;
use vortex::encoding::{EncodingRef, ENCODINGS};

pub use ree::*;

mod compress;
mod compute;
mod downcast;
mod ree;
mod serde;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_REE: EncodingRef = &REEEncoding;
