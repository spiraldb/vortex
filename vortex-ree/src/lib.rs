use linkme::distributed_slice;
pub use ree::*;
use vortex::encoding::{EncodingRef, ENCODINGS};

mod compress;
mod compute;
mod downcast;
mod ree;
mod serde;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_REE: EncodingRef = &REEEncoding;
