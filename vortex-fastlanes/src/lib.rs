use linkme::distributed_slice;

pub use bitpacking::*;
pub use r#for::*;
use vortex::array::{EncodingRef, ENCODINGS};

mod bitpacking;
mod r#for;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_BITPACKING: EncodingRef = &BitPackedEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_FOR: EncodingRef = &FoREncoding;
