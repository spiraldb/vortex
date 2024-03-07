use linkme::distributed_slice;

pub use bitpacking::*;
pub use ffor::*;
pub use r#for::*;
use vortex::array::{EncodingRef, ENCODINGS};

mod bitpacking;
mod ffor;
mod r#for;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_BITPACKING: EncodingRef = &BitPackedEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_FOR: EncodingRef = &FoREncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_FFOR: EncodingRef = &FFoREncoding;
