use linkme::distributed_slice;

pub use boolean::*;
use vortex::array::{EncodingRef, ENCODINGS};
pub use integer::*;

mod boolean;
mod downcast;
mod integer;
mod serde_tests;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ROARING_BOOL: EncodingRef = &RoaringBoolEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ROARING_INT: EncodingRef = &RoaringIntEncoding;
