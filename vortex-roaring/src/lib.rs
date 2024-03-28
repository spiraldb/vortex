use linkme::distributed_slice;

pub use boolean::*;
pub use integer::*;
use vortex::encoding::{EncodingRef, ENCODINGS};

mod boolean;
mod downcast;
mod integer;
mod serde_tests;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ROARING_BOOL: EncodingRef = &RoaringBoolEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ROARING_INT: EncodingRef = &RoaringIntEncoding;
