use linkme::distributed_slice;

pub use boolean::*;
use enc::array::{EncodingRef, ENCODINGS};
pub use integer::*;

mod boolean;
mod integer;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ROARING_BOOL: EncodingRef = &RoaringBoolEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ROARING_INT: EncodingRef = &RoaringIntEncoding;
