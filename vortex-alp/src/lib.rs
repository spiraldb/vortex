pub use alp::*;
pub use array::*;

use linkme::distributed_slice;
use vortex::encoding::{EncodingRef, ENCODINGS};

mod alp;
mod array;
mod compress;
mod compute;
mod downcast;
mod serde;
mod stats;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_ALP: EncodingRef = &ALPEncoding;
