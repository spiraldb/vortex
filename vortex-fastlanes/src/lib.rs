#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

pub use bitpacking::*;
pub use delta::*;
use linkme::distributed_slice;
pub use r#for::*;
use vortex::encoding::{EncodingRef, ENCODINGS};

mod bitpacking;
mod delta;
mod downcast;
mod r#for;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_BITPACKING: EncodingRef = &BitPackedEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_DELTA: EncodingRef = &DeltaEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_FOR: EncodingRef = &FoREncoding;
