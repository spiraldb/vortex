#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use std::mem::size_of;

use num_traits::{PrimInt, Unsigned};

mod bitpacking;
pub use bitpacking::*;

pub const ORDER: [u8; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

pub trait FastLanes: Sized + Unsigned + PrimInt {
    const T: usize = size_of::<Self>() * 8;
    const LANES: usize = 1024 / Self::T;
}

pub struct Pred<const B: bool>;

pub trait Satisfied {}

impl Satisfied for Pred<true> {}
