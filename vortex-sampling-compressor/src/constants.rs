#![allow(dead_code)]
#![allow(unused_imports)]

pub use cost::*;
pub use decompression::*;

mod cost {
    // structural pass-throughs have no cost
    pub const SPARSE_COST: u8 = 0;
    pub const STRUCT_COST: u8 = 0;
    pub const CHUNKED_COST: u8 = 0;

    // so fast that we can ignore the cost
    pub const BITPACKED_NO_PATCHES_COST: u8 = 0;
    pub const BITPACKED_WITH_PATCHES_COST: u8 = 0;
    pub const CONSTANT_COST: u8 = 0;
    pub const ZIGZAG_COST: u8 = 0;

    // "normal" encodings
    pub const ALP_COST: u8 = 1;
    pub const ALP_RD_COST: u8 = 1;
    pub const DATE_TIME_PARTS_COST: u8 = 1;
    pub const DICT_COST: u8 = 1;
    pub const FOR_COST: u8 = 1;
    pub const FSST_COST: u8 = 1;
    pub const ROARING_BOOL_COST: u8 = 1;
    pub const ROARING_INT_COST: u8 = 1;
    pub const RUN_END_COST: u8 = 1;
    pub const DELTA_COST: u8 = 1;
}

mod decompression {
    // structural pass-throughs
    pub const SPARSE_GIB_PER_S: f64 = f64::INFINITY;
    pub const STRUCT_GIB_PER_S: f64 = f64::INFINITY;
    pub const CHUNKED_GIB_PER_S: f64 = f64::INFINITY;

    // benchmarked decompression throughput
    pub const ALP_GIB_PER_S: f64 = 4.9;
    pub const ALP_RD_GIB_PER_S: f64 = 3.3;
    pub const BITPACKED_NO_PATCHES_GIB_PER_S: f64 = 36.0;
    pub const BITPACKED_WITH_PATCHES_GIB_PER_S: f64 = 33.2;
    pub const CONSTANT_GIB_PER_S: f64 = 200.0;
    pub const DATE_TIME_PARTS_GIB_PER_S: f64 = 20.0; // this is a guess
    pub const DELTA_GIB_PER_S: f64 = 4.7;
    pub const DICT_GIB_PER_S: f64 = 10.0;
    pub const FOR_GIB_PER_S: f64 = 8.7;
    pub const FSST_GIB_PER_S: f64 = 2.0;
    pub const ROARING_BOOL_GIB_PER_S: f64 = 5.0;
    pub const ROARING_INT_GIB_PER_S: f64 = 5.0;
    pub const RUN_END_GIB_PER_S: f64 = 10.0;
    pub const ZIGZAG_GIB_PER_S: f64 = 25.5;
}