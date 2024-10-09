#![allow(dead_code)]
pub mod decompression {
    pub const ALP_GIB_PER_S: f64 = 4.9;
    pub const ALP_RD_GIB_PER_S: f64 = 3.3;
    pub const BITPACKED_NO_PATCHES_GIB_PER_S: f64 = 36.0;
    pub const BITPACKED_WITH_PATCHES_GIB_PER_S: f64 = 33.2;
    pub const CONSTANT_GIB_PER_S: f64 = 100.0; // memcopy
    pub const DATE_TIME_PARTS_GIB_PER_S: f64 = 20.0; // this is a wild guess
    pub const DELTA_GIB_PER_S: f64 = 4.7;
    pub const DICT_GIB_PER_S: f64 = 10.0;
    pub const FOR_GIB_PER_S: f64 = 8.7;
    pub const FSST_GIB_PER_S: f64 = 2.0;
    pub const ROARING_BOOL_GIB_PER_S: f64 = 5.0;
    pub const ROARING_INT_GIB_PER_S: f64 = 5.0;
    pub const RUN_END_GIB_PER_S: f64 = 10.0;
    pub const SPARSE_GIB_PER_S: f64 = 100.0; // memcopy
    pub const ZIGZAG_GIB_PER_S: f64 = 25.5;
}

pub mod depth {
    pub const ALP_COST: u8 = 1;
    pub const ALP_RD_COST: u8 = 1;
    pub const BITPACKED_NO_PATCHES_COST: u8 = 0;
    pub const BITPACKED_WITH_PATCHES_COST: u8 = 0;
    pub const CONSTANT_COST: u8 = 0;
    pub const DATE_TIME_PARTS_COST: u8 = 1;
    pub const DELTA_COST: u8 = 1;
    pub const DICT_COST: u8 = 1;
    pub const FOR_COST: u8 = 1;
    pub const FSST_COST: u8 = 1;
    pub const ROARING_BOOL_COST: u8 = 1;
    pub const ROARING_INT_COST: u8 = 1;
    pub const RUN_END_COST: u8 = 1;
    pub const SPARSE_COST: u8 = 0;
    pub const ZIGZAG_COST: u8 = 1;
}
