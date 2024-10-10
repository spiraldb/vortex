#![allow(dead_code)]
pub mod cost {
    // structural pass-throughs have no cost
    pub const SPARSE_COST: u8 = 0;
    // TODO: struct
    // TODO: chunked

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

    // "expensive" encodings
    pub const DELTA_COST: u8 = 2;
}
