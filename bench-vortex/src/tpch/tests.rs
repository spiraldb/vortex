use vortex::{IntoArray, IntoCanonical};
use vortex_sampling_compressor::SamplingCompressor;

use crate::tpch::dbgen::{DBGen, DBGenOptions};
use crate::tpch::load_table;
use crate::tpch::schema::LINEITEM;

#[test]
fn test_thing() {
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let lineitem_vortex = rt.block_on(load_table(data_dir, "lineitem", &LINEITEM));

    let comments = lineitem_vortex.with_dyn(|a| {
        a.as_struct_array_unchecked()
            .field_by_name("l_comment")
            .unwrap()
    });

    let comments_canonical = comments
        .into_canonical()
        .unwrap()
        .into_varbin()
        .unwrap()
        .into_array();

    crate::setup_logger(log::LevelFilter::Debug);
    let compressor_fsst = SamplingCompressor::default();
    let compressed = compressor_fsst.compress(&comments_canonical, None).unwrap();
    println!("END compressed = {}", compressed.array().tree_display());
}
