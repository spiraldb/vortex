use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_csv::reader::Format;
use itertools::Itertools;
use vortex::array::chunked::ChunkedArray;
use vortex::array::IntoArray;
use vortex::arrow::FromArrowType;
use vortex::serde::WriteCtx;
use vortex_schema::DType;

use crate::data_downloads::{decompress_bz2, download_data};
use crate::idempotent;
use crate::reader::{compress_csv_to_vortex, default_csv_format};

#[allow(dead_code)]
pub fn medicare_data_csv() -> PathBuf {
    let fname = "Medicare1_1.csv.bz2";
    download_data(
        fname,
        "http://www.cwi.nl/~boncz/PublicBIbenchmark/Medicare1/Medicare1_1.csv.bz2",
    );
    decompress_bz2(
        "/Users/jcasale/fulcrum/vortex/bench-vortex/data/Medicare1_1.csv.bz2",
        "Medicare1_1.csv",
    )
}

// TODO(@jdcasale): figure out how to read csv into lance
// pub fn medicare_data_lance() -> PathBuf {
//     idempotent("taxi.lance", |path| {
//
//         let csv_file = File::open(medicare_data_csv()).unwrap();
//         let reader = BufReader::new(csv_file.try_clone().unwrap());
//
//         let (schema, _) = Format::default().with_delimiter(u8::try_from('|').unwrap()).infer_schema(&mut csv_file.try_clone().unwrap(), None).unwrap();
//
//         let csv_reader = arrow::csv::ReaderBuilder::new(
//             Arc::new(schema.clone()))
//             .with_batch_size(crate::reader::BATCH_SIZE)
//             .build(reader)
//             .unwrap();
//         let write_params = WriteParams::default();
//
//         Runtime::new().unwrap().block_on(Dataset::write(
//             csv_reader,
//             path.to_str().unwrap(),
//             Some(write_params),
//         ))
//     })
//     .unwrap()
// }

pub fn medicare_data_vortex_uncompressed() -> PathBuf {
    idempotent("taxi-uncompressed.vortex", |path| {
        let csv_file = File::open(medicare_data_csv()).unwrap();
        let reader = BufReader::new(csv_file.try_clone().unwrap());

        let (schema, _) = Format::default()
            .infer_schema(&mut csv_file.try_clone().unwrap(), None)
            .unwrap();

        let csv_reader = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_batch_size(crate::reader::BATCH_SIZE)
            .build(reader)?;

        let dtype = DType::from_arrow(SchemaRef::new(schema.clone()));

        let chunks = csv_reader
            .map(|batch_result| batch_result.unwrap())
            .map(|record_batch| record_batch.into_array())
            .collect_vec();
        let chunked = ChunkedArray::new(chunks, dtype.clone());

        let mut write = File::create(path).unwrap();
        let mut write_ctx = WriteCtx::new(&mut write);
        write_ctx.dtype(&dtype)?;
        write_ctx.write(&chunked)
    })
    .unwrap()
}

pub fn medicare_data_vortex() -> PathBuf {
    idempotent("taxi.vortex", |path| {
        let mut write = File::create(path).unwrap();
        let delimiter = u8::try_from('|').unwrap();
        compress_csv_to_vortex(
            medicare_data_csv(),
            default_csv_format().with_delimiter(delimiter),
            &mut write,
        )
    })
    .unwrap()
}
