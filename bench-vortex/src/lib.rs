use vortex::array::bool::BoolEncoding;
use vortex::array::chunked::ChunkedEncoding;
use vortex::array::constant::ConstantEncoding;

use vortex::array::primitive::PrimitiveEncoding;
use vortex::array::sparse::SparseEncoding;
use vortex::array::struct_::StructEncoding;
use vortex::array::typed::TypedEncoding;
use vortex::array::varbin::VarBinEncoding;
use vortex::array::varbinview::VarBinViewEncoding;
use vortex::array::Encoding;
use vortex_alp::ALPEncoding;
use vortex_dict::DictEncoding;
use vortex_fastlanes::{BitPackedEncoding, FoREncoding};
use vortex_ree::REEEncoding;
use vortex_roaring::RoaringBoolEncoding;

pub fn enumerate_arrays() -> Vec<&'static dyn Encoding> {
    vec![
        // TODO(ngates): fix https://github.com/fulcrum-so/vortex/issues/35
        // Builtins
        &BoolEncoding,
        &ChunkedEncoding,
        &ConstantEncoding,
        &PrimitiveEncoding,
        &SparseEncoding,
        &StructEncoding,
        &TypedEncoding,
        &VarBinEncoding,
        &VarBinViewEncoding,
        // Encodings
        &ALPEncoding,
        &DictEncoding,
        &BitPackedEncoding,
        // &DeltaEncoding,
        &FoREncoding,
        //&FFoREncoding,
        &REEEncoding,
        &RoaringBoolEncoding,
        //&RoaringIntEncoding,
        //&ZigZagEncoding,
    ]
}

#[cfg(test)]
mod test {
    use arrow_array::RecordBatchReader;
    use itertools::Itertools;
    use std::collections::HashSet;
    use std::fs::create_dir_all;
    use std::fs::File;
    use std::path::Path;

    use log::LevelFilter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ProjectionMask;
    use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

    use vortex::array::chunked::ChunkedArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::compress::{CompressConfig, CompressCtx};
    use vortex::formatter::display_tree;

    use crate::enumerate_arrays;

    pub fn download_taxi_data() -> &'static Path {
        let download_path = Path::new("data/yellow-tripdata-2023-11.parquet");
        if download_path.exists() {
            return download_path;
        }

        create_dir_all(download_path.parent().unwrap()).unwrap();
        let mut download_file = File::create(download_path).unwrap();
        reqwest::blocking::get(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet",
        )
        .unwrap()
        .copy_to(&mut download_file)
        .unwrap();

        download_path
    }

    fn setup_logger() {
        TermLogger::init(
            LevelFilter::Debug,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )
        .unwrap();
    }

    #[test]
    fn compression_ratio() {
        setup_logger();

        let file = File::open(download_taxi_data()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let _mask = ProjectionMask::roots(builder.parquet_schema(), [9]);
        let reader = builder
            //.with_projection(mask)
            .with_batch_size(64_000)
            //.with_batch_size(5_000_000)
            .build()
            .unwrap();

        // let array = ArrayRef::try_from((&mut reader) as &mut dyn RecordBatchReader).unwrap();
        let cfg = CompressConfig::new(
            HashSet::from_iter(enumerate_arrays().iter().map(|e| (*e).id())),
            HashSet::default(),
        );
        println!("Compression config {cfg:?}");
        let ctx = CompressCtx::new(&cfg);

        let schema = reader.schema();
        let mut uncompressed_size = 0;
        let chunks = reader
            .into_iter()
            .map(|batch_result| batch_result.unwrap())
            .map(|batch| ArrayRef::from(batch))
            .map(|array| {
                uncompressed_size += array.nbytes();
                ctx.compress(array.as_ref(), None).unwrap()
            })
            .collect_vec();

        let compressed = ChunkedArray::new(chunks, schema.try_into().unwrap()).boxed();

        // let compressed = CompressCtx::new(&cfg)
        //     .compress(array.as_ref(), None)
        //     .unwrap();
        println!("Compressed array {}", display_tree(compressed.as_ref()));
        println!(
            "NBytes {}, Ratio {}",
            compressed.nbytes(),
            compressed.nbytes() as f32 / uncompressed_size as f32
        );
    }
}
