use bench_vortex::reader::take_vortex;
use bench_vortex::setup_logger;
use bench_vortex::taxi_data::taxi_data_vortex;
use log::LevelFilter;

#[tokio::main]
pub async fn main() {
    setup_logger(LevelFilter::Error);
    let taxi_vortex = taxi_data_vortex();
    let rows = take_vortex(&taxi_vortex, &[10, 11, 12, 13, 100_000, 3_000_000])
        .await
        .unwrap();
    println!("TAKE TAXI DATA: {:?}", rows);
}
