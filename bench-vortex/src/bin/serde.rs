use bench_vortex::setup_logger;
use bench_vortex::taxi_data::{take_taxi_data, write_taxi_data};
use log::LevelFilter;

pub fn main() {
    setup_logger(LevelFilter::Debug);
    let taxi_spiral = write_taxi_data();
    let rows = take_taxi_data(&taxi_spiral, &[10, 11, 12, 13]); //, 100_000, 3_000_000]);
    println!("TAKE TAXI DATA: {:?}", rows);
}
