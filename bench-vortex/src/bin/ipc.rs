use log::LevelFilter;
use std::fs::File;

use bench_vortex::reader::open_vortex;
use bench_vortex::taxi_data::taxi_data_vortex;
use bench_vortex::{idempotent, setup_logger};
use vortex::serde::context::SerdeContext;
use vortex_ipc::writer::StreamWriter;

pub fn main() {
    setup_logger(LevelFilter::Error);

    let array = open_vortex(&taxi_data_vortex()).unwrap();

    let ipc = idempotent("ipc.vortex", |path| {
        let mut write = File::create(path)?;
        let ctx = SerdeContext::default();
        let mut writer = StreamWriter::try_new(&mut write, ctx)?;
        writer.write(&array)
    })
    .unwrap();

    println!("IPC TAXI DATA: {:?}", ipc);
}
