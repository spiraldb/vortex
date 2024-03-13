#![allow(unused_imports)]
#![allow(dead_code)]
mod generated {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/monsters_generated.rs"
    ));
}

pub use generated::*;

pub use flatbuffers;
