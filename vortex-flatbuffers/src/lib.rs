#![allow(unused_imports)]
#![allow(dead_code)]
mod flatbuffers {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/monsters_generated.rs"
    ));
}

pub use flatbuffers::*;
