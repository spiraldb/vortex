use vortex_error::VortexError;

pub mod flatbuffers {
    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/message.rs"));
    }
    pub use generated::vortex::*;
}

pub(crate) mod flatbuffers_deps {
    pub mod dtype {
        pub use vortex_schema::flatbuffers as dtype;
    }
}

pub mod context;
pub mod reader;
pub mod writer;

pub(crate) const fn missing(field: &'static str) -> impl FnOnce() -> VortexError {
    move || VortexError::InvalidSerde(format!("missing field: {}", field).into())
}
