pub mod reader;
pub mod writer;

mod statistics;
#[cfg(test)]
mod tests;

pub const MAGIC_BYTES: [u8; 4] = *b"VRX1";
