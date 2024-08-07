use projections::Projection;

pub mod batch;
pub mod buffered;
pub mod builder;
pub mod filtering;
pub mod projections;
pub mod schema;
pub mod stream;

const DEFAULT_BATCH_SIZE: usize = 65536;
const DEFAULT_PROJECTION: Projection = Projection::All;
