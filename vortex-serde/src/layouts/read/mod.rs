use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use arrow_array::builder::BooleanBufferBuilder;
use futures::StreamExt as _;
pub use layouts::{ChunkedLayoutSpec, ColumnLayoutSpec};
use vortex::array::BoolArray;
use vortex::validity::{ArrayValidity, Validity};
use vortex::{Array, IntoArray as _, IntoArrayVariant as _};
use vortex_error::VortexResult;

mod batch;
mod buffered;
mod builder;
mod cache;
mod context;
mod filtering;
mod footer;
mod layouts;
mod stream;

pub use builder::LayoutReaderBuilder;
pub use cache::LayoutMessageCache;
pub use context::*;
pub use filtering::RowFilter;
pub use stream::LayoutBatchStream;
use vortex_expr::VortexExpr;
pub use vortex_schema::projection::Projection;
pub use vortex_schema::Schema;

use crate::io::VortexReadAt;
use crate::stream_writer::ByteRange;

// Recommended read-size according to the AWS performance guide
const INITIAL_READ_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_BATCH_SIZE: usize = 65536;
const FILE_POSTSCRIPT_SIZE: usize = 20;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Scan {
    indices: Option<Array>,
    row_selection: Option<Array>,
    projection: Projection,
    filter: Option<RowFilter>,
    batch_size: usize,
}

/// Unique identifier for a message within a layout
pub type LayoutPartId = u16;
pub type MessageId = Vec<LayoutPartId>;

#[derive(Debug)]
pub enum ReadResult {
    ReadMore(Vec<(MessageId, ByteRange)>),
    Batch(Array),
}

pub trait Layout: Debug + Send {
    /// Reads the data from the underlying layout
    ///
    /// The layout can either return a batch data, i.e. an Array or ask for more layout messages to
    /// be read. When requesting messages to be read the caller should populate the messages in the cache
    /// and then call back into this function.
    ///
    /// The layout is finished reading when it returns None
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>>;

    // TODO(robert): Support stats pruning via planning. Requires propagating all the metadata
    //  to top level and then pushing down the result of it
    // Try to use metadata of the layout to perform pruning given the passed `Scan` object.
    //
    // The layout should perform any planning that's cheap and doesn't require reading the data.
    // fn plan(&mut self, scan: Scan) -> VortexResult<Option<PlanResult>>;
}

pub fn null_as_false(array: BoolArray) -> VortexResult<Array> {
    match array.validity() {
        Validity::NonNullable => Ok(array.into_array()),
        Validity::AllValid => {
            Ok(BoolArray::try_new(array.boolean_buffer(), Validity::NonNullable)?.into_array())
        }
        Validity::AllInvalid => Ok(BoolArray::from(vec![false; array.len()]).into_array()),
        Validity::Array(v) => {
            let bool_buffer = &array.boolean_buffer() & &v.into_bool()?.boolean_buffer();
            Ok(BoolArray::from(bool_buffer).into_array())
        }
    }
}

pub async fn build_selection<R: VortexReadAt + Unpin + Send + 'static>(
    reader: R,
    expr: Arc<dyn VortexExpr>,
    deserializer: LayoutDeserializer,
    message_cache: Arc<RwLock<LayoutMessageCache>>,
) -> VortexResult<Array> {
    let mut builder = LayoutReaderBuilder::new(reader, deserializer);
    let footer = builder.read_footer().await?;
    let row_filter = RowFilter::new(expr, footer.schema()?);
    builder = builder.with_message_cache(message_cache);

    let mut stream = builder.build().await?;
    let mut bool_builder = BooleanBufferBuilder::new(0);
    let mut validity_builder = vec![];

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let bool_array = row_filter.evaluate(&batch)?.into_bool()?;
        bool_builder.append_buffer(&bool_array.boolean_buffer());
        validity_builder.push(bool_array.logical_validity());
    }

    BoolArray::try_new(bool_builder.finish(), Validity::from_iter(validity_builder))
        .map(|a| a.into_array())
}

#[cfg(test)]
mod tests {
    use vortex::array::BoolArray;
    use vortex::validity::Validity;
    use vortex::IntoArrayVariant;

    use super::*;

    #[test]
    fn coerces_nulls() {
        let bool_array = BoolArray::from_vec(
            vec![true, true, false, false],
            Validity::Array(BoolArray::from(vec![true, false, true, false]).into()),
        );
        let non_null_array = null_as_false(bool_array).unwrap().into_bool().unwrap();
        assert_eq!(
            non_null_array.boolean_buffer().iter().collect::<Vec<_>>(),
            vec![true, false, false, false]
        );
    }
}
