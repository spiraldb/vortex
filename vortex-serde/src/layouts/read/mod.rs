use std::fmt::Debug;

use ahash::{HashMap, HashSet};
use arrow_buffer::BooleanBuffer;
pub use layouts::{ChunkedLayoutSpec, ColumnLayoutSpec};
use vortex::array::BoolArray;
use vortex::validity::Validity;
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
mod pruner;
mod selection;
mod stream;

pub use builder::LayoutReaderBuilder;
pub use cache::LayoutMessageCache;
pub use context::*;
pub use filtering::RowFilter;
pub use stream::LayoutBatchStream;
use vortex::stats::Stat;
use vortex_dtype::field::Field;
pub use vortex_schema::projection::Projection;
pub use vortex_schema::Schema;

use crate::layouts::read::selection::RowSelector;
use crate::stream_writer::ByteRange;

// Recommended read-size according to the AWS performance guide
const INITIAL_READ_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_BATCH_SIZE: usize = 65536;
const FILE_POSTSCRIPT_SIZE: usize = 20;

#[derive(Debug, Clone)]
pub struct Scan {
    rows: Option<RowSelector>,
    projection: Projection,
    filter: Option<RowFilter>,
    batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct PruningScan {
    stats_projection: HashMap<Field, HashSet<Stat>>,
    filter: Option<RowFilter>,
    row_count: u64,
}

/// Unique identifier for a message within a layout
pub type LayoutPartId = u16;
pub type MessageId = Vec<LayoutPartId>;

#[derive(Debug)]
pub enum ReadResult {
    ReadMore(Vec<(MessageId, ByteRange)>),
    Batch(Array),
}

#[derive(Debug)]
pub enum PlanResult {
    ReadMore(Vec<(MessageId, ByteRange)>),
    Range(RowSelector),
    // TODO(robert): This variant is only necessary when we start supporting multicolumn pruning expression where we need
    //  to extract metadata statistics from layout children
    Batch(Array),
}

pub trait LayoutReader: Debug + Send {
    /// Alter the reader to only include selected rows
    fn with_selected_rows(&mut self, row_selector: &RowSelector);

    /// Reads the data from the underlying layout
    ///
    /// The layout can either return a batch data, i.e. an Array or ask for more layout messages to
    /// be read. When requesting messages to be read the caller should populate the messages in the cache
    /// and then call back into this function.
    ///
    /// The layout is finished reading when it returns None
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>>;

    /// Try to use metadata of the layout to perform pruning given the passed `Scan` object.
    ///
    /// The layout should perform any planning that's cheap and doesn't require reading the data.
    fn plan(&mut self, scan: PruningScan) -> VortexResult<Option<PlanResult>>;
}

pub fn null_as_false(array: BoolArray) -> VortexResult<Array> {
    Ok(match array.validity() {
        Validity::NonNullable => array.into_array(),
        Validity::AllValid => {
            BoolArray::try_new(array.boolean_buffer(), Validity::NonNullable)?.into_array()
        }
        Validity::AllInvalid => BoolArray::from(BooleanBuffer::new_unset(array.len())).into_array(),
        Validity::Array(v) => {
            let bool_buffer = &array.boolean_buffer() & &v.into_bool()?.boolean_buffer();
            BoolArray::from(bool_buffer).into_array()
        }
    })
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
