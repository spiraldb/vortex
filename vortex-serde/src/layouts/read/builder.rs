use std::sync::{Arc, RwLock};

use bytes::BytesMut;
use vortex::{Array, ArrayDType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::projection::Projection;
use vortex_schema::Schema;

use crate::io::VortexReadAt;
use crate::layouts::read::cache::{LayoutMessageCache, RelativeLayoutCache};
use crate::layouts::read::context::LayoutDeserializer;
use crate::layouts::read::filtering::RowFilter;
use crate::layouts::read::footer::Footer;
use crate::layouts::read::stream::LayoutBatchStream;
use crate::layouts::read::{Scan, DEFAULT_BATCH_SIZE, FILE_POSTSCRIPT_SIZE, INITIAL_READ_SIZE};
use crate::layouts::MAGIC_BYTES;

pub struct LayoutReaderBuilder<R> {
    reader: R,
    layout_serde: LayoutDeserializer,
    projection: Option<Projection>,
    size: Option<u64>,
    indices: Option<Array>,
    row_filter: Option<RowFilter>,
    batch_size: Option<usize>,
    row_selection: Option<Array>,
    message_cache: Option<Arc<RwLock<LayoutMessageCache>>>,
}

impl<R: VortexReadAt> LayoutReaderBuilder<R> {
    pub fn new(reader: R, layout_serde: LayoutDeserializer) -> Self {
        Self {
            reader,
            layout_serde,
            projection: None,
            row_filter: None,
            size: None,
            indices: None,
            batch_size: None,
            row_selection: None,
            message_cache: None,
        }
    }

    pub fn with_length(mut self, len: u64) -> Self {
        self.size = Some(len);
        self
    }

    pub fn with_projection(mut self, projection: Projection) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_indices(mut self, array: Array) -> Self {
        // TODO(#441): Allow providing boolean masks
        assert!(
            array.dtype().is_int(),
            "Mask arrays have to be integer arrays"
        );
        self.indices = Some(array);
        self
    }

    pub fn with_row_filter(mut self, row_filter: RowFilter) -> Self {
        self.row_filter = Some(row_filter);
        self
    }

    pub fn with_row_selection(mut self, selection: Array) -> Self {
        assert!(
            selection.dtype().is_boolean(),
            "Row selection arrays must be a boolean array"
        );
        self.row_selection = Some(selection);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_message_cache(mut self, message_cache: Arc<RwLock<LayoutMessageCache>>) -> Self {
        self.message_cache = Some(message_cache);
        self
    }

    pub async fn build(mut self) -> VortexResult<LayoutBatchStream<R>> {
        let footer = self.read_footer().await?;
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let projection = self.projection.unwrap_or_default();

        let projected_dtype = match &projection {
            Projection::All => footer.dtype()?,
            Projection::Flat(projection) => footer.projected_dtype(projection)?,
        };

        let filter = self.row_filter.map(|f| {
            let schema = Schema::new(projected_dtype.clone());
            f.reorder(&schema)
        });

        let scan = Scan {
            filter,
            batch_size,
            projection,
            indices: self.indices,
            row_selection: self.row_selection,
        };

        let message_cache = self.message_cache.unwrap_or_default();
        let layouts_cache =
            RelativeLayoutCache::new(message_cache.clone(), projected_dtype.clone());

        let layout = footer.layout(scan.clone(), layouts_cache)?;

        Ok(LayoutBatchStream::new(
            self.reader,
            layout,
            message_cache,
            projected_dtype,
            scan,
        ))
    }

    async fn size(&self) -> usize {
        let size = match self.size {
            Some(s) => s,
            None => self.reader.size().await,
        };

        size as usize
    }

    pub async fn read_footer(&mut self) -> VortexResult<Footer> {
        let file_size = self.size().await;

        if file_size < FILE_POSTSCRIPT_SIZE {
            vortex_bail!(
                "Malformed vortex file, size {} must be at least {}",
                file_size,
                FILE_POSTSCRIPT_SIZE,
            )
        }

        let read_size = INITIAL_READ_SIZE.min(file_size);
        let mut buf = BytesMut::with_capacity(read_size);
        unsafe { buf.set_len(read_size) }

        let read_offset = (file_size - read_size) as u64;
        buf = self.reader.read_at_into(read_offset, buf).await?;

        let magic_bytes_loc = read_size - MAGIC_BYTES.len();

        let magic_number = &buf[magic_bytes_loc..];
        if magic_number != MAGIC_BYTES {
            vortex_bail!("Malformed file, invalid magic bytes, got {magic_number:?}")
        }

        let layout_offset =
            u64::from_le_bytes(buf[magic_bytes_loc - 8..magic_bytes_loc].try_into()?);
        let schema_offset =
            u64::from_le_bytes(buf[magic_bytes_loc - 16..magic_bytes_loc - 8].try_into()?);

        Ok(Footer {
            schema_offset,
            layout_offset,
            leftovers: buf.freeze(),
            leftovers_offset: read_offset,
            layout_serde: self.layout_serde.clone(),
        })
    }
}
