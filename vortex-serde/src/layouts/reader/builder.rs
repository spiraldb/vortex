use std::sync::{Arc, RwLock};

use bytes::BytesMut;
use vortex::{Array, ArrayDType};
use vortex_error::{vortex_bail, VortexResult};

use crate::io::VortexReadAt;
use crate::layouts::reader::context::LayoutDeserializer;
use crate::layouts::reader::filtering::RowFilter;
use crate::layouts::reader::footer::Footer;
use crate::layouts::reader::projections::Projection;
use crate::layouts::reader::stream::VortexLayoutBatchStream;
use crate::layouts::reader::{LayoutMessageCache, RelativeLayoutCache, Scan, DEFAULT_BATCH_SIZE};
use crate::layouts::MAGIC_BYTES;

pub struct VortexLayoutReaderBuilder<R> {
    reader: R,
    layout_serde: LayoutDeserializer,
    projection: Option<Projection>,
    len: Option<u64>,
    indices: Option<Array>,
    row_filter: Option<RowFilter>,
    batch_size: Option<usize>,
}

impl<R: VortexReadAt> VortexLayoutReaderBuilder<R> {
    // Recommended read-size according to the AWS performance guide
    const FOOTER_READ_SIZE: usize = 8 * 1024 * 1024;
    const FOOTER_TRAILER_SIZE: usize = 20;

    pub fn new(reader: R, layout_serde: LayoutDeserializer) -> Self {
        Self {
            reader,
            layout_serde,
            projection: None,
            row_filter: None,
            len: None,
            indices: None,
            batch_size: None,
        }
    }

    pub fn with_length(mut self, len: u64) -> Self {
        self.len = Some(len);
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

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub async fn build(mut self) -> VortexResult<VortexLayoutBatchStream<R>> {
        let footer = self.read_footer().await?;
        let projection = self.projection.unwrap_or_default();
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        let projected_dtype = match &projection {
            Projection::All => footer.dtype()?,
            Projection::Partial(projection) => footer.projected_dtype(projection)?,
        };

        let scan = Scan {
            projection,
            indices: self.indices,
            filter: self.row_filter,
            batch_size,
        };

        let message_cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let layouts_cache =
            RelativeLayoutCache::new(message_cache.clone(), projected_dtype.clone());

        let layout = footer.layout(scan.clone(), layouts_cache)?;

        VortexLayoutBatchStream::try_new(self.reader, layout, message_cache, projected_dtype, scan)
    }

    async fn len(&self) -> usize {
        let len = match self.len {
            Some(l) => l,
            None => self.reader.size().await,
        };

        len as usize
    }

    async fn read_footer(&mut self) -> VortexResult<Footer> {
        let file_length = self.len().await;

        if file_length < Self::FOOTER_TRAILER_SIZE {
            vortex_bail!(
                "Malformed vortex file, length {} must be at least {}",
                file_length,
                Self::FOOTER_TRAILER_SIZE,
            )
        }

        let read_size = Self::FOOTER_READ_SIZE.min(file_length);
        let mut buf = BytesMut::with_capacity(read_size);
        unsafe { buf.set_len(read_size) }

        let read_offset = (file_length - read_size) as u64;
        buf = self.reader.read_at_into(read_offset, buf).await?;

        let magic_bytes_loc = read_size - MAGIC_BYTES.len();

        let magic_number = &buf[magic_bytes_loc..];
        if magic_number != MAGIC_BYTES {
            vortex_bail!("Malformed file, invalid magic bytes, got {magic_number:?}")
        }

        let footer_offset = u64::from_le_bytes(
            buf[magic_bytes_loc - 8..magic_bytes_loc]
                .try_into()
                .unwrap(),
        );
        let schema_offset = u64::from_le_bytes(
            buf[magic_bytes_loc - 16..magic_bytes_loc - 8]
                .try_into()
                .unwrap(),
        );

        Ok(Footer {
            schema_offset,
            footer_offset,
            leftovers: buf.freeze(),
            leftovers_offset: read_offset,
            layout_serde: self.layout_serde.clone(),
        })
    }
}
