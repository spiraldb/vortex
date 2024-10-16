use std::sync::{Arc, RwLock};

use vortex::{Array, ArrayDType};
use vortex_error::VortexResult;
use vortex_schema::projection::Projection;

use crate::io::VortexReadAt;
use crate::layouts::read::cache::{LayoutMessageCache, LazyDeserializedDType, RelativeLayoutCache};
use crate::layouts::read::context::LayoutDeserializer;
use crate::layouts::read::filtering::RowFilter;
use crate::layouts::read::footer::FooterReader;
use crate::layouts::read::stream::LayoutBatchStream;
use crate::layouts::read::{Scan, DEFAULT_BATCH_SIZE};
use crate::layouts::ScanExpr;

pub struct LayoutReaderBuilder<R> {
    reader: R,
    layout_serde: LayoutDeserializer,
    projection: Option<Projection>,
    size: Option<u64>,
    indices: Option<Array>,
    row_filter: Option<RowFilter>,
    batch_size: Option<usize>,
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
        }
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
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

    pub async fn build(self) -> VortexResult<LayoutBatchStream<R>> {
        let file_size = self.size().await as u64;
        let footer = FooterReader::new(self.layout_serde)
            .read_footer(&self.reader, file_size)
            .await?;
        let row_count = footer.row_count()?;
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let read_projection = self.projection.unwrap_or_default();

        let projected_dtype = match read_projection {
            Projection::All => footer.dtype()?,
            Projection::Flat(ref projection) => footer.projected_dtype(projection)?,
        };

        let message_cache = Arc::new(RwLock::new(LayoutMessageCache::default()));

        let filter_reader = self
            .row_filter
            .map(|rf| {
                footer.layout(
                    Scan {
                        expr: ScanExpr::Filter(rf),
                        batch_size,
                    },
                    RelativeLayoutCache::new(
                        message_cache.clone(),
                        LazyDeserializedDType::from_bytes(footer.dtype_bytes()),
                    ),
                )
            })
            .transpose()?;

        let scan = Scan {
            batch_size,
            expr: ScanExpr::Projection(read_projection),
        };
        let data_reader = footer.layout(
            scan.clone(),
            RelativeLayoutCache::new(
                message_cache.clone(),
                LazyDeserializedDType::from_bytes(footer.dtype_bytes()),
            ),
        )?;

        Ok(LayoutBatchStream::new(
            self.reader,
            data_reader,
            filter_reader,
            message_cache,
            projected_dtype,
            row_count,
        ))
    }

    async fn size(&self) -> usize {
        let size = match self.size {
            Some(s) => s,
            None => self.reader.size().await,
        };

        size as usize
    }
}
