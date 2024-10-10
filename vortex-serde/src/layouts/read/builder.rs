use std::sync::{Arc, RwLock};

use bytes::BytesMut;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::projection::Projection;

use crate::io::VortexReadAt;
use crate::layouts::read::cache::{LayoutMessageCache, RelativeLayoutCache};
use crate::layouts::read::context::LayoutDeserializer;
use crate::layouts::read::filtering::RowFilter;
use crate::layouts::read::footer::Footer;
use crate::layouts::read::pruner::LayoutPruner;
use crate::layouts::read::selection::RowSelector;
use crate::layouts::read::stream::LayoutBatchStream;
use crate::layouts::read::{Scan, DEFAULT_BATCH_SIZE, FILE_POSTSCRIPT_SIZE, INITIAL_READ_SIZE};
use crate::layouts::{PruningScan, MAGIC_BYTES};

pub struct LayoutReaderBuilder<R> {
    reader: R,
    layout_serde: LayoutDeserializer,
    projection: Option<Projection>,
    size: Option<u64>,
    row_selector: Option<RowSelector>,
    row_filter: Option<RowFilter>,
    batch_size: Option<usize>,
    stats_pruning: bool,
}

impl<R: VortexReadAt> LayoutReaderBuilder<R> {
    pub fn new(reader: R, layout_serde: LayoutDeserializer) -> Self {
        Self {
            reader,
            layout_serde,
            projection: None,
            row_filter: None,
            size: None,
            row_selector: None,
            batch_size: None,
            stats_pruning: true,
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

    pub fn with_row_selector(mut self, array: RowSelector) -> Self {
        self.row_selector = Some(array);
        self
    }

    pub fn with_row_filter(mut self, row_filter: RowFilter) -> Self {
        self.row_filter = Some(row_filter);
        self
    }

    pub fn with_statistics_pruning(mut self, stat_pruning: bool) -> Self {
        self.stats_pruning = stat_pruning;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub async fn build(mut self) -> VortexResult<LayoutBatchStream<R>> {
        let footer = self.read_footer().await?;
        let row_count = footer.row_count()?;
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        let filter_projection = self
            .row_filter
            .as_ref()
            .map(|f| f.references())
            // This is necessary to have globally addressed columns in the relative cache,
            // there is probably a better of doing that, but this works for now and the API isn't very externally-useful.
            .map(|refs| footer.resolve_references(&refs.into_iter().collect::<Vec<_>>()))
            .transpose()?
            .map(Projection::from);

        let read_projection = self.projection.unwrap_or_default();

        let projected_dtype = match read_projection {
            Projection::All => footer.dtype()?,
            Projection::Flat(ref projection) => footer.projected_dtype(projection)?,
        };

        let filter_dtype = filter_projection
            .as_ref()
            .map(|p| match p {
                Projection::All => footer.dtype(),
                Projection::Flat(fields) => footer.projected_dtype(fields),
            })
            .transpose()?;

        let scan = Scan {
            filter: self.row_filter.clone(),
            batch_size,
            projection: read_projection,
            rows: self.row_selector.clone(),
        };

        let message_cache = Arc::new(RwLock::new(LayoutMessageCache::default()));

        let data_reader = footer.layout(
            scan.clone(),
            RelativeLayoutCache::new(message_cache.clone(), projected_dtype.clone()),
        )?;

        let pruning_scan = self
            .stats_pruning
            .then(|| {
                self.row_filter
                    .as_ref()
                    .and_then(|f| f.to_pruning_filter())
                    .map(|(pruning_filter, stats_projection)| PruningScan {
                        stats_projection,
                        filter: Some(pruning_filter),
                        row_count,
                    })
            })
            .flatten();

        let filter_reader = filter_dtype
            .clone()
            .zip(filter_projection)
            .map(|(dtype, projection)| {
                footer.layout(
                    Scan {
                        filter: self.row_filter,
                        batch_size,
                        projection,
                        rows: self.row_selector,
                    },
                    RelativeLayoutCache::new(message_cache.clone(), dtype),
                )
            })
            .transpose()?;

        let pruner = if let Some(pscan) = pruning_scan {
            if let Some(fr) = filter_reader {
                Ok(LayoutPruner::new(
                    self.reader,
                    fr,
                    message_cache.clone(),
                    pscan,
                ))
            } else {
                Err((self.reader, filter_reader))
            }
        } else {
            Err((self.reader, filter_reader))
        };

        Ok(LayoutBatchStream::new(
            data_reader,
            pruner,
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
