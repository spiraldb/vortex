use std::mem;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{ready, Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::{FutureExt, Stream};
use futures_util::future::BoxFuture;
use futures_util::{stream, StreamExt, TryStreamExt};
use projections::Projection;
use schema::Schema;
use vortex::array::StructArray;
use vortex::compute::unary::subtract_scalar;
use vortex::compute::{filter, filter_indices, search_sorted, slice, take, SearchSortedSide};
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{vortex_bail, VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::io::VortexReadAt;
use crate::layout::footer::Footer;
use crate::layout::reader::filtering::RowFilter;
use crate::layout::writer::layout_writer::MAGIC_BYTES;
use crate::layout::{
    Layout, LayoutReader, MessageId, MessagesCache, ReadResult, RelativeMessageCache, Scan,
};

pub mod batch;
pub mod buffered;
pub mod filtering;
pub mod projections;
pub mod schema;

const DEFAULT_BATCH_SIZE: usize = 65536;
const DEFAULT_PROJECTION: Projection = Projection::All;

pub struct VortexLayoutReaderBuilder<R> {
    reader: R,
    layout_serde: LayoutReader,
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

    pub fn new(reader: R, layout_serde: LayoutReader) -> Self {
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
        let projection = self.projection.unwrap_or(DEFAULT_PROJECTION);
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        let scan = Scan {
            projection,
            indices: self.indices,
            filter: self.row_filter,
            batch_size,
        };

        let message_cache = Arc::new(RwLock::new(MessagesCache::default()));
        let layouts_cache =
            RelativeMessageCache::new(footer.dtype()?, message_cache.clone(), Vec::new());

        let layout = footer.layout(scan.clone(), layouts_cache)?;

        VortexLayoutBatchStream::try_new(self.reader, layout, message_cache, footer.dtype()?, scan)
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

pub struct VortexLayoutBatchStream<R> {
    reader: Option<R>,
    layout: Box<dyn Layout>,
    scan: Scan,
    messages_cache: Arc<RwLock<MessagesCache>>,
    state: StreamingState<R>,
    dtype: DType,
    current_offset: usize,
}

impl<R: VortexReadAt> VortexLayoutBatchStream<R> {
    fn try_new(
        reader: R,
        layout: Box<dyn Layout>,
        messages_cache: Arc<RwLock<MessagesCache>>,
        dtype: DType,
        scan: Scan,
    ) -> VortexResult<Self> {
        Ok(VortexLayoutBatchStream {
            reader: Some(reader),
            layout,
            scan,
            messages_cache,
            state: Default::default(),
            dtype,
            current_offset: 0,
        })
    }

    pub fn schema(&self) -> Schema {
        Schema(self.dtype.clone())
    }

    // TODO(robert): Remove this once we support row pruning
    fn take_batch(&mut self, batch: &Array) -> VortexResult<Array> {
        let curr_offset = self.current_offset;
        let indices = self.scan.indices.as_ref().expect("should be there");
        let left =
            search_sorted(indices, curr_offset, SearchSortedSide::Left)?.to_zero_offset_index();
        let right = search_sorted(indices, curr_offset + batch.len(), SearchSortedSide::Left)?
            .to_zero_offset_index();

        self.current_offset += batch.len();
        // TODO(ngates): this is probably too heavy to run on the event loop. We should spawn
        //  onto a worker pool.
        let indices_for_batch = slice(indices, left, right)?.into_primitive()?;
        let shifted_arr = match_each_integer_ptype!(indices_for_batch.ptype(), |$T| {
            subtract_scalar(&indices_for_batch.into_array(), &Scalar::from(curr_offset as $T))?
        });

        take(batch, &shifted_arr)
    }
}

type StreamStateFuture<R> = BoxFuture<'static, VortexResult<(R, Vec<(Vec<MessageId>, Bytes)>)>>;

#[derive(Default)]
enum StreamingState<R> {
    #[default]
    Init,
    Reading(StreamStateFuture<R>),
    Decoding(Array),
    Error,
}

impl<R: VortexReadAt + Unpin + Send + 'static> Stream for VortexLayoutBatchStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    let rr = self.layout.read()?;
                    if let Some(read) = rr {
                        match read {
                            ReadResult::GetMsgs(r1, r2) => {
                                assert!(r2.is_empty(), "Read ahead not yet supported");
                                let read =
                                    mem::take(&mut self.reader).expect("Invalid state transition");
                                let f = async move {
                                    let ar = read;
                                    let bufs = stream::iter(r1.into_iter())
                                        .map(|(id, range)| {
                                            println!("Range id: {id:?}, bytes: {range:?}");
                                            let mut buf = BytesMut::with_capacity(range.len());
                                            unsafe { buf.set_len(range.len()) }
                                            let buf_result = ar.read_at_into(range.begin, buf);
                                            buf_result.map(|res_ft| {
                                                res_ft
                                                    .map(|res| (id, res.freeze()))
                                                    .map_err(VortexError::from)
                                            })
                                        })
                                        .buffered(10)
                                        .try_collect()
                                        .await;
                                    bufs.map(|b| (ar, b))
                                }
                                .boxed();
                                self.state = StreamingState::Reading(f);
                            }
                            ReadResult::Batch(a) => self.state = StreamingState::Decoding(a),
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
                StreamingState::Decoding(arr) => {
                    let mut batch = arr.clone();
                    if self.scan.indices.is_some() {
                        batch = self.take_batch(&batch)?;
                    }

                    if let Some(row_filter) = &self.scan.filter {
                        let mask = filter_indices(&batch, &row_filter.disjunction)?;
                        batch = filter(&batch, &mask)?;
                    }

                    let projected = match &self.scan.projection {
                        Projection::All => batch,
                        Projection::Partial(indices) => StructArray::try_from(batch.clone())?
                            .project(indices.as_ref())?
                            .into_array(),
                    };

                    self.state = StreamingState::Init;
                    return Poll::Ready(Some(Ok(projected)));
                }
                StreamingState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((read, buffers)) => {
                        let mut write_cache = self.messages_cache.write().unwrap();
                        for (id, buf) in buffers {
                            write_cache.set(id, buf)
                        }
                        drop(write_cache);
                        self.reader = Some(read);
                        self.state = StreamingState::Init
                    }
                    Err(e) => {
                        self.state = StreamingState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                StreamingState::Error => return Poll::Ready(None),
            }
        }
    }
}
