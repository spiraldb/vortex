use std::mem;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{ready, Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::future::BoxFuture;
use futures_util::{stream, FutureExt, StreamExt, TryStreamExt};
use vortex::array::StructArray;
use vortex::compute::unary::subtract_scalar;
use vortex::compute::{filter, search_sorted, slice, take, SearchSortedSide};
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{vortex_err, vortex_panic, VortexError, VortexResult};
use vortex_scalar::Scalar;
use vortex_schema::projection::Projection;
use vortex_schema::Schema;

use super::null_as_false;
use crate::io::VortexReadAt;
use crate::layouts::read::cache::LayoutMessageCache;
use crate::layouts::read::{Layout, MessageId, ReadResult, Scan};
use crate::stream_writer::ByteRange;

pub struct LayoutBatchStream<R> {
    reader: Option<R>,
    layout: Box<dyn Layout>,
    scan: Scan,
    messages_cache: Arc<RwLock<LayoutMessageCache>>,
    state: StreamingState<R>,
    dtype: DType,
    current_offset: usize,
    result_projection: Projection,
}

impl<R: VortexReadAt> LayoutBatchStream<R> {
    pub fn new(
        reader: R,
        layout: Box<dyn Layout>,
        messages_cache: Arc<RwLock<LayoutMessageCache>>,
        dtype: DType,
        scan: Scan,
        result_projection: Projection,
    ) -> Self {
        LayoutBatchStream {
            reader: Some(reader),
            layout,
            scan,
            messages_cache,
            result_projection,

            dtype,
            state: Default::default(),
            current_offset: 0,
        }
    }

    pub fn schema(&self) -> Schema {
        Schema::new(self.dtype.clone())
    }

    // TODO(robert): Push this logic down to layouts
    fn take_batch(&mut self, batch: &Array) -> VortexResult<Array> {
        let curr_offset = self.current_offset;
        let indices = self
            .scan
            .indices
            .as_ref()
            .ok_or_else(|| vortex_err!("Missing scan indices"))?;
        let left = search_sorted(indices, curr_offset, SearchSortedSide::Left)?.to_index();
        let right =
            search_sorted(indices, curr_offset + batch.len(), SearchSortedSide::Left)?.to_index();

        self.current_offset += batch.len();

        let indices_for_batch = slice(indices, left, right)?.into_primitive()?;
        let shifted_arr = match_each_integer_ptype!(indices_for_batch.ptype(), |$T| {
            subtract_scalar(&indices_for_batch.into_array(), &Scalar::from(curr_offset as $T))?
        });

        take(batch, &shifted_arr)
    }
}

type StreamStateFuture<R> = BoxFuture<'static, VortexResult<(R, Vec<(MessageId, Bytes)>)>>;

#[derive(Default)]
enum StreamingState<R> {
    #[default]
    Init,
    Reading(StreamStateFuture<R>),
    Decoding(Array),
    Error,
}

impl<R: VortexReadAt + Unpin + Send + 'static> Stream for LayoutBatchStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    if let Some(read) = self.layout.read_next()? {
                        match read {
                            ReadResult::ReadMore(messages) => {
                                let reader = mem::take(&mut self.reader)
                                    .ok_or_else(|| vortex_err!("Invalid state transition"))?;
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = StreamingState::Reading(read_future);
                            }
                            ReadResult::Batch(a) => self.state = StreamingState::Decoding(a),
                        }
                    } else {
                        if let Some(selection) = self.scan.row_selection.as_ref() {
                            assert!(selection.is_empty());
                        }
                        return Poll::Ready(None);
                    }
                }
                StreamingState::Decoding(arr) => {
                    let mut batch = arr.clone();

                    if let Some(selection) = self.scan.row_selection.take() {
                        let batch_selection = slice(&selection, 0, batch.len())?;
                        let batch_selection = null_as_false(batch_selection.into_bool()?)?;
                        let reminder = slice(&selection, batch.len(), selection.len())?;

                        self.scan.row_selection = Some(reminder);

                        batch = filter(&batch, &batch_selection)?;
                    }

                    if self.scan.indices.is_some() {
                        batch = self.take_batch(&batch)?;
                    }

                    if let Some(row_filter) = &self.scan.filter {
                        batch = row_filter.evaluate(&batch)?;
                    }

                    batch = match &self.result_projection {
                        Projection::All => batch,
                        Projection::Flat(v) => {
                            StructArray::try_from(batch)?.project(v)?.into_array()
                        }
                    };

                    self.state = StreamingState::Init;
                    return Poll::Ready(Some(Ok(batch)));
                }
                StreamingState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((read, buffers)) => {
                        let mut write_cache =
                            self.messages_cache.write().unwrap_or_else(|poison| {
                                vortex_panic!("Failed to write to message cache: {poison}")
                            });
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

async fn read_ranges<R: VortexReadAt>(
    reader: R,
    ranges: Vec<(MessageId, ByteRange)>,
) -> VortexResult<(R, Vec<(MessageId, Bytes)>)> {
    stream::iter(ranges.into_iter())
        .map(|(id, range)| {
            let mut buf = BytesMut::with_capacity(range.len());
            unsafe { buf.set_len(range.len()) }

            let read_ft = reader.read_at_into(range.begin, buf);

            read_ft.map(|result| {
                result
                    .map(|res| (id, res.freeze()))
                    .map_err(VortexError::from)
            })
        })
        .buffered(10)
        .try_collect()
        .await
        .map(|b| (reader, b))
}
