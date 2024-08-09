use std::mem;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{ready, Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::future::BoxFuture;
use futures_util::{stream, FutureExt, StreamExt, TryStreamExt};
use vortex::compute::unary::subtract_scalar;
use vortex::compute::{filter, filter_indices, search_sorted, slice, take, SearchSortedSide};
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::io::VortexReadAt;
use crate::layouts::reader::schema::Schema;
use crate::layouts::reader::{Layout, LayoutMessageCache, MessageId, ReadResult, Scan};
use crate::writer::ByteRange;

pub struct VortexLayoutBatchStream<R> {
    reader: Option<R>,
    layout: Box<dyn Layout>,
    scan: Scan,
    messages_cache: Arc<RwLock<LayoutMessageCache>>,
    state: StreamingState<R>,
    dtype: DType,
    current_offset: usize,
}

impl<R: VortexReadAt> VortexLayoutBatchStream<R> {
    pub fn try_new(
        reader: R,
        layout: Box<dyn Layout>,
        messages_cache: Arc<RwLock<LayoutMessageCache>>,
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

    // TODO(robert): Push this logic down to layouts
    fn take_batch(&mut self, batch: &Array) -> VortexResult<Array> {
        let curr_offset = self.current_offset;
        let indices = self.scan.indices.as_ref().expect("should be there");
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

impl<R: VortexReadAt + Unpin + Send + 'static> Stream for VortexLayoutBatchStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    if let Some(read) = self.layout.read()? {
                        match read {
                            ReadResult::GetMsgs(messages) => {
                                let reader =
                                    mem::take(&mut self.reader).expect("Invalid state transition");
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = StreamingState::Reading(read_future);
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

                    self.state = StreamingState::Init;
                    return Poll::Ready(Some(Ok(batch)));
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
