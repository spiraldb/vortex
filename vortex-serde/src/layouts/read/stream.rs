use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{ready, Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::future::BoxFuture;
use futures_util::{stream, FutureExt, StreamExt, TryStreamExt};
use vortex::Array;
use vortex_dtype::DType;
use vortex_error::{vortex_err, vortex_panic, VortexError, VortexExpect, VortexResult};
use vortex_schema::Schema;

use crate::io::VortexReadAt;
use crate::layouts::read::cache::LayoutMessageCache;
use crate::layouts::read::selection::{RowRange, RowSelector};
use crate::layouts::read::{LayoutReader, MessageId, ReadResult};
use crate::layouts::RangeResult;
use crate::stream_writer::ByteRange;

pub struct LayoutBatchStream<R> {
    dtype: DType,
    row_count: u64,
    input: Option<R>,
    layout_reader: Box<dyn LayoutReader>,
    filter_reader: Option<Box<dyn LayoutReader>>,
    messages_cache: Arc<RwLock<LayoutMessageCache>>,
    current_selector: RowSelector,
    state: StreamingState<R>,
}

impl<R: VortexReadAt> LayoutBatchStream<R> {
    pub fn new(
        input: R,
        layout_reader: Box<dyn LayoutReader>,
        filter_reader: Option<Box<dyn LayoutReader>>,
        messages_cache: Arc<RwLock<LayoutMessageCache>>,
        dtype: DType,
        row_count: u64,
    ) -> Self {
        let state = if filter_reader.is_some() {
            StreamingState::FilterInit
        } else {
            StreamingState::Init(false)
        };

        LayoutBatchStream {
            dtype,
            row_count,
            input: Some(input),
            layout_reader,
            filter_reader,
            messages_cache,
            current_selector: RowSelector::new(
                vec![RowRange::new(0, row_count as usize)],
                row_count as usize,
            ),
            state,
        }
    }

    pub fn schema(&self) -> Schema {
        Schema::new(self.dtype.clone())
    }

    fn store_messages(&self, messages: Vec<(MessageId, Bytes)>) {
        let mut write_cache_guard = self
            .messages_cache
            .write()
            .unwrap_or_else(|poison| vortex_panic!("Failed to write to message cache: {poison}"));
        for (message_id, buf) in messages {
            write_cache_guard.set(message_id, buf);
        }
    }
}

type StreamStateFuture<R> = BoxFuture<'static, VortexResult<(R, Vec<(MessageId, Bytes)>)>>;

enum StreamingState<R> {
    Init(bool),
    FilterInit,
    Reading(StreamStateFuture<R>, bool),
    Error,
}

impl<R: VortexReadAt + Unpin + 'static> Stream for LayoutBatchStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init(more_filter) => {
                    let more_filter = *more_filter;
                    let selector = self.current_selector.clone();
                    if let Some(read) = self.layout_reader.read_next(selector)? {
                        match read {
                            ReadResult::ReadMore(messages) => {
                                let reader = self.input.take().ok_or_else(|| {
                                    vortex_err!("Invalid state transition - reader dropped")
                                })?;
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = StreamingState::Reading(read_future, false);
                            }
                            ReadResult::Batch(a) => return Poll::Ready(Some(Ok(a))),
                        }
                    } else if more_filter {
                        self.state = StreamingState::FilterInit;
                    } else {
                        return Poll::Ready(None);
                    }
                }
                StreamingState::FilterInit => {
                    if let Some(fr) = self
                        .filter_reader
                        .as_mut()
                        .vortex_expect("Can't filter without reader")
                        .read_range()?
                    {
                        match fr {
                            RangeResult::ReadMore(messages) => {
                                let reader = self.input.take().ok_or_else(|| {
                                    vortex_err!("Invalid state transition - reader dropped")
                                })?;
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = StreamingState::Reading(read_future, true);
                            }
                            RangeResult::Range(rs) => {
                                self.current_selector = rs;
                                self.state = StreamingState::Init(true);
                            }
                        }
                    } else {
                        self.state = StreamingState::Init(false);
                    }
                }
                StreamingState::Reading(f, filter_more) => match ready!(f.poll_unpin(cx)) {
                    Ok((input, messages)) => {
                        let filter_more = *filter_more;
                        self.store_messages(messages);
                        self.input = Some(input);

                        self.state = if filter_more {
                            StreamingState::FilterInit
                        } else {
                            StreamingState::Init(self.filter_reader.is_some())
                        };
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
