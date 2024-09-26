use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{ready, Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::future::BoxFuture;
use futures_util::{stream, FutureExt, StreamExt, TryStreamExt};
use vortex::compute::filter;
use vortex::stats::ArrayStatistics;
use vortex::{Array, IntoArrayVariant};
use vortex_dtype::DType;
use vortex_error::{vortex_err, vortex_panic, VortexError, VortexResult};
use vortex_schema::Schema;

use super::null_as_false;
use crate::io::VortexReadAt;
use crate::layouts::read::cache::LayoutMessageCache;
use crate::layouts::read::{LayoutReader, MessageId, ReadResult, Scan};
use crate::stream_writer::ByteRange;

pub struct LayoutBatchStream<R> {
    input: Option<R>,
    layout_reader: Box<dyn LayoutReader>,
    #[allow(dead_code)]
    scan: Scan,
    messages_cache: Arc<RwLock<LayoutMessageCache>>,
    state: StreamingState<R>,
    dtype: DType,
    cached_mask: Option<Array>,
}

impl<R: VortexReadAt> LayoutBatchStream<R> {
    pub fn new(
        input: R,
        layout_reader: Box<dyn LayoutReader>,
        messages_cache: Arc<RwLock<LayoutMessageCache>>,
        dtype: DType,
        scan: Scan,
    ) -> Self {
        LayoutBatchStream {
            input: Some(input),
            layout_reader,
            scan,
            messages_cache,
            dtype,
            state: StreamingState::default(),
            cached_mask: None,
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
                    if let Some(read) = self.layout_reader.read_next()? {
                        match read {
                            ReadResult::ReadMore(messages) => {
                                let reader = self.input.take().ok_or_else(|| {
                                    vortex_err!("Invalid state transition - reader dropped")
                                })?;
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = StreamingState::Reading(read_future);
                            }
                            ReadResult::Batch(a) => self.state = StreamingState::Decoding(a),
                            ReadResult::Selection(_) => todo!(),
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }

                StreamingState::Decoding(arr) => {
                    let mut batch = arr.clone();

                    if let Some(mask) = self.cached_mask.take() {
                        let mask = null_as_false(mask.into_bool()?)?;

                        if mask.statistics().compute_true_count().unwrap_or_default() == 0 {
                            self.state = StreamingState::Init;
                            continue;
                        }

                        batch = filter(batch, mask)?;
                    }

                    self.state = StreamingState::Init;
                    return Poll::Ready(Some(Ok(batch)));
                }
                StreamingState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((input, messages)) => {
                        self.store_messages(messages);
                        self.input = Some(input);

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
