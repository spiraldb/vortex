use std::mem;
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
use vortex_error::{vortex_err, vortex_panic, VortexError, VortexExpect, VortexResult};
use vortex_schema::Schema;

use super::null_as_false;
use crate::io::VortexReadAt;
use crate::layouts::read::cache::LayoutMessageCache;
use crate::layouts::read::pruner::LayoutPruner;
use crate::layouts::read::selection::RowSelector;
use crate::layouts::read::{LayoutReader, MessageId, ReadResult, Scan};
use crate::stream_writer::ByteRange;

type BatchPrunerReader<R> = Result<LayoutPruner<R>, (R, Option<Box<dyn LayoutReader>>)>;

pub struct LayoutBatchStream<R> {
    input: Option<R>,
    layout_reader: Box<dyn LayoutReader>,
    filter_reader: Option<Box<dyn LayoutReader>>,
    layout_pruner: Option<LayoutPruner<R>>,
    scan: Scan,
    messages_cache: Arc<RwLock<LayoutMessageCache>>,
    state: StreamingState<R>,
    dtype: DType,
    cached_mask: Option<Array>,
    pruned_rows: RowSelector,
}

impl<R: VortexReadAt> LayoutBatchStream<R> {
    pub fn new(
        layout_reader: Box<dyn LayoutReader>,
        pruner: BatchPrunerReader<R>,
        messages_cache: Arc<RwLock<LayoutMessageCache>>,
        dtype: DType,
        scan: Scan,
    ) -> Self {
        let mut input = None;
        let mut filter_reader = None;
        let mut layout_pruner = None;
        let mut state = StreamingState::Init;
        if let Ok(pruner) = pruner {
            layout_pruner = Some(pruner);
            state = StreamingState::Pruning;
        } else if let Err((i, fr)) = pruner {
            input = Some(i);
            filter_reader = fr;
            if filter_reader.is_some() {
                state = StreamingState::FilterInit;
            }
        }

        Self {
            input,
            layout_reader,
            filter_reader,
            layout_pruner,
            scan,
            messages_cache,
            dtype,
            state,
            cached_mask: None,
            pruned_rows: RowSelector::default(),
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
    FilterInit,
    Pruning,
    Reading(StreamStateFuture<R>),
    FilterReading(StreamStateFuture<R>),
    Decoding(Array),
    Error,
}

impl<R: VortexReadAt + Unpin + 'static> Stream for LayoutBatchStream<R> {
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
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
                StreamingState::FilterInit => {
                    if let Some(read) = self
                        .filter_reader
                        .as_mut()
                        .vortex_expect("Can't filter without reader")
                        .read_next()?
                    {
                        match read {
                            ReadResult::ReadMore(messages) => {
                                let reader = self.input.take().ok_or_else(|| {
                                    vortex_err!("Invalid state transition - reader dropped")
                                })?;
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = StreamingState::FilterReading(read_future);
                            }
                            ReadResult::Batch(a) => {
                                let mask = self
                                    .scan
                                    .filter
                                    .as_ref()
                                    .vortex_expect("Cant filter without filter")
                                    .evaluate(&a)?;
                                self.cached_mask = Some(mask);
                                self.state = StreamingState::Init;
                            }
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

                    self.state = if self.filter_reader.is_some() {
                        StreamingState::FilterInit
                    } else {
                        StreamingState::Init
                    };
                    return Poll::Ready(Some(Ok(batch)));
                }
                StreamingState::Reading(f) | StreamingState::FilterReading(f) => {
                    match ready!(f.poll_unpin(cx)) {
                        Ok((input, messages)) => {
                            self.store_messages(messages);
                            self.input = Some(input);

                            self.state = match &self.state {
                                StreamingState::Reading(_) => StreamingState::Init,
                                StreamingState::FilterReading(_) => StreamingState::FilterInit,
                                _ => unreachable!("Matched on wrong state"),
                            }
                        }
                        Err(e) => {
                            self.state = StreamingState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                StreamingState::Pruning => {
                    if let Some(pruner) = &mut self.layout_pruner {
                        match ready!(pruner.try_poll_next_unpin(cx)) {
                            None => {
                                let (input, filter_reader) = self
                                    .layout_pruner
                                    .take()
                                    .vortex_expect("Already checked for presence")
                                    .into_parts();
                                self.state = StreamingState::FilterInit;
                                self.input = Some(input);
                                self.filter_reader = Some(filter_reader);
                                let pruned_rows = mem::take(&mut self.pruned_rows);
                                self.layout_reader.with_selected_rows(&pruned_rows);
                                if let Some(r) = self.filter_reader.as_mut() {
                                    r.with_selected_rows(&pruned_rows)
                                }
                            }
                            Some(rs) => self.pruned_rows.extend(rs?),
                        }
                    } else {
                        self.state = if self.filter_reader.is_some() {
                            StreamingState::FilterInit
                        } else {
                            StreamingState::Init
                        };
                    }
                }
                StreamingState::Error => return Poll::Ready(None),
            }
        }
    }
}

pub async fn read_ranges<R: VortexReadAt>(
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
