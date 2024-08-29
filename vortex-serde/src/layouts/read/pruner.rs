use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{ready, Context, Poll};

use bytes::Bytes;
use futures::Stream;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use vortex_error::{vortex_err, vortex_panic, VortexExpect, VortexResult};

use crate::io::VortexReadAt;
use crate::layouts::read::cache::LayoutMessageCache;
use crate::layouts::read::selection::RowSelector;
use crate::layouts::read::stream::read_ranges;
use crate::layouts::read::MessageId;
use crate::layouts::{LayoutReader, PlanResult, PruningScan};

pub struct LayoutPruner<R> {
    input: Option<R>,
    layout: Box<dyn LayoutReader>,
    scan: PruningScan,
    messages_cache: Arc<RwLock<LayoutMessageCache>>,
    state: PruningState<R>,
}

impl<R: VortexReadAt> LayoutPruner<R> {
    pub fn new(
        reader: R,
        layout: Box<dyn LayoutReader>,
        messages_cache: Arc<RwLock<LayoutMessageCache>>,
        scan: PruningScan,
    ) -> Self {
        Self {
            input: Some(reader),
            layout,
            scan,
            messages_cache,
            state: Default::default(),
        }
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

    pub fn into_parts(self) -> (R, Box<dyn LayoutReader>) {
        (self.input.vortex_expect("Must have input"), self.layout)
    }
}

impl<R> Debug for LayoutPruner<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayoutPruner")
            .field("layout", &self.layout)
            .field("scan", &self.scan)
            .field("messages_cache", &self.messages_cache)
            .finish()
    }
}

type PruneStateFuture<R> = BoxFuture<'static, VortexResult<(R, Vec<(MessageId, Bytes)>)>>;

#[derive(Default)]
enum PruningState<R> {
    #[default]
    Init,
    Reading(PruneStateFuture<R>),
    Error,
}

impl<R: VortexReadAt + Unpin + 'static> Stream for LayoutPruner<R> {
    type Item = VortexResult<RowSelector>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                PruningState::Init => {
                    let scan = self.scan.clone();
                    if let Some(read) = self.layout.plan(scan)? {
                        match read {
                            PlanResult::ReadMore(messages) => {
                                let reader = self
                                    .input
                                    .take()
                                    .ok_or_else(|| vortex_err!("Invalid state transition"))?;
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = PruningState::Reading(read_future);
                            }
                            PlanResult::Range(a) => {
                                self.state = PruningState::Init;
                                return Poll::Ready(Some(Ok(a)));
                            }
                            PlanResult::Batch(_) => {
                                Err(vortex_err!("Batch should never reach top level stream"))?
                            }
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
                PruningState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((input, buffers)) => {
                        self.store_messages(buffers);
                        self.input = Some(input);
                        self.state = PruningState::Init
                    }
                    Err(e) => {
                        self.state = PruningState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                PruningState::Error => return Poll::Ready(None),
            }
        }
    }
}
