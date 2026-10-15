// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scripted planners, morsels, and a scripted IO session for driver tests.
//!
//! A scripted object follows its script exactly and panics when the driver calls it against the
//! protocol, so a driver bug becomes a test failure that names the script position.

use std::ops::Range;
use std::sync::Arc;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use futures::channel::oneshot;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::segments::FetchTracker;
use vortex_io::segments::IoIntent;
use vortex_io::segments::IoOwnerId;
use vortex_io::segments::SegmentHandle;
use vortex_io::segments::SegmentId;
use vortex_io::segments::SegmentIoSession;
use vortex_io::segments::SegmentKey;
use vortex_io::segments::SegmentRequest;
use vortex_io::segments::SharedBytes;
use vortex_io::segments::SourceId;
use vortex_utils::aliases::hash_map::HashMap;

use crate::planning::morsel::Morsel;
use crate::planning::morsel::MorselOutput;
use crate::planning::planner::Planner;
use crate::planning::planner::PlannerOutput;
use crate::planning::planner::State;
use crate::planning::planner::WorkScope;

/// Prefix of the events a [`ScriptedIo`] records.
const IO: &str = "io ";

/// Shared record of the calls the driver made, as readable one-line events.
///
/// Planners and morsels record `compute`, `publish` and `deliver` events; a [`ScriptedIo`]
/// records `io submit`, `io read` and `io withdraw` events into the same log, so tests can
/// assert how IO interleaves with the work.
#[derive(Clone, Default)]
pub struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn push(&self, event: String) {
        self.0.lock().push(event);
    }

    /// Every event, in order.
    pub fn events(&self) -> Vec<String> {
        self.0.lock().clone()
    }

    /// The calls the driver made on planners and morsels, without IO events.
    pub fn calls(&self) -> Vec<String> {
        self.0
            .lock()
            .iter()
            .filter(|event| !event.starts_with(IO))
            .cloned()
            .collect()
    }

    /// The events that start with `prefix`.
    pub fn matching(&self, prefix: &str) -> Vec<String> {
        self.0
            .lock()
            .iter()
            .filter(|event| event.starts_with(prefix))
            .cloned()
            .collect()
    }
}

/// The key of segment `segment` in the only source the tests use.
pub fn key(segment: u32) -> SegmentKey {
    SegmentKey::new(SourceId(0), SegmentId::from(segment))
}

pub fn fetch(segment: u32) -> SegmentRequest {
    SegmentRequest::new(key(segment), IoIntent::Fetch)
}

pub fn announce(segment: u32) -> SegmentRequest {
    SegmentRequest::new(key(segment), IoIntent::Announce)
}

pub fn prefetch(segment: u32) -> SegmentRequest {
    SegmentRequest::new(key(segment), IoIntent::Prefetch)
}

pub fn forget(segment: u32) -> SegmentRequest {
    SegmentRequest::new(key(segment), IoIntent::Forget)
}

/// One scripted planner step. Every step but `Await` and `Wait` is the output of one `compute()`.
pub enum PlannerStep {
    Done,
    Continue,
    /// Publishes the requests and goes straight on to the next step.
    NeedsIO(Vec<SegmentRequest>),
    Planner(WorkScope, Vec<PlannerStep>),
    Morsel(WorkScope, Vec<MorselStep>),
    /// Publishes the requests, then waits until every fetch among them is delivered.
    Io(Vec<SegmentRequest>),
    /// Waits for fetches an earlier `NeedsIO` step published.
    Await(Vec<u32>),
    /// Reports `Waiting` without having published anything.
    Wait,
}

/// One scripted morsel step.
pub enum MorselStep {
    Compute(MorselOutput),
    /// Publishes the requests, then waits until every fetch among them is delivered.
    Io(Vec<SegmentRequest>),
}

/// The segments a waiting step still expects.
struct Pending(Vec<SegmentKey>);

impl Pending {
    fn of(batch: &[SegmentRequest]) -> Self {
        Self(
            batch
                .iter()
                .filter(|request| request.intent == IoIntent::Fetch)
                .map(|request| request.key)
                .collect(),
        )
    }

    /// Removes a delivered segment and returns whether the step has everything it waited for.
    fn deliver(&mut self, name: &str, position: usize, key: SegmentKey) -> bool {
        let index = self
            .0
            .iter()
            .position(|pending| *pending == key)
            .unwrap_or_else(|| {
                panic!("{name}: delivery of unexpected segment {key:?} at step {position}")
            });
        self.0.remove(index);
        self.0.is_empty()
    }
}

fn log_delivery(log: &Log, name: &str, key: SegmentKey, bytes: &BufferHandle) {
    let expected = expected_bytes(*key.segment);
    assert_eq!(
        bytes.len(),
        expected.len(),
        "{name}: delivered bytes of the wrong segment"
    );
    log.push(format!("deliver {name} {}", *key.segment));
}

pub struct ScriptedPlanner {
    name: &'static str,
    script: Vec<PlannerStep>,
    position: usize,
    /// What the step at `position` waits for, once it has published or received something.
    pending: Option<Pending>,
    log: Log,
}

impl ScriptedPlanner {
    pub fn new(name: &'static str, script: Vec<PlannerStep>, log: &Log) -> Self {
        Self {
            name,
            script,
            position: 0,
            pending: None,
            log: log.clone(),
        }
    }

    /// A fresh scripted planner, boxed as the driver takes it.
    pub fn boxed(name: &'static str, script: Vec<PlannerStep>, log: &Log) -> Box<dyn Planner> {
        Box::new(Self::new(name, script, log))
    }
}

impl Planner for ScriptedPlanner {
    fn state(&self) -> State {
        match self.script.get(self.position) {
            None => State::Done,
            Some(PlannerStep::Io(_)) if self.pending.is_none() => State::NeedsCompute,
            Some(PlannerStep::Io(_) | PlannerStep::Await(_) | PlannerStep::Wait) => State::Waiting,
            Some(_) => State::NeedsCompute,
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        let position = self.position;
        if let Some(PlannerStep::Io(batch)) = self.script.get(position) {
            assert!(
                self.pending.is_none(),
                "{}: compute called while script step {position} waits for IO",
                self.name
            );
            self.log.push(format!("publish {}", self.name));
            // A batch with nothing to wait for is left for the driver to reject.
            self.pending = Some(Pending::of(batch));
            return Ok(PlannerOutput::NeedsIO(batch.clone()));
        }
        self.log.push(format!("compute {}", self.name));
        self.position += 1;
        let Some(slot) = self.script.get_mut(position) else {
            panic!("{}: compute past the end of the script", self.name);
        };
        let step = std::mem::replace(slot, PlannerStep::Done);
        Ok(match step {
            PlannerStep::Done => PlannerOutput::Done,
            PlannerStep::Continue => PlannerOutput::Continue,
            PlannerStep::NeedsIO(batch) => PlannerOutput::NeedsIO(batch),
            PlannerStep::Planner(scope, script) => {
                PlannerOutput::Planner(scope, Self::boxed("child", script, &self.log))
            }
            PlannerStep::Morsel(scope, script) => PlannerOutput::Morsel(
                scope,
                Box::new(ScriptedMorsel::new("morsel", script, &self.log)),
            ),
            PlannerStep::Io(_) | PlannerStep::Await(_) | PlannerStep::Wait => panic!(
                "{}: compute called while script step {position} waits for IO",
                self.name
            ),
        })
    }

    fn deliver(&mut self, key: SegmentKey, bytes: BufferHandle) {
        log_delivery(&self.log, self.name, key, &bytes);
        let position = self.position;
        if let Some(PlannerStep::Await(segments)) = self.script.get(position)
            && self.pending.is_none()
        {
            self.pending = Some(Pending(
                segments.iter().map(|segment| self::key(*segment)).collect(),
            ));
        }
        let Some(pending) = self.pending.as_mut() else {
            panic!(
                "{}: unexpected delivery at script step {position}",
                self.name
            );
        };
        if pending.deliver(self.name, position, key) {
            self.pending = None;
            self.position += 1;
        }
    }
}

pub struct ScriptedMorsel {
    name: &'static str,
    script: Vec<MorselStep>,
    position: usize,
    pending: Option<Pending>,
    log: Log,
}

impl ScriptedMorsel {
    pub fn new(name: &'static str, script: Vec<MorselStep>, log: &Log) -> Self {
        Self {
            name,
            script,
            position: 0,
            pending: None,
            log: log.clone(),
        }
    }
}

impl Morsel for ScriptedMorsel {
    fn state(&self) -> State {
        match self.script.get(self.position) {
            None => State::Done,
            Some(MorselStep::Io(_)) if self.pending.is_some() => State::Waiting,
            Some(_) => State::NeedsCompute,
        }
    }

    fn compute(&mut self) -> VortexResult<MorselOutput> {
        let position = self.position;
        match self.script.get_mut(position) {
            Some(MorselStep::Io(batch)) => {
                assert!(
                    self.pending.is_none(),
                    "{}: compute called while script step {position} waits for IO",
                    self.name
                );
                let batch = batch.clone();
                self.log.push(format!("publish {}", self.name));
                self.pending = Some(Pending::of(&batch));
                Ok(MorselOutput::NeedsIO(batch))
            }
            Some(MorselStep::Compute(output)) => {
                self.log.push(format!("compute {}", self.name));
                self.position += 1;
                Ok(std::mem::replace(output, MorselOutput::Done))
            }
            None => panic!("{}: compute past the end of the script", self.name),
        }
    }

    fn deliver(&mut self, key: SegmentKey, bytes: BufferHandle) {
        log_delivery(&self.log, self.name, key, &bytes);
        let position = self.position;
        let Some(pending) = self.pending.as_mut() else {
            panic!(
                "{}: unexpected delivery at script step {position}",
                self.name
            );
        };
        if pending.deliver(self.name, position, key) {
            self.pending = None;
            self.position += 1;
        }
    }
}

/// The bytes every read of `segment` answers with: one byte per segment number, plus one.
pub fn expected_bytes(segment: u32) -> ByteBuffer {
    let len = usize::try_from(segment).expect("small segment") + 1;
    ByteBuffer::copy_from(vec![u8::try_from(segment % 256).expect("byte"); len])
}

/// When a [`ScriptedIo`] completes the reads that were fetched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    /// Only when the test calls [`ScriptedIo::complete`].
    Manual,
    /// One read per `poll_ready`, the earliest fetched first.
    Fifo,
    /// One read per `poll_ready`, the latest fetched first.
    Lifo,
}

/// Keeps one read registered while any handle of it lives, and records its withdrawal.
struct Registration {
    segment: u32,
    log: Log,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.log.push(format!("{IO}withdraw {}", self.segment));
    }
}

type ReadResult = Result<BufferHandle, Arc<VortexError>>;

/// One registered read, shared by every handle of its segment while any of them lives.
struct Read {
    bytes: SharedBytes,
    registration: Weak<Registration>,
    sender: Option<oneshot::Sender<ReadResult>>,
    /// The order of the first fetch of the read; announced or prefetched reads are never read
    /// on their own.
    fetched: Option<u64>,
}

#[derive(Default)]
struct IoState {
    reads: HashMap<SegmentKey, Read>,
    failures: HashMap<SegmentKey, String>,
    cached: HashMap<SegmentKey, VortexResult<BufferHandle>>,
    tracker: FetchTracker,
    submissions: Vec<(IoOwnerId, SegmentRequest)>,
    performed: Vec<u32>,
    next_fetch: u64,
}

/// A [`SegmentIoSession`] whose reads complete when the test, or its [`Completion`] policy, says
/// so.
///
/// Like a real service, it shares one read between every request of a segment while any handle
/// of it lives, and withdraws the read when the last handle is dropped. It records every
/// submission, every read it performs, and every withdrawal into the [`Log`].
pub struct ScriptedIo {
    state: Mutex<IoState>,
    completion: Completion,
    log: Log,
}

impl ScriptedIo {
    pub fn new(completion: Completion, log: &Log) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::default(),
            completion,
            log: log.clone(),
        })
    }

    /// Completes fetched reads in fetch order whenever the driver polls.
    pub fn auto(log: &Log) -> Arc<Self> {
        Self::new(Completion::Fifo, log)
    }

    /// Completes fetched reads only when the test calls [`complete`](Self::complete).
    pub fn manual(log: &Log) -> Arc<Self> {
        Self::new(Completion::Manual, log)
    }

    /// The session as the driver takes it.
    pub fn session(self: &Arc<Self>) -> Arc<dyn SegmentIoSession> {
        Arc::clone(self) as Arc<dyn SegmentIoSession>
    }

    /// Makes reads of `segment` fail with `message`.
    pub fn fail(&self, segment: u32, message: &str) {
        self.state
            .lock()
            .failures
            .insert(key(segment), message.to_string());
    }

    /// Answers fetches of `segment` from a cache, with handles that already hold the result.
    pub fn cache(&self, segment: u32, result: VortexResult<BufferHandle>) {
        self.state.lock().cached.insert(key(segment), result);
    }

    /// Completes the registered read of `segment` with its answer.
    pub fn complete(&self, segment: u32) -> VortexResult<()> {
        let mut state = self.state.lock();
        let key = key(segment);
        let Some(read) = state.reads.get_mut(&key) else {
            vortex_bail!("no read of segment {segment} is registered");
        };
        if read.registration.strong_count() == 0 {
            vortex_bail!("the read of segment {segment} was withdrawn");
        }
        let Some(sender) = read.sender.take() else {
            vortex_bail!("the read of segment {segment} already completed");
        };
        let result = match state.failures.get(&key) {
            Some(message) => Err(Arc::new(vortex_err!("{message}"))),
            None => Ok(BufferHandle::new_host(expected_bytes(segment))),
        };
        // The receiver lives in the read's shared future, which every handle holds.
        drop(sender.send(result));
        state.performed.push(segment);
        self.log.push(format!("{IO}read {segment}"));
        Ok(())
    }

    /// Every request the driver submitted, with its owner, in submission order.
    pub fn submissions(&self) -> Vec<(IoOwnerId, SegmentRequest)> {
        self.state.lock().submissions.clone()
    }

    /// The segments read, in completion order.
    pub fn performed(&self) -> Vec<u32> {
        self.state.lock().performed.clone()
    }

    /// The segments with a registered read some handle still holds, in segment order.
    pub fn live_reads(&self) -> Vec<u32> {
        let mut live: Vec<u32> = self
            .state
            .lock()
            .reads
            .iter()
            .filter(|(_, read)| read.registration.strong_count() > 0)
            .map(|(key, _)| *key.segment)
            .collect();
        live.sort_unstable();
        live
    }

    /// The fetched read the completion policy completes next, if any.
    fn next_to_complete(&self) -> Option<u32> {
        let state = self.state.lock();
        let candidates = state.reads.iter().filter_map(|(key, read)| {
            let alive = read.registration.strong_count() > 0 && read.sender.is_some();
            read.fetched
                .filter(|_| alive)
                .map(|order| (order, *key.segment))
        });
        match self.completion {
            Completion::Manual => None,
            Completion::Fifo => candidates.min().map(|(_, segment)| segment),
            Completion::Lifo => candidates.max().map(|(_, segment)| segment),
        }
    }

    /// A handle of the read of `key`, registering a new read unless a live one exists.
    fn handle(&self, state: &mut IoState, key: SegmentKey) -> SegmentHandle {
        if let Some(read) = state.reads.get(&key)
            && let Some(registration) = read.registration.upgrade()
        {
            return SegmentHandle::new(read.bytes.clone(), registration);
        }
        let (sender, receiver) = oneshot::channel::<ReadResult>();
        let bytes = receiver
            .map(|result| {
                result.unwrap_or_else(|_| Err(Arc::new(vortex_err!("read was cancelled"))))
            })
            .boxed()
            .shared();
        let registration = Arc::new(Registration {
            segment: *key.segment,
            log: self.log.clone(),
        });
        state.reads.insert(
            key,
            Read {
                bytes: bytes.clone(),
                registration: Arc::downgrade(&registration),
                sender: Some(sender),
                fetched: None,
            },
        );
        SegmentHandle::new(bytes, registration)
    }
}

impl SegmentIoSession for ScriptedIo {
    fn submit(
        &self,
        owner: IoOwnerId,
        batch: &[SegmentRequest],
    ) -> VortexResult<Vec<SegmentHandle>> {
        if batch.is_empty() {
            vortex_bail!("ScriptedIo: empty submission");
        }
        let described: Vec<String> = batch
            .iter()
            .map(|request| format!("{:?} {}", request.intent, *request.key.segment))
            .collect();
        self.log
            .push(format!("{IO}submit {}: {}", owner.0, described.join(", ")));
        let mut state = self.state.lock();
        let mut handles = Vec::with_capacity(batch.len());
        for request in batch {
            state.submissions.push((owner, *request));
            match request.intent {
                IoIntent::Forget => vortex_bail!("ScriptedIo: a submission held a Forget"),
                IoIntent::Fetch => {
                    if let Some(cached) = state.cached.get(&request.key) {
                        let result = match cached {
                            Ok(bytes) => Ok(bytes.clone()),
                            Err(err) => Err(vortex_err!("{err}")),
                        };
                        handles.push(SegmentHandle::ready(result));
                        continue;
                    }
                    let handle = self.handle(&mut state, request.key);
                    let order = state.next_fetch;
                    state.next_fetch += 1;
                    if let Some(read) = state.reads.get_mut(&request.key) {
                        read.fetched.get_or_insert(order);
                    }
                    state.tracker.track(owner, &handle);
                    handles.push(handle);
                }
                IoIntent::Announce | IoIntent::Prefetch => {
                    let handle = self.handle(&mut state, request.key);
                    handles.push(handle);
                }
            }
        }
        Ok(handles)
    }

    fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        ready: &mut Vec<IoOwnerId>,
    ) -> Poll<VortexResult<()>> {
        if let Some(segment) = self.next_to_complete() {
            // The policy only picks live reads that have not completed, so this cannot fail.
            drop(self.complete(segment));
        }
        self.state.lock().tracker.poll_ready(cx, ready)
    }
}

/// A small non-nullable `u32` array of `n` rows.
pub fn array_of(n: u64) -> ArrayRef {
    PrimitiveArray::from_iter(0..u32::try_from(n).expect("small test array")).into_array()
}

/// An execution context over a bare array session, for `assert_arrays_eq!`.
pub fn ctx() -> ExecutionCtx {
    array_session().create_execution_ctx()
}

pub fn scope(rows: Range<u64>) -> WorkScope {
    WorkScope {
        file_ordinal: 0,
        rows,
    }
}
