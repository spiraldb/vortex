// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Wake;
use std::thread;
use std::thread::Thread;

use futures::StreamExt;
use rstest::rstest;
use vortex_buffer::buffer;
use vortex_io::request::IoIntent;
use vortex_io::request::IoRequest;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoSource;
use vortex_io::request::IoTarget;

use super::*;
use crate::planning::morsel::MorselOutput;
use crate::planning::tests::scripted::Log;
use crate::planning::tests::scripted::MorselStep;
use crate::planning::tests::scripted::PlannerStep;
use crate::planning::tests::scripted::RecordingIoSource;
use crate::planning::tests::scripted::ScriptedPlanner;
use crate::planning::tests::scripted::array_of;
use crate::planning::tests::scripted::scope;

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

/// Runs each worker on a thread of its own.
fn threads() -> Spawn {
    Arc::new(|worker| {
        thread::spawn(move || block_on(worker));
    })
}

/// Gives every session a source of its own, with the bytes at offset 0.
struct Sessions;

impl IoService for Sessions {
    fn session(&self) -> Arc<dyn IoSource> {
        let source = RecordingIoSource::default();
        source.canned_bytes(0, buffer![0u8, 1, 2, 3].into_byte_buffer());
        Arc::new(source)
    }
}

fn lengths() -> MapFn<usize> {
    Arc::new(|array: ArrayRef| Ok(array.len()))
}

/// A unit that reads a segment, then emits a morsel with `rows` rows over `start..start + 1`.
fn unit(start: u64, rows: u64) -> PlannerStep {
    PlannerStep::Planner(
        scope(start..start + 1),
        vec![
            PlannerStep::Io(vec![IoRequest {
                intent: IoIntent::Fetch,
                request: IoRequestId(0),
                target: IoTarget::range(0, 4),
            }]),
            PlannerStep::Morsel(
                scope(start..start + 1),
                vec![
                    MorselStep::Compute(MorselOutput::Batch(array_of(rows))),
                    MorselStep::Compute(MorselOutput::Done),
                ],
            ),
            PlannerStep::Done,
        ],
    )
}

fn run(root: Vec<PlannerStep>, options: PoolOptions) -> Vec<VortexResult<usize>> {
    let log = Log::default();
    let stream = Pool::stream(
        ScriptedPlanner::boxed("file", root, &log),
        scope(0..0),
        Arc::new(Sessions),
        lengths(),
        threads(),
        options,
    );
    block_on(stream.collect())
}

fn options(max_workers: usize, ordered: bool) -> PoolOptions {
    PoolOptions {
        max_workers,
        units_per_worker: 2,
        ordered,
    }
}

#[rstest]
#[case::one_worker(1)]
#[case::several_workers(3)]
fn ordered_output_follows_hand_out_order(#[case] workers: usize) -> VortexResult<()> {
    let mut root: Vec<_> = (1..=8).map(|rows| unit(rows, rows)).collect();
    root.push(PlannerStep::Done);
    let lengths = run(root, options(workers, true))
        .into_iter()
        .collect::<VortexResult<Vec<_>>>()?;
    assert_eq!(lengths, (1..=8).collect::<Vec<usize>>());
    Ok(())
}

#[test]
fn unordered_output_yields_every_batch() -> VortexResult<()> {
    let mut root: Vec<_> = (1..=8).map(|rows| unit(rows, rows)).collect();
    root.push(PlannerStep::Done);
    let mut lengths = run(root, options(3, false))
        .into_iter()
        .collect::<VortexResult<Vec<_>>>()?;
    lengths.sort_unstable();
    assert_eq!(lengths, (1..=8).collect::<Vec<usize>>());
    Ok(())
}

#[test]
fn a_root_with_no_units_ends_the_stream() {
    assert!(run(vec![PlannerStep::Done], options(2, true)).is_empty());
}

#[test]
fn a_failing_unit_ends_the_stream_with_its_error() {
    let root = vec![
        unit(0, 1),
        PlannerStep::Planner(scope(1..2), vec![PlannerStep::Wait]),
        PlannerStep::Done,
    ];
    let outputs = run(root, options(2, true));
    let error = outputs
        .iter()
        .find_map(|output| output.as_ref().err())
        .map(|error| error.to_string());
    assert!(
        error
            .as_deref()
            .is_some_and(|message| message.contains("no outstanding fetch")),
        "{error:?}"
    );
}
