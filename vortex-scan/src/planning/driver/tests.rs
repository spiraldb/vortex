// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::task::Waker;

use rstest::rstest;
use vortex_array::assert_arrays_eq;
use vortex_error::VortexResult;

use super::*;
use crate::planning::tests::scripted::Completion;
use crate::planning::tests::scripted::Log;
use crate::planning::tests::scripted::MorselStep;
use crate::planning::tests::scripted::PlannerStep;
use crate::planning::tests::scripted::ScriptedIo;
use crate::planning::tests::scripted::ScriptedPlanner;
use crate::planning::tests::scripted::announce;
use crate::planning::tests::scripted::array_of;
use crate::planning::tests::scripted::ctx;
use crate::planning::tests::scripted::expected_bytes;
use crate::planning::tests::scripted::fetch;
use crate::planning::tests::scripted::forget;
use crate::planning::tests::scripted::prefetch;
use crate::planning::tests::scripted::scope;

fn driver(io: &Arc<ScriptedIo>) -> Driver {
    Driver::new(io.session()).with_step_limit(1_000)
}

fn one_batch_morsel(scope: WorkScope, rows: u64) -> PlannerStep {
    PlannerStep::Morsel(
        scope,
        vec![
            MorselStep::Compute(MorselOutput::Batch(array_of(rows))),
            MorselStep::Compute(MorselOutput::Done),
        ],
    )
}

fn error_message<T>(result: VortexResult<T>) -> Option<String> {
    result.err().map(|err| err.to_string())
}

#[test]
fn root_done_produces_nothing() -> VortexResult<()> {
    let log = Log::default();
    let root = ScriptedPlanner::boxed("root", vec![PlannerStep::Done], &log);
    let batches = driver(&ScriptedIo::auto(&log)).run(root)?;
    assert!(batches.is_empty());
    assert_eq!(log.events(), vec!["compute root"]);
    Ok(())
}

#[test]
fn one_morsel_one_batch() -> VortexResult<()> {
    let log = Log::default();
    let root = ScriptedPlanner::boxed(
        "root",
        vec![one_batch_morsel(scope(10..20), 3), PlannerStep::Done],
        &log,
    );
    let batches = driver(&ScriptedIo::auto(&log)).run(root)?;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].scope, scope(10..20));
    assert_arrays_eq!(batches[0].array, array_of(3), &mut ctx());
    Ok(())
}

#[test]
fn two_morsels_emit_in_order() -> VortexResult<()> {
    let log = Log::default();
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            one_batch_morsel(scope(0..1), 1),
            one_batch_morsel(scope(1..3), 2),
            PlannerStep::Done,
        ],
        &log,
    );
    let batches = driver(&ScriptedIo::auto(&log)).run(root)?;
    let scopes: Vec<_> = batches.iter().map(|batch| batch.scope.clone()).collect();
    assert_eq!(scopes, vec![scope(0..1), scope(1..3)]);
    assert_arrays_eq!(batches[0].array, array_of(1), &mut ctx());
    assert_arrays_eq!(batches[1].array, array_of(2), &mut ctx());
    Ok(())
}

#[test]
fn dfs_finishes_child_subtree_before_parent_continues() -> VortexResult<()> {
    let log = Log::default();
    let child = PlannerStep::Planner(
        scope(0..5),
        vec![one_batch_morsel(scope(0..5), 1), PlannerStep::Done],
    );
    let root = ScriptedPlanner::boxed(
        "parent",
        vec![
            child,
            PlannerStep::Continue,
            one_batch_morsel(scope(5..10), 2),
            PlannerStep::Done,
        ],
        &log,
    );
    let batches = driver(&ScriptedIo::auto(&log)).run(root)?;
    let scopes: Vec<_> = batches.iter().map(|batch| batch.scope.clone()).collect();
    assert_eq!(scopes, vec![scope(0..5), scope(5..10)]);
    assert_eq!(
        log.events(),
        vec![
            "compute parent",
            "compute child",
            "compute morsel",
            "compute morsel",
            "compute child",
            "compute parent",
            "compute parent",
            "compute morsel",
            "compute morsel",
            "compute parent",
        ]
    );
    Ok(())
}

#[test]
fn continue_requeues() -> VortexResult<()> {
    let log = Log::default();
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::Continue,
            PlannerStep::Continue,
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&ScriptedIo::auto(&log)).run(root)?;
    assert_eq!(log.calls(), vec!["compute root"; 3]);
    Ok(())
}

/// Every fetch of a batch is registered in one submission, and each is delivered, and its read
/// withdrawn, as soon as its bytes arrive, in whatever order the reads finish.
#[rstest]
#[case::fifo(Completion::Fifo, [1, 2])]
#[case::lifo(Completion::Lifo, [2, 1])]
fn fetches_are_delivered_as_reads_finish(
    #[case] completion: Completion,
    #[case] order: [u32; 2],
) -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::new(completion, &log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![PlannerStep::Io(vec![fetch(1), fetch(2)]), PlannerStep::Done],
        &log,
    );
    assert!(driver(&io).run(root)?.is_empty());
    let mut expected = vec![
        "publish root".to_string(),
        "io submit 0: Fetch 1, Fetch 2".to_string(),
    ];
    for segment in order {
        expected.push(format!("io read {segment}"));
        expected.push(format!("io withdraw {segment}"));
        expected.push(format!("deliver root {segment}"));
    }
    expected.push("compute root".to_string());
    assert_eq!(log.events(), expected);
    assert_eq!(io.performed(), order);
    Ok(())
}

/// A fetch published while CPU work is left is read once, and delivered when it arrives even
/// though the planner only waits for it later.
#[test]
fn fetch_published_ahead_is_delivered_when_awaited() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::NeedsIO(vec![fetch(1)]),
            PlannerStep::Continue,
            PlannerStep::Await(vec![1]),
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&io).run(root)?;
    assert_eq!(io.performed(), vec![1]);
    assert_eq!(
        log.calls(),
        vec![
            "compute root",
            "compute root",
            "deliver root 1",
            "compute root",
        ]
    );
    Ok(())
}

/// Once its bytes are delivered a fetch is no longer outstanding, so the item may fetch the
/// segment again, which reads it again.
#[test]
fn a_delivered_segment_can_be_fetched_again() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::Io(vec![fetch(1)]),
            PlannerStep::Io(vec![fetch(1)]),
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&io).run(root)?;
    assert_eq!(io.performed(), vec![1, 1]);
    assert_eq!(log.matching("deliver"), vec!["deliver root 1"; 2]);
    Ok(())
}

#[test]
fn failed_read_fails_the_run() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    io.fail(2, "disk on fire");
    let root = ScriptedPlanner::boxed(
        "root",
        vec![PlannerStep::Io(vec![fetch(1), fetch(2)]), PlannerStep::Done],
        &log,
    );
    let err = error_message(driver(&io).run(root));
    assert!(
        err.as_deref().is_some_and(|m| m.contains("disk on fire")),
        "{err:?}"
    );
    assert_eq!(io.performed(), vec![1, 2]);
    assert_eq!(log.matching("deliver"), vec!["deliver root 1"]);
    Ok(())
}

#[test]
fn completions_out_of_order_across_parked_items() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::new(Completion::Lifo, &log);
    let child = |segment: u32| {
        PlannerStep::Planner(
            scope(0..1),
            vec![PlannerStep::Io(vec![fetch(segment)]), PlannerStep::Done],
        )
    };
    let root = ScriptedPlanner::boxed("root", vec![child(5), child(6), PlannerStep::Done], &log);
    driver(&io).run(root)?;
    assert_eq!(
        log.matching("deliver"),
        vec!["deliver child 6", "deliver child 5"],
        "both children were parked before either read finished"
    );
    Ok(())
}

/// Items that fetch the same segment share one read, and each is delivered its bytes.
#[test]
fn fetches_of_one_segment_by_different_items_share_a_read() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let child = PlannerStep::Planner(
        scope(0..1),
        vec![PlannerStep::Io(vec![fetch(3)]), PlannerStep::Done],
    );
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::NeedsIO(vec![fetch(3)]),
            child,
            PlannerStep::Await(vec![3]),
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&io).run(root)?;
    assert_eq!(io.performed(), vec![3]);
    let mut deliveries = log.matching("deliver");
    deliveries.sort();
    assert_eq!(deliveries, vec!["deliver child 3", "deliver root 3"]);
    assert_eq!(log.matching("io withdraw"), vec!["io withdraw 3"]);
    Ok(())
}

#[test]
fn morsel_needs_io_midway() -> VortexResult<()> {
    let log = Log::default();
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::Morsel(
                scope(0..4),
                vec![
                    MorselStep::Compute(MorselOutput::Batch(array_of(1))),
                    MorselStep::Io(vec![fetch(0)]),
                    MorselStep::Compute(MorselOutput::Batch(array_of(2))),
                    MorselStep::Compute(MorselOutput::Done),
                ],
            ),
            PlannerStep::Done,
        ],
        &log,
    );
    let batches = driver(&ScriptedIo::auto(&log)).run(root)?;
    assert_eq!(batches.len(), 2);
    assert_arrays_eq!(batches[0].array, array_of(1), &mut ctx());
    assert_arrays_eq!(batches[1].array, array_of(2), &mut ctx());
    assert_eq!(log.matching("deliver"), vec!["deliver morsel 0"]);
    Ok(())
}

#[rstest]
#[case::empty_batch(
    vec![PlannerStep::Morsel(scope(0..1), vec![MorselStep::Compute(MorselOutput::Batch(array_of(0)))])],
    "empty batch"
)]
#[case::empty_io(vec![PlannerStep::Io(vec![])], "empty batch")]
#[case::republished_fetch(
    vec![PlannerStep::NeedsIO(vec![fetch(0)]), PlannerStep::NeedsIO(vec![fetch(0)])],
    "published while it is outstanding"
)]
#[case::duplicate_fetch_in_one_batch(
    vec![PlannerStep::NeedsIO(vec![fetch(0), fetch(0)])],
    "published while it is outstanding"
)]
#[case::waiting_for_nothing(vec![PlannerStep::Wait], "waits with no outstanding fetch")]
#[case::still_waiting_after_the_last_delivery(
    vec![PlannerStep::Io(vec![fetch(0)]), PlannerStep::Wait],
    "waits with no outstanding fetch"
)]
#[case::waiting_on_an_announcement(
    vec![PlannerStep::Io(vec![announce(0)])],
    "waits with no outstanding fetch"
)]
#[case::waiting_on_a_prefetch(
    vec![PlannerStep::Io(vec![prefetch(0)])],
    "waits with no outstanding fetch"
)]
#[case::waiting_on_a_forget(
    vec![PlannerStep::Io(vec![forget(0)])],
    "waits with no outstanding fetch"
)]
fn protocol_errors(#[case] mut steps: Vec<PlannerStep>, #[case] message: &str) -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    steps.push(PlannerStep::Done);
    let root = ScriptedPlanner::boxed("root", steps, &log);
    let err = error_message(driver(&io).run(root));
    assert!(
        err.as_deref().is_some_and(|m| m.contains(message)),
        "{err:?}"
    );
    assert!(io.performed().len() <= 1);
    Ok(())
}

#[test]
fn step_limit_is_enforced() -> VortexResult<()> {
    let log = Log::default();
    let mut steps: Vec<_> = (0..100).map(|_| PlannerStep::Continue).collect();
    steps.push(PlannerStep::Done);
    let root = ScriptedPlanner::boxed("root", steps, &log);
    let driver = Driver::new(ScriptedIo::auto(&log).session()).with_step_limit(10);
    let err = error_message(driver.run(root));
    assert!(
        err.as_deref()
            .is_some_and(|m| m.contains("step limit of 10")),
        "{err:?}"
    );
    Ok(())
}

/// A failed read fails the run whether its bytes arrive later or are already there at submit.
#[rstest]
#[case::from_cache(true)]
#[case::from_a_read(false)]
fn failed_fetch_fails_the_run(#[case] cached: bool) -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    if cached {
        io.cache(1, Err(vortex_err!("checksum mismatch")));
    } else {
        io.fail(1, "checksum mismatch");
    }
    let root = ScriptedPlanner::boxed(
        "root",
        vec![PlannerStep::Io(vec![fetch(1)]), PlannerStep::Done],
        &log,
    );
    let err = error_message(driver(&io).run(root));
    assert!(
        err.as_deref()
            .is_some_and(|m| m.contains("checksum mismatch")),
        "{err:?}"
    );
    assert!(log.matching("deliver").is_empty());
    Ok(())
}

/// A fetch answered with a ready handle, such as a cache hit, is delivered while it is submitted,
/// so the item never parks and the run never waits.
#[test]
fn ready_fetch_is_delivered_at_submit() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::manual(&log);
    io.cache(1, Ok(BufferHandle::new_host(expected_bytes(1))));
    let root = ScriptedPlanner::boxed(
        "root",
        vec![PlannerStep::Io(vec![fetch(1)]), PlannerStep::Done],
        &log,
    );
    let mut run = Run::new().with_step_limit(1_000);
    run.admit(root, scope(0..1), io.session());
    assert_eq!(drain(&mut run)?, vec!["done RootId(0)", "idle"]);
    assert_eq!(
        log.events(),
        vec![
            "publish root",
            "io submit 0: Fetch 1",
            "deliver root 1",
            "compute root",
        ]
    );
    assert!(io.performed().is_empty());
    Ok(())
}

/// Announcements and prefetches are registered, but nobody waits for them, they are never read
/// on their own, and nothing is delivered. The root holds them until it finishes.
#[rstest]
#[case::announce(IoIntent::Announce)]
#[case::prefetch(IoIntent::Prefetch)]
fn hints_are_held_by_the_root_until_it_finishes(#[case] intent: IoIntent) -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let hint = SegmentRequest::new(fetch(1).key, intent);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::NeedsIO(vec![hint]),
            one_batch_morsel(scope(0..1), 1),
            PlannerStep::Done,
        ],
        &log,
    );
    let batches = driver(&io).run(root)?;
    assert_eq!(batches.len(), 1);
    assert_eq!(io.submissions(), vec![(IoOwnerId(0), hint)]);
    assert!(io.performed().is_empty());
    assert_eq!(
        log.events(),
        vec![
            "compute root".to_string(),
            format!("io submit 0: {intent:?} 1"),
            "compute root".to_string(),
            "compute morsel".to_string(),
            "compute morsel".to_string(),
            "compute root".to_string(),
            "io withdraw 1".to_string(),
        ]
    );
    Ok(())
}

/// A fetch consumes one hint of its segment. The read stays registered after the delivery only
/// while the root holds another hint of it; each announcement is a hint, but a prefetch adds
/// one only when the root holds none.
#[rstest]
#[case::one_announcement(vec![announce(1)], false)]
#[case::two_announcements(vec![announce(1), announce(1)], true)]
#[case::two_prefetches(vec![prefetch(1), prefetch(1)], false)]
#[case::announced_then_prefetched(vec![announce(1), prefetch(1)], false)]
#[case::prefetched_then_announced(vec![prefetch(1), announce(1)], true)]
fn fetch_consumes_one_hint(
    #[case] hints: Vec<SegmentRequest>,
    #[case] still_hinted: bool,
) -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::NeedsIO(hints),
            PlannerStep::Io(vec![fetch(1)]),
            PlannerStep::Continue,
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&io).run(root)?;
    assert_eq!(io.performed(), vec![1], "the fetch read the hinted segment");
    let events = log.events();
    let delivered = events
        .iter()
        .position(|event| event == "deliver root 1")
        .ok_or_else(|| vortex_err!("not delivered: {events:?}"))?;
    let withdrawn = events
        .iter()
        .position(|event| event == "io withdraw 1")
        .ok_or_else(|| vortex_err!("not withdrawn: {events:?}"))?;
    assert_eq!(
        withdrawn > delivered,
        still_hinted,
        "a remaining hint keeps the read until the root finishes: {events:?}"
    );
    assert_eq!(log.matching("io withdraw"), vec!["io withdraw 1"]);
    assert_eq!(
        events.last().map(String::as_str),
        if still_hinted {
            Some("io withdraw 1")
        } else {
            Some("compute root")
        }
    );
    Ok(())
}

/// A `Forget` drops every hint the root holds for the segment at once, is never submitted, and
/// leaves other segments' hints alone.
#[test]
fn forget_withdraws_every_hint_of_its_segment() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::NeedsIO(vec![announce(1), announce(1), prefetch(2)]),
            PlannerStep::NeedsIO(vec![forget(1)]),
            PlannerStep::Continue,
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&io).run(root)?;
    assert_eq!(
        log.events(),
        vec![
            "compute root",
            "io submit 0: Announce 1, Announce 1, Prefetch 2",
            "compute root",
            "io withdraw 1",
            "compute root",
            "compute root",
            "io withdraw 2",
        ]
    );
    assert!(
        io.submissions()
            .iter()
            .all(|(_, request)| request.intent != IoIntent::Forget)
    );
    assert!(io.performed().is_empty());
    Ok(())
}

/// A batch applies in order: a `Forget` withdraws the hints published before it in the same batch,
/// and keeps those published after it.
#[rstest]
#[case::forget_after_announce(vec![announce(1), forget(1)], vec!["compute root", "io submit 0: Announce 1", "io withdraw 1", "compute root", "compute root"])]
#[case::forget_before_announce(vec![forget(1), announce(1)], vec!["compute root", "io submit 0: Announce 1", "compute root", "compute root", "io withdraw 1"])]
fn forget_applies_in_batch_order(
    #[case] batch: Vec<SegmentRequest>,
    #[case] expected: Vec<&str>,
) -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::NeedsIO(batch),
            PlannerStep::Continue,
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&io).run(root)?;
    assert_eq!(log.events(), expected);
    Ok(())
}

/// A `Forget` published with other requests withdraws the hints, and the rest are submitted.
#[test]
fn forget_alongside_other_requests() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::NeedsIO(vec![announce(1)]),
            PlannerStep::Io(vec![forget(1), fetch(2), announce(3)]),
            PlannerStep::Done,
        ],
        &log,
    );
    driver(&io).run(root)?;
    assert_eq!(
        log.events(),
        vec![
            "compute root",
            "io submit 0: Announce 1",
            "publish root",
            "io withdraw 1",
            "io submit 0: Fetch 2, Announce 3",
            "io read 2",
            "io withdraw 2",
            "deliver root 2",
            "compute root",
            "io withdraw 3",
        ]
    );
    Ok(())
}

const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<Run>();
};

fn ready_root(name: &'static str, log: &Log, rows: Range<u64>) -> Box<dyn Planner> {
    ScriptedPlanner::boxed(
        name,
        vec![one_batch_morsel(scope(rows), 1), PlannerStep::Done],
        log,
    )
}

/// Fetches segment 1 and announces segment 2, then emits one batch of two rows.
fn reading_root(name: &'static str, log: &Log, rows: Range<u64>) -> Box<dyn Planner> {
    ScriptedPlanner::boxed(
        name,
        vec![
            PlannerStep::Io(vec![fetch(1), announce(2)]),
            one_batch_morsel(scope(rows), 2),
            PlannerStep::Done,
        ],
        log,
    )
}

/// Drains `run` until it waits or idles, describing what it handed back.
fn drain(run: &mut Run) -> VortexResult<Vec<String>> {
    let mut events = Vec::new();
    loop {
        events.push(match run.advance()? {
            Progress::Batch(batch) => format!("batch {:?} {:?}", batch.root, batch.scope.rows),
            Progress::RootDone(root) => format!("done {root:?}"),
            Progress::Waiting => {
                events.push("waiting".to_string());
                return Ok(events);
            }
            Progress::Idle => {
                events.push("idle".to_string());
                return Ok(events);
            }
        });
    }
}

fn poll(run: &mut Run) -> Poll<VortexResult<()>> {
    run.poll_completion(&mut Context::from_waker(Waker::noop()))
}

/// Roots run side by side: a root with ready work finishes while another waits for its read, and
/// each root reads through its own session, whose reads are withdrawn when the root finishes.
#[test]
fn roots_finish_independently() -> VortexResult<()> {
    let log = Log::default();
    let (reading_io, ready_io) = (ScriptedIo::manual(&log), ScriptedIo::manual(&log));
    let mut run = Run::new().with_step_limit(1_000);
    let reading = run.admit(
        reading_root("reading", &log, 0..2),
        scope(0..2),
        reading_io.session(),
    );
    let ready = run.admit(
        ready_root("ready", &log, 2..3),
        scope(2..3),
        ready_io.session(),
    );
    assert_eq!((reading, ready), (RootId(0), RootId(1)));
    assert_eq!(run.live_roots(), 2);

    assert_eq!(
        drain(&mut run)?,
        vec!["batch RootId(1) 2..3", "done RootId(1)", "waiting"]
    );
    assert_eq!(run.live_roots(), 1);
    assert!(ready_io.submissions().is_empty());
    assert_eq!(reading_io.live_reads(), vec![1, 2]);

    reading_io.complete(1)?;
    assert_eq!(
        drain(&mut run)?,
        vec!["batch RootId(0) 0..2", "done RootId(0)", "idle"]
    );
    assert_eq!(run.live_roots(), 0);
    assert!(reading_io.live_reads().is_empty());
    Ok(())
}

/// `poll_completion` stays pending until a read finishes, then delivers every finished fetch of
/// every root.
#[test]
fn poll_completion_delivers_from_every_waiting_root() -> VortexResult<()> {
    let log = Log::default();
    let sessions = [ScriptedIo::manual(&log), ScriptedIo::manual(&log)];
    let mut run = Run::new().with_step_limit(1_000);
    for (index, io) in (0u64..).zip(&sessions) {
        let rows = index..index + 1;
        run.admit(
            reading_root("root", &log, rows.clone()),
            scope(rows),
            io.session(),
        );
    }
    assert_eq!(drain(&mut run)?, vec!["waiting"]);
    assert!(poll(&mut run).is_pending());
    for io in &sessions {
        io.complete(1)?;
    }
    assert!(matches!(poll(&mut run), Poll::Ready(Ok(()))));
    assert_eq!(log.matching("deliver"), vec!["deliver root 1"; 2]);
    let mut events = drain(&mut run)?;
    events.sort();
    assert_eq!(
        events,
        vec![
            "batch RootId(0) 0..1",
            "batch RootId(1) 1..2",
            "done RootId(0)",
            "done RootId(1)",
            "idle",
        ]
    );
    Ok(())
}

#[test]
fn poll_completion_without_a_live_root_fails() {
    let mut run = Run::new();
    let err = match poll(&mut run) {
        Poll::Ready(result) => error_message(result),
        Poll::Pending => None,
    };
    assert!(
        err.as_deref().is_some_and(|m| m.contains("no live root")),
        "{err:?}"
    );
}

/// Cancelling a root drops its work and withdraws its fetches and hints; its read can no longer
/// be completed, and the other root carries on.
#[test]
fn cancel_withdraws_a_roots_reads() -> VortexResult<()> {
    let log = Log::default();
    let (cancelled_io, kept_io) = (ScriptedIo::manual(&log), ScriptedIo::manual(&log));
    let mut run = Run::new().with_step_limit(1_000);
    let cancelled = run.admit(
        reading_root("cancelled", &log, 0..2),
        scope(0..2),
        cancelled_io.session(),
    );
    run.admit(
        reading_root("kept", &log, 2..4),
        scope(2..4),
        kept_io.session(),
    );
    assert_eq!(drain(&mut run)?, vec!["waiting"]);
    assert_eq!(cancelled_io.live_reads(), vec![1, 2]);

    run.cancel(cancelled);
    assert_eq!(run.live_roots(), 1);
    assert!(cancelled_io.live_reads().is_empty());
    assert_eq!(kept_io.live_reads(), vec![1, 2]);
    assert!(cancelled_io.complete(1).is_err());

    kept_io.complete(1)?;
    assert_eq!(
        drain(&mut run)?,
        vec!["batch RootId(1) 2..4", "done RootId(1)", "idle"]
    );
    assert_eq!(log.matching("deliver"), vec!["deliver kept 1"]);
    Ok(())
}

#[test]
fn dropping_a_run_withdraws_every_read() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::manual(&log);
    let mut run = Run::new();
    run.admit(reading_root("root", &log, 0..2), scope(0..2), io.session());
    assert_eq!(drain(&mut run)?, vec!["waiting"]);
    assert_eq!(io.live_reads(), vec![1, 2]);
    drop(run);
    assert!(io.live_reads().is_empty());
    let mut withdrawn = log.matching("io withdraw");
    withdrawn.sort();
    assert_eq!(withdrawn, vec!["io withdraw 1", "io withdraw 2"]);
    Ok(())
}

/// Waiting for M fetches costs the driver one delivery each, not a visit that re-lists the rest.
#[test]
fn a_waiting_item_is_not_revisited_per_delivery() -> VortexResult<()> {
    const FETCHES: u32 = 64;
    let log = Log::default();
    let io = ScriptedIo::auto(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::Io((0..FETCHES).map(fetch).collect()),
            PlannerStep::Done,
        ],
        &log,
    );
    // Publish and the final compute: the deliveries in between cost no visit.
    Driver::new(io.session()).with_step_limit(2).run(root)?;
    assert_eq!(io.performed().len(), FETCHES as usize);
    assert_eq!(log.matching("deliver").len(), FETCHES as usize);
    Ok(())
}

#[test]
fn waking_subtree_precedes_an_already_discovered_sibling() -> VortexResult<()> {
    let log = Log::default();
    let io = ScriptedIo::manual(&log);
    let root = ScriptedPlanner::boxed(
        "root",
        vec![
            PlannerStep::Planner(
                scope(0..1),
                vec![
                    PlannerStep::Io(vec![fetch(0)]),
                    one_batch_morsel(scope(0..1), 1),
                    PlannerStep::Done,
                ],
            ),
            PlannerStep::Morsel(
                scope(1..2),
                vec![
                    MorselStep::Compute(MorselOutput::Batch(array_of(1))),
                    MorselStep::Compute(MorselOutput::Batch(array_of(1))),
                    MorselStep::Compute(MorselOutput::Done),
                ],
            ),
            PlannerStep::Done,
        ],
        &log,
    );
    let mut run = Run::new().with_step_limit(1_000);
    run.admit(root, scope(0..2), io.session());
    let Progress::Batch(batch) = run.advance()? else {
        panic!("the later sibling should run while the first child waits");
    };
    assert_eq!(batch.scope, scope(1..2));

    io.complete(0)?;
    assert!(matches!(poll(&mut run), Poll::Ready(Ok(()))));
    // The waking child creates a grandchild after the sibling already exists. Its subtree
    // must still win, including over the sibling's continuation after its first batch.
    let Progress::Batch(batch) = run.advance()? else {
        panic!("the waking subtree should produce a batch first");
    };
    assert_eq!(batch.scope, scope(0..1));
    assert_eq!(
        drain(&mut run)?,
        vec!["batch RootId(0) 1..2", "done RootId(0)", "idle"]
    );
    Ok(())
}

#[test]
fn runnable_roots_follow_admission_order() -> VortexResult<()> {
    let log = Log::default();
    let mut run = Run::new().with_step_limit(1_000);
    for rows in [0..1, 1..2] {
        let root = ScriptedPlanner::boxed(
            "root",
            vec![
                PlannerStep::Continue,
                one_batch_morsel(scope(rows.clone()), 1),
                PlannerStep::Done,
            ],
            &log,
        );
        run.admit(root, scope(rows), ScriptedIo::manual(&log).session());
    }
    assert_eq!(
        drain(&mut run)?,
        vec![
            "batch RootId(0) 0..1",
            "done RootId(0)",
            "batch RootId(1) 1..2",
            "done RootId(1)",
            "idle",
        ]
    );
    Ok(())
}

#[test]
fn polls_io_while_a_later_branch_keeps_computing() -> VortexResult<()> {
    let log = Log::default();
    let mut run = Run::new().with_step_limit(1_000);
    run.admit(
        reading_root("earlier", &log, 0..1),
        scope(0..1),
        ScriptedIo::auto(&log).session(),
    );
    let mut script: Vec<_> = (0..IO_POLL_INTERVAL * 2)
        .map(|_| PlannerStep::Continue)
        .collect();
    script.push(one_batch_morsel(scope(1..2), 1));
    script.push(PlannerStep::Done);
    run.admit(
        ScriptedPlanner::boxed("later", script, &log),
        scope(1..2),
        ScriptedIo::auto(&log).session(),
    );
    let Progress::Batch(batch) = run.advance()? else {
        panic!("a finished read should wake the earlier root before the later root finishes");
    };
    assert_eq!(batch.root, RootId(0));
    let later_visits = log
        .events()
        .iter()
        .filter(|event| event.as_str() == "compute later")
        .count();
    assert!(later_visits > 0 && later_visits <= IO_POLL_INTERVAL);
    Ok(())
}
