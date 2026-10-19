// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use futures::channel::oneshot;
use futures::task::ArcWake;
use futures::task::waker;
use vortex_error::vortex_err;

use super::*;

struct Awaiting {
    tasks: VecDeque<BoxFuture<'static, VortexResult<()>>>,
    read: Option<IoRequest>,
    waiting_read: bool,
    emitted: bool,
}

impl IoConsumer for Awaiting {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {
        self.waiting_read = false;
    }
}

impl Planner for Awaiting {
    fn state(&self) -> State {
        if self.emitted {
            State::Done
        } else if self.tasks.is_empty() && self.read.is_none() && self.waiting_read {
            State::Waiting
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if let Some(read) = self.read.take() {
            return Ok(PlannerOutput::NeedsIO(vec![read]));
        }
        if let Some(task) = self.tasks.pop_front() {
            return Ok(PlannerOutput::Await(task));
        }
        self.emitted = true;
        Ok(PlannerOutput::Planner(
            scope(0..3),
            ScriptedPlanner::boxed(
                "after task",
                vec![one_batch_morsel(scope(0..3), 3), PlannerStep::Done],
                &Log::default(),
            ),
        ))
    }
}

fn awaiting(tasks: Vec<BoxFuture<'static, VortexResult<()>>>, read: bool) -> Box<dyn Planner> {
    Box::new(Awaiting {
        tasks: tasks.into(),
        read: read.then(|| request(0, 0)),
        waiting_read: read,
        emitted: false,
    })
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[rstest]
fn completion_wakes_a_planner_and_preserves_its_continuation(
    #[values(false, true)] read: bool,
) -> VortexResult<()> {
    let (send, recv) = oneshot::channel::<()>();
    let task = async move { recv.await.map_err(|_| vortex_err!("cancelled")) }.boxed();
    let root = awaiting(vec![task], read);
    let mut run = Run::new();
    run.admit(root, scope(0..3), source());
    assert!(matches!(run.advance()?, Progress::Waiting));
    let wakes = Arc::new(WakeCount::default());
    let waker = waker(Arc::clone(&wakes));
    if read {
        let Poll::Ready(result) = run.poll_completion(&mut Context::from_waker(&waker)) else {
            panic!("the canned read should complete")
        };
        result?;
        assert!(matches!(run.advance()?, Progress::Waiting));
    }
    assert!(
        run.poll_completion(&mut Context::from_waker(&waker))
            .is_pending()
    );
    send.send(()).map_err(|_| vortex_err!("receiver dropped"))?;
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    let Progress::Batch(batch) = run.advance()? else {
        panic!("missing continuation output")
    };
    assert_arrays_eq!(batch.array, array_of(3), &mut ctx());
    assert!(matches!(run.advance()?, Progress::RootDone(_)));
    Ok(())
}

#[test]
fn completed_future_keeps_waiting_for_outstanding_io() -> VortexResult<()> {
    let root = awaiting(vec![async { Ok(()) }.boxed()], true);
    let mut run = Run::new();
    run.admit(root, scope(0..3), source());
    assert!(matches!(run.advance()?, Progress::Waiting));
    assert!(run.tasks.is_empty());
    let Poll::Ready(result) = run.poll_completion(&mut Context::from_waker(Waker::noop())) else {
        panic!("the outstanding read should complete")
    };
    result?;
    assert!(matches!(run.advance()?, Progress::Batch(_)));
    Ok(())
}

#[test]
fn repeated_awaits_preserve_the_planner_continuation() -> VortexResult<()> {
    let tasks = (0..3).map(|_| async { Ok(()) }.boxed()).collect();
    let root = awaiting(tasks, false);
    assert_eq!(driver(&source()).run(root)?.len(), 1);
    Ok(())
}

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[rstest]
fn cancellation_drops_pending_child_work(
    #[values(false, true)] cancel_root: bool,
) -> VortexResult<()> {
    let drops = Arc::new(AtomicUsize::new(0));
    let guard = DropCount(Arc::clone(&drops));
    let root = awaiting(
        vec![
            async move {
                let _guard = guard;
                futures::future::pending().await
            }
            .boxed(),
        ],
        false,
    );
    let mut run = Run::new();
    let root = run.admit(root, scope(0..3), source());
    assert!(matches!(run.advance()?, Progress::Waiting));
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    if cancel_root {
        run.cancel(root);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
    drop(run);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn task_failure_fails_the_run() {
    let root = awaiting(
        vec![async { Err(vortex_err!("child task failed")) }.boxed()],
        false,
    );
    let mut run = Run::new();
    run.admit(root, scope(0..3), source());
    let error = run.advance().err().expect("child error must fail the run");
    assert!(error.to_string().contains("child task failed"));
}
