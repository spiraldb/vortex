// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use futures::channel::oneshot;
use futures::task::ArcWake;
use futures::task::waker;
use parking_lot::Mutex;
use vortex_error::vortex_err;

use super::*;

struct Awaiting {
    tasks: VecDeque<WorkFuture>,
    remaining: usize,
    completed: Arc<Mutex<Vec<u64>>>,
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
        } else if self.tasks.is_empty()
            && self.read.is_none()
            && (self.remaining > 0 || self.waiting_read)
        {
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

    fn task_complete(&mut self, id: u64) -> VortexResult<()> {
        self.remaining -= 1;
        self.completed.lock().push(id);
        Ok(())
    }
}

fn awaiting(tasks: Vec<WorkFuture>, read: bool) -> (Box<dyn Planner>, Arc<Mutex<Vec<u64>>>) {
    let completed = Arc::new(Mutex::new(Vec::new()));
    (
        Box::new(Awaiting {
            remaining: tasks.len(),
            tasks: tasks.into(),
            completed: Arc::clone(&completed),
            read: read.then(|| request(0, 0)),
            waiting_read: read,
            emitted: false,
        }),
        completed,
    )
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
    let task = WorkFuture {
        id: 7,
        future: async move { recv.await.map_err(|_| vortex_err!("cancelled")) }.boxed(),
    };
    let (root, completed) = awaiting(vec![task], read);
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
    assert_eq!(*completed.lock(), vec![7]);
    assert!(matches!(run.advance()?, Progress::RootDone(_)));
    Ok(())
}

#[test]
fn multiple_ready_tasks_are_delivered_before_checking_for_a_stall() -> VortexResult<()> {
    let tasks = (0..3)
        .map(|id| WorkFuture {
            id,
            future: async { Ok(()) }.boxed(),
        })
        .collect();
    let (root, completed) = awaiting(tasks, false);
    assert_eq!(driver(&source()).run(root)?.len(), 1);
    let mut completed = completed.lock().clone();
    completed.sort_unstable();
    assert_eq!(completed, vec![0, 1, 2]);
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
    let (root, _) = awaiting(
        vec![WorkFuture {
            id: 0,
            future: async move {
                let _guard = guard;
                futures::future::pending().await
            }
            .boxed(),
        }],
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
    let (root, _) = awaiting(
        vec![WorkFuture {
            id: 0,
            future: async { Err(vortex_err!("child task failed")) }.boxed(),
        }],
        false,
    );
    let mut run = Run::new();
    run.admit(root, scope(0..3), source());
    let error = run.advance().err().expect("child error must fail the run");
    assert!(error.to_string().contains("child task failed"));
}

#[test]
fn duplicate_pending_task_identifiers_are_rejected() {
    let (root, _) = awaiting(
        (0..2)
            .map(|_| WorkFuture {
                id: 4,
                future: futures::future::pending().boxed(),
            })
            .collect(),
        false,
    );
    let mut run = Run::new();
    run.admit(root, scope(0..3), source());
    let error = run
        .advance()
        .err()
        .expect("duplicate task must fail the run");
    assert!(error.to_string().contains("published twice"));
}
