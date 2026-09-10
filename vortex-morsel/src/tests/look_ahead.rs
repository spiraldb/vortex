// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;

use parking_lot::Mutex;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::Nullability;
use vortex_array::expr::get_item;
use vortex_array::expr::gt;
use vortex_array::expr::lit;
use vortex_array::expr::root;
use vortex_array::expr::select;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::plan::ExactPlan;
use vortex_layout::segments::SegmentId;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use super::aligned_fixture;
use super::session;
use crate::RowDomain;
use crate::build_plan;
use crate::cells::SharedCells;
use crate::io::IoDemand;
use crate::io::IoDemandStream;
use crate::io::IoKey;
use crate::io::IoPlane;
use crate::io::IoPriority;
use crate::io::IoRequest;
use crate::io::IoService;
use crate::io::IoTicket;
use crate::io::IoUse;
use crate::io::ProducerId;
use crate::node::Child;
use crate::node::Cx;
use crate::node::Env;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;
use crate::node::Tree;
use crate::node::WaitSet;
use crate::nodes::ChunkedExec;
use crate::nodes::StructExec;
use crate::stats::ScanStats;

struct Harness {
    range: Range<u64>,
    demand: Mask,
    io: IoPlane,
    service: Arc<IoService>,
    requests: IoDemandStream,
    cells: SharedCells,
    dictionaries: Mutex<HashMap<ExactPlan, ArrayRef>>,
    session: VortexSession,
    stats: ScanStats,
}

impl Harness {
    fn new(domain: &RowDomain) -> Self {
        let (service, requests) = IoService::new();
        Self {
            range: domain.range().clone(),
            demand: domain.snapshot(),
            io: IoPlane::new(Arc::clone(&service)),
            service,
            requests,
            cells: SharedCells::disabled(),
            dictionaries: Mutex::default(),
            session: session(),
            stats: ScanStats::default(),
        }
    }

    fn env(&mut self) -> Env<'_> {
        Env {
            io: &self.io,
            cells: &self.cells,
            dictionaries: &self.dictionaries,
            session: &self.session,
            stats: &mut self.stats,
        }
    }

    fn look_ahead(&mut self, tree: &mut Tree) -> VortexResult<(LookAhead, WaitSet)> {
        let demand = self.demand.clone();
        tree.look_ahead(self.range.clone(), &demand, self.env())
    }

    fn submit(&mut self) -> Vec<IoRequest> {
        self.service.start(&self.io.take_reads());
        let mut requests = Vec::new();
        while let Ok(IoDemand::Start(batch)) = self.requests.try_recv() {
            requests.extend(batch);
        }
        requests
    }
}

/// Discovers a second read only after the first read is available.
struct GatedRead {
    domain: RowDomain,
    id: u32,
    gated: bool,
    first: Option<IoTicket>,
    visits: Rc<RefCell<Vec<u32>>>,
}

impl Operator for GatedRead {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        self.visits.borrow_mut().push(self.id);
        let domain = self.domain.range().clone();
        let first = match self.first {
            Some(ticket) => ticket,
            None => {
                let ticket = cx.register(IoUse {
                    key: key(self.id),
                    extent: domain.clone(),
                    producer: ProducerId(self.id),
                    demand: self.domain.demand(domain.clone())?,
                })?;
                self.first = Some(ticket);
                ticket
            }
        };
        if self.gated {
            if cx.ready(first)?.is_none() {
                cx.wait(first);
                return Ok(LookAhead::Blocked);
            }
            cx.register(IoUse {
                key: key(self.id + 10),
                extent: domain.clone(),
                producer: ProducerId(self.id),
                demand: self.domain.demand(domain)?,
            })?;
        }
        Ok(LookAhead::Complete)
    }

    fn next(&mut self, _hint: &Mask, _cx: &mut Cx<'_>) -> VortexResult<Step> {
        Ok(Step::Finished)
    }

    fn close(&mut self, _cx: &mut Cx<'_>) {}

    fn describe(&self) -> String {
        format!("GatedRead({})", self.id)
    }
}

fn key(id: u32) -> IoKey {
    IoKey::Segment(SegmentId::from(id))
}

#[rstest]
#[case::struct_fields(false)]
#[case::chunks(true)]
fn blocked_children_do_not_hide_siblings_and_resume_without_duplicate_requests(
    #[case] chunked: bool,
) -> VortexResult<()> {
    let domain = RowDomain::new(0..12, Mask::new_true(12))?;
    let visits = Rc::new(RefCell::new(Vec::new()));
    let mut children = Vec::new();
    for id in 1..=3 {
        let child_domain = if chunked {
            let start = u64::from(id - 1) * 4;
            domain.slice(start..start + 4)?.rebase(0)?
        } else {
            domain.clone()
        };
        children.push(Child::new(
            id,
            Box::new(GatedRead {
                domain: child_domain,
                id,
                gated: id != 2,
                first: None,
                visits: Rc::clone(&visits),
            }),
        ));
    }
    let parent: Box<dyn Operator> = if chunked {
        Box::new(ChunkedExec::new(
            Arc::from([0, 4, 8, 12]),
            children.into_iter().map(Some).collect(),
            DType::Bool(Nullability::NonNullable),
            domain.clone(),
        )?)
    } else {
        Box::new(StructExec::new(
            FieldNames::from(["a", "b", "c"]),
            children,
            None,
            domain.clone(),
        ))
    };
    let mut tree = Tree::new(Child::new(0, parent), 0, 0);
    let mut harness = Harness::new(&domain);

    let (result, waits) = harness.look_ahead(&mut tree)?;
    assert_eq!(result, LookAhead::Blocked);
    assert_eq!(*visits.borrow(), [1, 2, 3]);
    assert_eq!(waits.waits().len(), 2);
    assert_eq!(harness.submit().len(), 3);

    harness
        .service
        .completions()
        .complete(key(1), Ok(BufferHandle::new_host(ByteBuffer::empty())));
    let (result, waits) = harness.look_ahead(&mut tree)?;
    assert_eq!(result, LookAhead::Blocked);
    assert_eq!(*visits.borrow(), [1, 2, 3, 1, 3]);
    assert_eq!(waits.waits().len(), 1);
    let requests = harness.submit();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].key, key(11));

    harness
        .service
        .completions()
        .complete(key(3), Ok(BufferHandle::new_host(ByteBuffer::empty())));
    assert_eq!(harness.look_ahead(&mut tree)?.0, LookAhead::Complete);
    assert_eq!(*visits.borrow(), [1, 2, 3, 1, 3, 3]);
    assert_eq!(harness.submit().len(), 1);
    assert_eq!(harness.look_ahead(&mut tree)?.0, LookAhead::Complete);
    assert!(harness.submit().is_empty());
    assert_eq!(harness.stats.io_uses, 5);
    Ok(())
}

#[test]
fn submitted_requests_observe_refinement_and_later_uses() -> VortexResult<()> {
    let domain = RowDomain::new(100..110, Mask::new_true(10))?;
    let mut harness = Harness::new(&domain);
    let view = domain.demand(102..107)?;
    harness.io.register(
        IoUse {
            key: key(1),
            extent: 0..10,
            producer: ProducerId(1),
            demand: view.clone(),
        },
        IoPriority::Speculative,
        &mut harness.stats,
    )?;
    let request = harness
        .submit()
        .pop()
        .ok_or_else(|| vortex_err!("missing request"))?;
    assert!(request.demands()[0].snapshot().all_true());

    let child = domain.slice(103..109)?.rebase(0)?;
    child.refine(&Mask::from_indices(6, [0, 2, 5]))?;
    assert_eq!(request.demands()[0].range(), &(102..107));
    assert_eq!(
        request.demands()[0].snapshot(),
        Mask::from_indices(5, [0, 1, 3])
    );
    domain.refine(&Mask::from_indices(10, [2, 3]))?;
    assert_eq!(view.snapshot(), Mask::from_indices(5, [0, 1]));

    let other = RowDomain::new(0..10, Mask::new_true(10))?;
    harness.io.register(
        IoUse {
            key: key(1),
            extent: 0..10,
            producer: ProducerId(2),
            demand: other.demand(0..10)?,
        },
        IoPriority::Required,
        &mut harness.stats,
    )?;
    assert!(harness.submit().is_empty());
    assert_eq!(request.demands().len(), 2);
    other.refine(&Mask::new_false(10))?;
    assert!(request.demands()[1].snapshot().all_false());
    assert_eq!(
        request.demands()[0].snapshot(),
        Mask::from_indices(5, [0, 1])
    );
    assert!(domain.demand(99..101).is_err());
    assert!(domain.refine(&Mask::new_true(9)).is_err());
    Ok(())
}

#[test]
fn execution_refines_the_demand_attached_by_flat_look_ahead() -> VortexResult<()> {
    let session = session();
    let fixture = aligned_fixture(&session, 100)?;
    let plan = build_plan(
        &fixture.layout,
        &select(vec!["b"], root()),
        Some(&gt(get_item("a", root()), lit(50i32))),
    )?;
    let domain = RowDomain::new(0..100, Mask::new_true(100))?;
    let mut harness = Harness::new(&domain);
    let mut tree = plan.instantiate(0..100);
    assert_eq!(harness.look_ahead(&mut tree)?.0, LookAhead::Complete);
    let requests = harness.submit();
    assert_eq!(requests.len(), 2);
    let projected = requests
        .iter()
        .find(|request| request.priority == IoPriority::Speculative)
        .ok_or_else(|| vortex_err!("projection was not registered"))?;
    let attached = projected.demands()[0].clone();
    assert_eq!(attached.snapshot().true_count(), 100);
    for request in &requests {
        let IoKey::Segment(id) = request.key;
        harness.service.completions().complete(
            request.key,
            Ok(BufferHandle::new_host(
                fixture.segment_buffers[*id as usize].clone(),
            )),
        );
    }
    let (result, _) = tree.next(0..100, &Mask::new_true(100), harness.env())?;
    let Step::Batch(batch) = result else {
        return Err(vortex_err!("no output batch"));
    };
    assert_eq!(batch.value.into_array()?.len(), 49);
    assert_eq!(attached.snapshot(), Mask::from_indices(100, 51..100));
    tree.close(0..100, &Mask::new_true(100), harness.env());
    Ok(())
}
