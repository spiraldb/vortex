// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Correctness suites for the morsel executor.
//!
//! Every suite is differential: the V1 `LayoutReader` is the oracle, and a run passes only when
//! it emits the same rows in the same order. The properties the design document lists are each
//! expressed as a variation the output must be invariant under — thread count, morsel size,
//! conjunct policy, decode-cache budget, chunk alignment.

// Fixture generation counts rows into `i32` columns at sizes that trivially fit; the cast lint
// only makes the generators harder to read.
#![allow(clippy::cast_possible_truncation)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use futures::FutureExt;
use futures::future::poll_fn;
use futures::future::try_join_all;
use parking_lot::Mutex;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::expr::and;
use vortex_array::expr::get_item;
use vortex_array::expr::gt;
use vortex_array::expr::gt_eq;
use vortex_array::expr::lit;
use vortex_array::expr::lt;
use vortex_array::expr::lt_eq;
use vortex_array::expr::pack;
use vortex_array::expr::root;
use vortex_array::expr::select;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::runtime::single::block_on;
use vortex_io::session::RuntimeSession;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::LayoutRef;
use vortex_layout::layout_children;
use vortex_layout::layouts::chunked::ChunkedLayout;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::layouts::row_idx::row_idx;
use vortex_layout::layouts::struct_::StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::layouts::zoned::writer::ZonedLayoutOptions;
use vortex_layout::layouts::zoned::writer::ZonedStrategy;
use vortex_layout::segments::ReadAtNowait;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

use crate::DemandHintDelivery;
use crate::PushMorselScanExecutor;
use crate::SegmentSourceDriver;
use crate::driver::StreamCancellation;
use crate::fixtures::Column;
use crate::fixtures::Fixture;
use crate::fixtures::write_fixture;
use crate::fixtures::write_fixture_with;
use crate::harness::MorselConfig;
use crate::harness::Query;
use crate::harness::RunOutcome;
use crate::harness::assert_same_rows;
use crate::harness::run_morsel;
use crate::harness::run_v1;
use crate::nodes::ConjunctMode;

fn session() -> VortexSession {
    array_session()
        .with::<LayoutSession>()
        .with::<RuntimeSession>()
}

fn i32_chunks(values: &[i32], boundaries: &[usize]) -> Vec<ArrayRef> {
    cut(values, boundaries)
        .into_iter()
        .map(|slice| {
            PrimitiveArray::new(Buffer::copy_from(slice), Validity::NonNullable).into_array()
        })
        .collect()
}

fn utf8_chunks(values: &[i32], boundaries: &[usize]) -> Vec<ArrayRef> {
    cut(values, boundaries)
        .into_iter()
        .map(|slice| {
            VarBinViewArray::from_iter_str(slice.iter().map(|v| format!("row-{v:06}"))).into_array()
        })
        .collect()
}

/// Split `values` at `boundaries`, which are exclusive ends in ascending order.
fn cut<'a>(values: &'a [i32], boundaries: &[usize]) -> Vec<&'a [i32]> {
    let mut out = Vec::with_capacity(boundaries.len());
    let mut start = 0;
    for &end in boundaries {
        out.push(&values[start..end]);
        start = end;
    }
    assert_eq!(start, values.len(), "boundaries must cover every value");
    out
}

/// The canonical misaligned fixture: three columns cut on three different boundary sets.
fn misaligned_fixture(session: &VortexSession, rows: usize) -> VortexResult<Fixture> {
    let col_a: Vec<i32> = (0..rows as i32).collect();
    let col_b: Vec<i32> = (0..rows as i32).map(|v| (v * 7) % 101).collect();
    let col_c: Vec<i32> = (0..rows as i32).map(|v| (v * 13) % 17).collect();

    let thirds = boundaries(rows, 3);
    let fifths = boundaries(rows, 5);
    let sevenths = boundaries(rows, 7);

    block_on(|_handle| async {
        write_fixture(
            vec![
                Column::new("a", i32_chunks(&col_a, &thirds)),
                Column::new("b", i32_chunks(&col_b, &fifths)),
                Column::new("c", utf8_chunks(&col_c, &sevenths)),
            ],
            session,
        )
        .await
    })
}

/// The same data with every column cut on the same boundaries — the aligned reference.
fn aligned_fixture(session: &VortexSession, rows: usize) -> VortexResult<Fixture> {
    let col_a: Vec<i32> = (0..rows as i32).collect();
    let col_b: Vec<i32> = (0..rows as i32).map(|v| (v * 7) % 101).collect();
    let col_c: Vec<i32> = (0..rows as i32).map(|v| (v * 13) % 17).collect();
    let single = vec![rows];

    block_on(|_handle| async {
        write_fixture(
            vec![
                Column::new("a", i32_chunks(&col_a, &single)),
                Column::new("b", i32_chunks(&col_b, &single)),
                Column::new("c", utf8_chunks(&col_c, &single)),
            ],
            session,
        )
        .await
    })
}

fn boundaries(rows: usize, parts: usize) -> Vec<usize> {
    let step = rows.div_ceil(parts);
    let mut out = Vec::with_capacity(parts);
    let mut end = step;
    while end < rows {
        out.push(end);
        end += step;
    }
    out.push(rows);
    out
}

fn queries() -> Vec<Query> {
    vec![
        Query {
            name: "select-all",
            projection: select(vec!["a", "b", "c"], root()),
            filter: None,
        },
        Query {
            name: "project-two",
            projection: select(vec!["a", "c"], root()),
            filter: None,
        },
        Query {
            name: "one-conjunct",
            projection: select(vec!["a", "b"], root()),
            filter: Some(gt(get_item("a", root()), lit(400i32))),
        },
        Query {
            name: "two-conjuncts",
            projection: select(vec!["a", "b", "c"], root()),
            filter: Some(and(
                gt(get_item("a", root()), lit(100i32)),
                lt(get_item("b", root()), lit(50i32)),
            )),
        },
        Query {
            name: "selective",
            projection: select(vec!["a", "c"], root()),
            filter: Some(and(
                gt(get_item("a", root()), lit(900i32)),
                lt(get_item("b", root()), lit(10i32)),
            )),
        },
        Query {
            name: "empty-result",
            projection: select(vec!["a"], root()),
            filter: Some(gt(get_item("a", root()), lit(1_000_000i32))),
        },
        Query {
            name: "filter-on-unprojected",
            projection: select(vec!["c"], root()),
            filter: Some(lt(get_item("b", root()), lit(30i32))),
        },
        Query {
            name: "packed-projection",
            projection: pack(
                vec![("x", get_item("a", root())), ("y", get_item("b", root()))],
                Nullability::NonNullable,
            ),
            filter: Some(gt(get_item("a", root()), lit(200i32))),
        },
    ]
}

const ROWS: usize = 1000;

#[rstest]
#[case::single(1)]
#[case::multiple_planning_batches(130)]
fn flat_chunks_share_one_source_and_pipeline(#[case] chunks: usize) -> VortexResult<()> {
    let session = session();
    let values = (0..chunks as i32).collect::<Vec<_>>();
    let boundaries = (1..=chunks).collect::<Vec<_>>();
    let fixture = block_on(|_handle| async {
        write_fixture(
            vec![Column::new("a", i32_chunks(&values, &boundaries))],
            &session,
        )
        .await
    })?;
    let query = Query {
        name: "segment-run",
        projection: select(vec!["a"], root()),
        filter: None,
    };
    let plan = crate::build_plan(
        &fixture.layout,
        &query.projection,
        None,
        ConjunctMode::Cascade,
    )?;
    assert_eq!(plan.len(), 3);
    assert_eq!(plan.sources().len(), 1);
    assert_eq!(plan.sources()[0].root_range, 0..chunks as u64);
    assert_eq!(plan.pipelines().len(), 1);
    assert_eq!(plan.flat_uses().count(), chunks);
    assert_eq!(
        plan.natural_splits(),
        &(1..=chunks as u64).collect::<Vec<_>>()
    );
    let oracle = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;
    let actual = run_morsel(
        &session,
        &fixture.layout,
        &fixture.segments,
        &query,
        MorselConfig {
            morsel_rows: chunks as u64,
            ..Default::default()
        },
    )?;
    assert_same_rows(
        &session,
        &v1_dtype(&fixture.layout, &query)?,
        &oracle,
        &actual,
    )?;
    let stats = actual
        .stats
        .ok_or_else(|| vortex_err!("morsel run omitted stats"))?;
    assert_eq!(stats.morsels, 1);
    assert_eq!(stats.push_source_activations, 1);
    assert_eq!(stats.decodes, chunks as u64);
    Ok(())
}

#[rstest]
#[case::projection(false)]
#[case::filtered(true)]
fn nested_chunked_keeps_its_boundary_above_fused_leaves(
    #[case] filtered: bool,
) -> VortexResult<()> {
    let session = session();
    let values = (0..32).collect::<Vec<i32>>();
    let fixture = block_on(|_handle| async {
        write_fixture(
            vec![Column::new("a", i32_chunks(&values, &[8, 16, 24, 32]))],
            &session,
        )
        .await
    })?;
    let column = fixture
        .layout
        .slot(1)?
        .ok_or_else(|| vortex_err!("missing column"))?;
    let leaves = (0..4)
        .map(|index| {
            column
                .slot(index)?
                .ok_or_else(|| vortex_err!("missing flat child"))
        })
        .collect::<VortexResult<Vec<_>>>()?;
    let groups = leaves
        .chunks(2)
        .map(|children| {
            ChunkedLayout::new(
                16,
                column.dtype().clone(),
                layout_children(children.to_vec()),
            )
            .into_layout()
        })
        .collect::<Vec<_>>();
    let column =
        ChunkedLayout::new(32, column.dtype().clone(), layout_children(groups)).into_layout();
    let layout = StructLayout::new(32, fixture.layout.dtype().clone(), vec![column]).into_layout();
    let query = Query {
        name: "nested-segment-runs",
        projection: select(vec!["a"], root()),
        filter: filtered.then(|| gt(get_item("a", root()), lit(5i32))),
    };
    let plan = crate::build_plan(
        &layout,
        &query.projection,
        query.filter.as_ref(),
        ConjunctMode::Cascade,
    )?;
    let expected = if filtered {
        vec![0..16, 16..32, 0..16, 16..32]
    } else {
        vec![0..16, 16..32]
    };
    assert_eq!(
        plan.sources()
            .iter()
            .map(|source| source.root_range.clone())
            .collect::<Vec<_>>(),
        expected
    );
    let oracle = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;
    for morsel_rows in [0, 12, 32] {
        let actual = run_morsel(
            &session,
            &layout,
            &fixture.segments,
            &query,
            MorselConfig {
                morsel_rows,
                ..Default::default()
            },
        )?;
        assert_same_rows(&session, &v1_dtype(&layout, &query)?, &oracle, &actual)?;
    }
    Ok(())
}

#[test]
fn q6_ranges_build_three_predicate_sources_and_match_v1() -> VortexResult<()> {
    let session = session();
    let fixture = aligned_fixture(&session, ROWS)?;
    let projection = select(vec!["a", "b"], root());
    let a = || get_item("a", root());
    let b = || get_item("b", root());
    let filter = and(
        and(gt_eq(a(), lit(100i32)), lt(a(), lit(900i32))),
        and(
            and(gt_eq(b(), lit(10i32)), lt_eq(b(), lit(80i32))),
            lt(a(), lit(850i32)),
        ),
    );
    let plan = crate::build_plan(
        &fixture.layout,
        &projection,
        Some(&filter),
        ConjunctMode::Cascade,
    )?;
    let predicate_slots = plan
        .sources()
        .iter()
        .filter_map(|source| match source.role {
            crate::SourceRole::Predicate { slot, .. } => Some(slot),
            crate::SourceRole::Projection => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(predicate_slots, [0, 1, 2]);

    let query = Query {
        name: "q6-range-fusion",
        projection,
        filter: Some(filter),
    };
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let oracle = run_v1(&session, &fixture.layout, &segments, &query)?;
    let actual = run_morsel(
        &session,
        &fixture.layout,
        &segments,
        &query,
        MorselConfig {
            threads: 2,
            morsel_rows: 128,
            ..Default::default()
        },
    )?;
    assert_same_rows(
        &session,
        &v1_dtype(&fixture.layout, &query)?,
        &oracle,
        &actual,
    )?;
    let stats = actual.stats.as_ref().expect("morsel runs report stats");
    assert!(stats.push_inline_gates > 0);
    assert_eq!(stats.push_cold_frame_spills, 0);
    Ok(())
}

#[test]
fn cascade_frontier_cursor_moves_down_rows_and_right_groups() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let projection = select(vec!["a", "b"], root());
    let filter = and(
        gt(get_item("a", root()), lit(100i32)),
        lt(get_item("b", root()), lit(80i32)),
    );
    let cascade = crate::build_plan(
        &fixture.layout,
        &projection,
        Some(&filter),
        ConjunctMode::Cascade,
    )?;
    let ranges = crate::morsels(&cascade, 128);
    let mut first = cascade.frontier(ranges[0].clone());
    assert_eq!(first.range(), &ranges[0]);
    let first_batch = first.next_io(1)?;
    assert_eq!(first_batch.kind(), crate::IoGroupKind::Conjunct);
    assert_eq!(first_batch.io().len(), 1);
    assert!(first_batch.is_complete());
    assert!(first.right()?);
    assert_eq!(first.range(), &ranges[0]);
    let second_batch = first.next_io(1)?;
    assert_eq!(second_batch.kind(), crate::IoGroupKind::Conjunct);
    assert_eq!(second_batch.io().len(), 1);
    assert!(second_batch.is_complete());
    assert!(first.right()?);
    let mut projection_io = Vec::new();
    let projection_kind = loop {
        let batch = first.next_io(1)?;
        projection_io.extend_from_slice(batch.io());
        if batch.is_complete() {
            break batch.kind();
        }
    };
    assert_eq!(projection_kind, crate::IoGroupKind::Projection);
    assert!(!projection_io.is_empty());
    assert!(!first.right()?);
    first.down(ranges[1].clone());
    assert_eq!(first.range(), &ranges[1]);
    assert_eq!(first.group(), 0);
    assert_eq!(first.next_io(1)?.kind(), crate::IoGroupKind::Conjunct);
    Ok(())
}

#[test]
fn parallel_frontier_groups_all_conjuncts_and_resumes_in_bits() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let filter = and(
        gt(get_item("a", root()), lit(100i32)),
        lt(get_item("b", root()), lit(80i32)),
    );
    let parallel = crate::build_plan(
        &fixture.layout,
        &select(vec!["a", "b"], root()),
        Some(&filter),
        ConjunctMode::Parallel,
    )?;
    let ranges = crate::morsels(&parallel, 128);
    let mut first = parallel.frontier(ranges[0].clone());
    let first_piece = first.next_io(1)?;
    assert_eq!(first_piece.kind(), crate::IoGroupKind::Conjunct);
    assert_eq!(first_piece.io().len(), 1);
    assert!(!first_piece.is_complete());
    assert!(first.right().is_err());
    let second_piece = first.next_io(1)?;
    assert_eq!(second_piece.io().len(), 1);
    assert!(second_piece.is_complete());
    assert!(first.right()?);
    assert_eq!(first.next_io(1)?.kind(), crate::IoGroupKind::Projection);
    Ok(())
}

#[test]
fn frontier_scheduler_depths_match_v1() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let query = Query {
        name: "frontier-depths",
        projection: select(vec!["a", "c"], root()),
        filter: Some(and(
            gt(get_item("a", root()), lit(100i32)),
            lt(get_item("b", root()), lit(80i32)),
        )),
    };
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let oracle = run_v1(&session, &fixture.layout, &segments, &query)?;

    for mode in [ConjunctMode::Cascade, ConjunctMode::Parallel] {
        for resident in [1, 2, 3] {
            for (depth, speculative, adaptive) in [
                (None, 0, false),
                (Some(0), 0, false),
                (Some(1), 0, false),
                (Some(1), 1, false),
                (Some(1), 2, false),
                (Some(1), 0, true),
            ] {
                let refill_widths: &[usize] = if depth.is_some() { &[1, 4] } else { &[1] };
                for &frontier_refill_ranges in refill_widths {
                    let actual = run_morsel(
                        &session,
                        &fixture.layout,
                        &segments,
                        &query,
                        MorselConfig {
                            threads: 2,
                            morsel_rows: 64,
                            mode,
                            resident_morsels_per_thread: resident,
                            frontier_lookahead_per_thread: depth,
                            speculative_frontiers: speculative,
                            adaptive_frontiers: adaptive,
                            frontier_refill_ranges,
                            ..Default::default()
                        },
                    )?;
                    assert_same_rows(
                        &session,
                        &v1_dtype(&fixture.layout, &query)?,
                        &oracle,
                        &actual,
                    )?;
                }
            }
        }
    }

    let projection = Query {
        name: "frontier-projection",
        projection: select(vec!["a", "c"], root()),
        filter: None,
    };
    let oracle = run_v1(&session, &fixture.layout, &segments, &projection)?;
    let actual = run_morsel(
        &session,
        &fixture.layout,
        &segments,
        &projection,
        MorselConfig {
            threads: 2,
            morsel_rows: 64,
            resident_morsels_per_thread: 3,
            frontier_lookahead_per_thread: Some(1),
            speculative_frontiers: 2,
            ..Default::default()
        },
    )?;
    assert_same_rows(
        &session,
        &v1_dtype(&fixture.layout, &projection)?,
        &oracle,
        &actual,
    )?;
    Ok(())
}

#[test]
fn null_range_bound_matches_v1() -> VortexResult<()> {
    let session = session();
    let fixture = aligned_fixture(&session, ROWS)?;
    let null = lit(Scalar::null(DType::Primitive(
        PType::I32,
        Nullability::Nullable,
    )));
    let query = Query {
        name: "null-range-bound",
        projection: select(vec!["a"], root()),
        filter: Some(and(
            gt_eq(get_item("a", root()), null),
            lt_eq(get_item("a", root()), lit(500i32)),
        )),
    };
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let oracle = run_v1(&session, &fixture.layout, &segments, &query)?;
    let actual = run_morsel(
        &session,
        &fixture.layout,
        &segments,
        &query,
        MorselConfig {
            threads: 2,
            morsel_rows: 128,
            ..Default::default()
        },
    )?;
    assert_same_rows(
        &session,
        &v1_dtype(&fixture.layout, &query)?,
        &oracle,
        &actual,
    )?;
    Ok(())
}

/// Property: the executor agrees with V1 on every query, over misaligned chunks.
#[rstest]
fn matches_v1_oracle(#[values(1, 2, 4)] threads: usize) -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);

    for query in queries() {
        let v1 = run_v1(&session, &fixture.layout, &segments, &query)?;
        let morsel = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig {
                threads,
                ..Default::default()
            },
        )
        .map_err(|err| err.with_context(format!("query {}", query.name)))?;
        assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)
            .map_err(|err| err.with_context(format!("query {}", query.name)))?;
    }
    Ok(())
}

#[rstest]
fn demand_hint_delivery_is_not_observable(
    #[values(
        DemandHintDelivery::Immediate,
        DemandHintDelivery::Disabled,
        DemandHintDelivery::Delayed(usize::MAX)
    )]
    demand_hints: DemandHintDelivery,
) -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    for query in queries() {
        let v1 = run_v1(&session, &fixture.layout, &segments, &query)?;
        let morsel = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig {
                threads: 2,
                demand_hints,
                ..Default::default()
            },
        )?;
        assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
        if query.filter.is_some() {
            let stats = morsel.stats.as_ref().expect("morsel runs report stats");
            assert!(stats.demand_hints_emitted > 0);
            assert!(stats.demand_state_live_max <= ROWS as u64);
            match demand_hints {
                DemandHintDelivery::Immediate => assert!(stats.demand_hints_observed > 0),
                DemandHintDelivery::Disabled => {
                    assert_eq!(stats.demand_hints_observed, 0);
                    assert!(stats.demand_hints_dropped > 0);
                }
                DemandHintDelivery::Delayed(_) => assert!(stats.demand_hints_dropped > 0),
            }
        }
    }
    Ok(())
}

#[test]
fn leaf_batch_crosses_multiple_parent_edges_inline() -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..32).collect();
    let fixture = block_on(|_handle| async {
        write_fixture(vec![Column::new("a", i32_chunks(&values, &[32]))], &session).await
    })?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let query = Query {
        name: "inline-parent-chain",
        projection: select(vec!["a"], root()),
        filter: None,
    };
    let v1 = run_v1(&session, &fixture.layout, &segments, &query)?;
    let morsel = run_morsel(
        &session,
        &fixture.layout,
        &segments,
        &query,
        MorselConfig {
            ..Default::default()
        },
    )?;
    assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    assert!(stats.push_inline_transfers >= 2);
    assert_eq!(
        stats.push_pipeline_stage_calls, 3,
        "one terminal leaf batch should cross the unary chain without a final root credit"
    );
    assert!(stats.push_fast_stage_transfers >= 2);
    assert_eq!(
        stats.push_fast_stage_transfers, stats.push_inline_transfers,
        "every payload edge in the unary/cross-boundary chain should stay on the fast path"
    );
    assert_eq!(
        stats.push_cold_frame_spills, 0,
        "a terminal unary chain must not enter the cold frame dispatcher"
    );
    assert_eq!(
        stats.push_runtime_mask_clones, 0,
        "typed routing must move selection with the batch instead of cloning it"
    );
    assert_eq!(stats.push_dispatch_spills, 0);
    Ok(())
}

#[rstest]
fn completion_sink_receives_each_morsel_in_order(
    #[values(1, 4)] threads: usize,
    #[values(false, true)] bounded: bool,
) -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..32).collect();
    let fixture = block_on(|_handle| async {
        write_fixture(
            vec![Column::new("a", i32_chunks(&values, &[3, 11, 19, 27, 32]))],
            &session,
        )
        .await
    })?;
    let filter = and(
        gt_eq(get_item("a", root()), lit(8i32)),
        lt(get_item("a", root()), lit(20i32)),
    );
    let plan = Arc::new(crate::build_plan(
        &fixture.layout,
        &get_item("a", root()),
        Some(&filter),
        ConjunctMode::Cascade,
    )?);
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let sink_outputs = Arc::clone(&outputs);
    let mut scan = crate::MorselScan::new(plan, session.clone())
        .with_threads(threads)
        .with_morsels(vec![0..8, 8..16, 16..24, 24..32])
        .with_completion_sink(move |index, batch| {
            sink_outputs
                .lock()
                .push((index, batch, std::thread::current().id()));
        });
    if bounded {
        scan = scan.with_output_capacity(1, 1);
    }
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let (batches, _) = SegmentSourceDriver::new(segments)
        .connect_on_thread(scan)?
        .run()?;
    assert!(batches.is_empty());
    let outputs = std::mem::take(&mut *outputs.lock());
    assert_eq!(outputs.len(), 4);
    for (expected_index, (index, batch, thread)) in outputs.into_iter().enumerate() {
        assert_eq!(index, expected_index);
        if threads == 1 && !bounded {
            assert_eq!(thread, std::thread::current().id());
        }
        let batch = batch?;
        match index {
            1 | 2 => {
                let batch = batch.ok_or_else(|| vortex_err!("missing nonempty morsel {index}"))?;
                let range = if index == 1 { 8..16 } else { 16..20 };
                let expected = PrimitiveArray::from_iter(range).into_array();
                assert_arrays_eq!(batch, expected, &mut session.create_execution_ctx());
            }
            _ => assert!(batch.is_none()),
        }
    }
    Ok(())
}

#[test]
fn inline_completion_can_cancel_remaining_morsels() -> VortexResult<()> {
    let session = session();
    let fixture = aligned_fixture(&session, 32)?;
    let plan = Arc::new(crate::build_plan(
        &fixture.layout,
        &get_item("a", root()),
        None,
        ConjunctMode::Cascade,
    )?);
    let cancellation = StreamCancellation::new();
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let sink_outputs = Arc::clone(&outputs);
    let sink_cancellation = Arc::clone(&cancellation);
    let scan = crate::MorselScan::new(plan, session.clone())
        .with_threads(1)
        .with_morsels(vec![0..8, 8..16, 16..24, 24..32])
        .with_cancellation(cancellation)
        .with_completion_sink(move |index, batch| {
            sink_outputs.lock().push((index, batch));
            sink_cancellation.cancel();
        });
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let (batches, _) = SegmentSourceDriver::new(segments)
        .connect_on_thread(scan)?
        .run()?;
    assert!(batches.is_empty());
    let outputs = std::mem::take(&mut *outputs.lock());
    assert_eq!(outputs.len(), 1);
    let (index, batch) = outputs
        .into_iter()
        .next()
        .ok_or_else(|| vortex_err!("missing first morsel"))?;
    assert_eq!(index, 0);
    let batch = batch?.ok_or_else(|| vortex_err!("first morsel has no rows"))?;
    let expected = PrimitiveArray::from_iter(0i32..8).into_array();
    assert_arrays_eq!(batch, expected, &mut session.create_execution_ctx());
    Ok(())
}

#[test]
fn bounded_stream_resumes_in_order_after_consumer_stall() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let query = Query {
        name: "bounded-stream",
        projection: select(vec!["a", "b", "c"], root()),
        filter: None,
    };
    let v1 = run_v1(&session, &fixture.layout, &segments, &query)?;
    let plan = Arc::new(crate::build_plan(
        &fixture.layout,
        &query.projection,
        None,
        ConjunctMode::Cascade,
    )?);
    let cut = crate::driver::morsels(&plan, 0);
    let scan = crate::MorselScan::new(plan, session.clone())
        .with_threads(4)
        .with_morsels(cut)
        .with_share_decodes(false)
        .with_output_capacity(1, 1);
    let mut stream = SegmentSourceDriver::new(segments)
        .connect_on_thread(scan)?
        .into_stream()?;

    std::thread::sleep(Duration::from_millis(20));
    let mut batches = Vec::new();
    for batch in stream.by_ref() {
        batches.push(batch?);
    }
    let (stats, wall) = stream.finish()?;
    let streamed = RunOutcome {
        rows: batches.iter().map(|batch| batch.len()).sum(),
        batches,
        wall,
        time_to_first_batch: stats.time_to_first_batch,
        stats: Some(stats.clone()),
        source_io_requests: None,
        source_io_bytes: None,
        source_nowait: None,
        source_io_occupancy: None,
    };
    assert_same_rows(
        &session,
        &v1_dtype(&fixture.layout, &query)?,
        &v1,
        &streamed,
    )?;
    assert!(stats.output_credit_blocks > 0);
    assert!(stats.output_rows_max > 1, "one oversized batch must escape");
    assert!(stats.push_inline_transfers > 0);
    Ok(())
}

#[test]
fn dropping_stream_cancels_stalled_scan() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let query = Query {
        name: "cancel-stream",
        projection: select(vec!["a", "b", "c"], root()),
        filter: None,
    };
    let plan = Arc::new(crate::build_plan(
        &fixture.layout,
        &query.projection,
        None,
        ConjunctMode::Cascade,
    )?);
    let cut = crate::driver::morsels(&plan, 0);
    let scan = crate::MorselScan::new(plan, session)
        .with_threads(4)
        .with_morsels(cut)
        .with_share_decodes(false)
        .with_output_capacity(1, 1);
    let mut stream = SegmentSourceDriver::new(segments)
        .connect_on_thread(scan)?
        .into_stream()?;
    drop(stream.next().transpose()?);
    drop(stream);
    Ok(())
}

struct NeverReadySource;

impl SegmentSource for NeverReadySource {
    fn request(&self, _id: SegmentId) -> SegmentFuture {
        futures::future::pending().boxed()
    }

    fn prefers_background_reads(&self) -> bool {
        true
    }
}

#[test]
fn dropping_stream_cancels_never_ready_io() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let query = Query {
        name: "cancel-never-ready",
        projection: select(vec!["a", "b", "c"], root()),
        filter: None,
    };
    let plan = Arc::new(crate::build_plan(
        &fixture.layout,
        &query.projection,
        None,
        ConjunctMode::Cascade,
    )?);
    let cut = crate::driver::morsels(&plan, 0);
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let scan = crate::MorselScan::new(plan, session)
            .with_threads(2)
            .with_morsels(cut);
        let stream = SegmentSourceDriver::new(Arc::new(NeverReadySource))
            .connect_on_thread(scan)
            .and_then(|scan| scan.into_stream());
        match stream {
            Ok(stream) => {
                std::thread::sleep(Duration::from_millis(20));
                drop(stream);
                drop(done_tx.send(Ok(())));
            }
            Err(err) => drop(done_tx.send(Err(err))),
        }
    });
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .map_err(|_| vortex_err!("dropping a stream did not cancel never-ready IO"))??;
    Ok(())
}

#[test]
#[allow(clippy::single_range_in_vec_init)]
fn rejects_invalid_morsel_cuts_before_starting() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let query = Query {
        name: "invalid-cut",
        projection: select(vec!["a"], root()),
        filter: None,
    };
    let plan = Arc::new(crate::build_plan(
        &fixture.layout,
        &query.projection,
        None,
        ConjunctMode::Cascade,
    )?);
    let row_count = plan.row_count();
    let invalid = [
        vec![],
        vec![0..0, 0..row_count],
        vec![1..row_count],
        vec![0..10, 11..row_count],
        vec![0..20, 10..row_count],
        vec![10..row_count, 0..10],
        vec![0..row_count + 1],
        vec![0..row_count - 1],
    ];
    for cut in invalid {
        let result = crate::MorselScan::new(Arc::clone(&plan), session.clone())
            .with_morsels(cut)
            .into_stream();
        assert!(result.is_err());
    }
    Ok(())
}

/// Property: misaligned chunking is invisible. The same logical table stored with three
/// different per-column chunkings must produce byte-identical output to the single-chunk
/// reference.
#[test]
fn misaligned_chunks_match_aligned_reference() -> VortexResult<()> {
    let session = session();
    let misaligned = misaligned_fixture(&session, ROWS)?;
    let aligned = aligned_fixture(&session, ROWS)?;
    let misaligned_segments: Arc<dyn SegmentSource> = Arc::clone(&misaligned.segments);
    let aligned_segments: Arc<dyn SegmentSource> = Arc::clone(&aligned.segments);

    for query in queries() {
        let left = run_morsel(
            &session,
            &misaligned.layout,
            &misaligned_segments,
            &query,
            MorselConfig::default(),
        )
        .map_err(|err| err.with_context(format!("query {}", query.name)))?;
        let right = run_morsel(
            &session,
            &aligned.layout,
            &aligned_segments,
            &query,
            MorselConfig::default(),
        )?;
        assert_same_rows(
            &session,
            &v1_dtype(&misaligned.layout, &query)?,
            &left,
            &right,
        )
        .map_err(|err| err.with_context(format!("query {}", query.name)))?;
    }
    Ok(())
}

/// The document's specific misaligned-chunk case: fields chunked `[0,3,10)` against `[0,6,10)`.
#[test]
fn document_misalignment_case() -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..10).collect();
    let fixture = block_on(|_handle| async {
        write_fixture(
            vec![
                Column::new("a", i32_chunks(&values, &[3, 10])),
                Column::new("b", i32_chunks(&values, &[6, 10])),
            ],
            &session,
        )
        .await
    })?;
    let reference = block_on(|_handle| async {
        write_fixture(
            vec![
                Column::new("a", i32_chunks(&values, &[10])),
                Column::new("b", i32_chunks(&values, &[10])),
            ],
            &session,
        )
        .await
    })?;

    let query = Query {
        name: "doc-case",
        projection: select(vec!["a", "b"], root()),
        filter: Some(gt(get_item("a", root()), lit(2i32))),
    };
    let dtype = v1_dtype(&fixture.layout, &query)?;

    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);
    let reference_segments: Arc<dyn SegmentSource> = Arc::clone(&reference.segments);

    let left = run_morsel(
        &session,
        &fixture.layout,
        &segments,
        &query,
        MorselConfig::default(),
    )?;
    let right = run_morsel(
        &session,
        &reference.layout,
        &reference_segments,
        &query,
        MorselConfig::default(),
    )?;
    let v1 = run_v1(&session, &fixture.layout, &segments, &query)?;

    assert_same_rows(&session, &dtype, &left, &right)?;
    assert_same_rows(&session, &dtype, &left, &v1)?;

    // The morsel cut must be the union of both columns' boundaries.
    let plan = crate::build_plan(
        &fixture.layout,
        &query.projection,
        query.filter.as_ref(),
        ConjunctMode::Cascade,
    )?;
    assert_eq!(plan.natural_splits(), &[3, 6, 10]);

    let projection = query.projection.bind(fixture.layout.dtype())?;
    let filter = query
        .filter
        .as_ref()
        .map(|filter| filter.bind(fixture.layout.dtype()))
        .transpose()?;
    let executor = PushMorselScanExecutor::new(Arc::clone(&fixture.layout), Arc::clone(&segments));
    assert_eq!(
        executor.full_file_splits(&projection, filter.as_ref())?,
        [0, 3, 6, 10]
    );
    Ok(())
}

/// Property: the result does not depend on how the scan is cut into morsels.
#[rstest]
fn independent_of_morsel_size(#[values(0, 1, 7, 128, 4096)] morsel_rows: u64) -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);

    for query in queries() {
        let dtype = v1_dtype(&fixture.layout, &query)?;
        let v1 = run_v1(&session, &fixture.layout, &segments, &query)?;
        let morsel = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig {
                morsel_rows,
                ..Default::default()
            },
        )
        .map_err(|err| err.with_context(format!("query {}", query.name)))?;
        assert_same_rows(&session, &dtype, &v1, &morsel)
            .map_err(|err| err.with_context(format!("query {}", query.name)))?;
    }
    Ok(())
}

/// Property: cascade and parallel conjunct policies are observationally identical.
#[test]
fn conjunct_policy_is_not_observable() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);

    for query in queries() {
        let dtype = v1_dtype(&fixture.layout, &query)?;
        let cascade = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig {
                mode: ConjunctMode::Cascade,
                ..Default::default()
            },
        )?;
        let parallel = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig {
                mode: ConjunctMode::Parallel,
                ..Default::default()
            },
        )?;
        assert_same_rows(&session, &dtype, &cascade, &parallel)
            .map_err(|err| err.with_context(format!("query {}", query.name)))?;
    }
    Ok(())
}

/// Property: the leased shared cells are an optimisation only. Disabling them must not change
/// a single row, at any thread count — the chaos check for the decode-reuse mechanism.
#[rstest]
fn shared_cells_are_not_observable(#[values(1, 4)] threads: usize) -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);

    for query in queries() {
        let dtype = v1_dtype(&fixture.layout, &query)?;
        let shared = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig {
                threads,
                ..Default::default()
            },
        )?;
        let unshared = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig {
                threads,
                share_decodes: false,
                ..Default::default()
            },
        )?;
        assert_same_rows(&session, &dtype, &shared, &unshared)
            .map_err(|err| err.with_context(format!("query {}", query.name)))?;

        let shared_stats = shared.stats.as_ref().expect("morsel runs report stats");
        let unshared_stats = unshared.stats.as_ref().expect("morsel runs report stats");
        assert_eq!(unshared_stats.decode_reuses, 0);
        assert_eq!(
            shared_stats.decodes + shared_stats.decode_reuses,
            unshared_stats.decodes,
            "query {}: every skipped decode must be accounted for by a reuse",
            query.name
        );
    }
    Ok(())
}

/// Property: on the misaligned fixture, sharing actually fires — a chunk overlapped by several
/// per-split morsels is decoded once and reused for the rest.
#[test]
fn shared_cells_reuse_straddled_chunks() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);

    let query = Query {
        name: "reuse",
        projection: select(vec!["a", "b", "c"], root()),
        filter: None,
    };
    let run = run_morsel(
        &session,
        &fixture.layout,
        &segments,
        &query,
        MorselConfig::default(),
    )?;
    let stats = run.stats.as_ref().expect("morsel runs report stats");
    assert!(
        stats.decode_reuses > 0,
        "expected cross-morsel decode reuse on a misaligned fixture, got none"
    );
    // Each of the 15 chunks (3 + 5 + 7) is decoded exactly once across the whole scan.
    assert_eq!(stats.decodes, 15);
    Ok(())
}

struct CountingSegmentSource {
    inner: Arc<dyn SegmentSource>,
    requests: Arc<AtomicUsize>,
}

struct RecordingSegmentSource {
    inner: Arc<dyn SegmentSource>,
    requests: Arc<Mutex<Vec<SegmentId>>>,
}

impl SegmentSource for RecordingSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.requests.lock().push(id);
        self.inner.request(id)
    }
}

#[test]
fn executor_prunes_zones_before_registering_data_io() -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..12).collect();
    let strategy = ZonedStrategy::new(
        FlatLayoutStrategy::default(),
        FlatLayoutStrategy::default(),
        ZonedLayoutOptions {
            block_size: NonZeroUsize::new(4).ok_or_else(|| vortex_err!("zero block size"))?,
            ..Default::default()
        },
    );
    let fixture = block_on(|handle| {
        let runtime_session = session.clone().with_handle(handle);
        async move {
            write_fixture_with(
                vec![Column::new("a", i32_chunks(&values, &[4, 8, 12]))],
                Arc::new(strategy),
                &runtime_session,
            )
            .await
        }
    })?;

    let field = fixture
        .layout
        .slot(1)?
        .ok_or_else(|| vortex_err!("fixture has no field layout"))?;
    let mut data_ids = Vec::new();
    let mut stats_ids = Vec::new();
    for index in 0..field.nchildren() {
        let zoned = field
            .slot(index)?
            .ok_or_else(|| vortex_err!("fixture has no zoned child {index}"))?;
        assert!(zoned.is::<Zoned>());
        let data = zoned
            .slot(0)?
            .ok_or_else(|| vortex_err!("zoned fixture has no data child"))?;
        let stats = zoned
            .slot(1)?
            .ok_or_else(|| vortex_err!("zoned fixture has no stats child"))?;
        data_ids.push(data.as_::<Flat>().segment_id());
        stats_ids.push(stats.as_::<Flat>().segment_id());
    }

    let projection_expr = select(vec!["a"], root());
    let filter_expr = gt(get_item("a", root()), lit(100i32));
    let plan = crate::build_plan(
        &fixture.layout,
        &projection_expr,
        Some(&filter_expr),
        ConjunctMode::Cascade,
    )?;
    let frontier_range = 0..4;
    let mut pruning = plan.frontier(frontier_range);
    let mut pruning_ids = Vec::new();
    let mut pruning_polls = 0;
    loop {
        let batch = pruning.next_io(1)?;
        pruning_polls += 1;
        assert_eq!(batch.kind(), crate::IoGroupKind::Pruning);
        pruning_ids.extend(batch.io().iter().map(|key| {
            let crate::IoKey::Segment(id) = *key;
            id
        }));
        if batch.is_complete() {
            break;
        }
    }
    assert_eq!(pruning_polls, 1);
    assert_eq!(pruning_ids, stats_ids[..1]);

    let second_pruning_range = 4..8;
    let mut second_pruning = plan.frontier(second_pruning_range);
    let second_pruning_batch = second_pruning.next_io(1)?;
    assert_eq!(second_pruning_batch.kind(), crate::IoGroupKind::Pruning);
    assert!(second_pruning_batch.is_complete());
    assert_eq!(
        second_pruning_batch.io().to_vec(),
        [crate::IoKey::Segment(stats_ids[1])]
    );
    assert!(pruning.right()?);
    let conjunct_batch = pruning.next_io(1)?;
    assert_eq!(conjunct_batch.kind(), crate::IoGroupKind::Conjunct);
    assert!(conjunct_batch.is_complete());
    assert!(pruning.right()?);
    assert_eq!(pruning.next_io(1)?.kind(), crate::IoGroupKind::Projection);

    let requests = Arc::new(Mutex::new(Vec::new()));
    let source: Arc<dyn SegmentSource> = Arc::new(RecordingSegmentSource {
        inner: Arc::clone(&fixture.segments),
        requests: Arc::clone(&requests),
    });
    let projection = projection_expr.bind(fixture.layout.dtype())?;
    let filter = filter_expr.bind(fixture.layout.dtype())?;
    let executor = PushMorselScanExecutor::new(Arc::clone(&fixture.layout), source)
        .with_target_rows(4)
        .with_threads(2);
    let outputs = block_on(|handle| async move {
        let mut outputs = Vec::new();
        for _ in 0..2 {
            let tasks = executor.build(
                session.clone().with_handle(handle.clone()),
                projection.clone(),
                Some(filter.clone()),
                None,
                vortex_scan::selection::Selection::All,
                None,
                0,
            )?;
            outputs.extend(try_join_all(tasks).await?);
        }
        Ok::<_, vortex_error::VortexError>(outputs)
    })?;

    assert!(outputs.iter().all(Option::is_none));
    let requests = requests.lock();
    assert!(data_ids.iter().all(|id| !requests.contains(id)));
    assert!(
        stats_ids
            .iter()
            .all(|id| requests.iter().filter(|requested| *requested == id).count() == 2)
    );
    Ok(())
}

#[test]
fn executor_projects_row_idx_with_offset() -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..12).collect();
    let fixture = block_on(|handle| {
        let runtime_session = session.clone().with_handle(handle);
        async move {
            write_fixture(
                vec![Column::new("a", i32_chunks(&values, &[4, 8, 12]))],
                &runtime_session,
            )
            .await
        }
    })?;
    let projection =
        pack([("idx", row_idx())], Nullability::NonNullable).bind(fixture.layout.dtype())?;
    let executor =
        PushMorselScanExecutor::new(Arc::clone(&fixture.layout), Arc::clone(&fixture.segments));
    let runtime_session = session.clone();
    let outputs = block_on(|handle| async move {
        let tasks = executor.build(
            runtime_session.with_handle(handle),
            projection,
            None,
            None,
            vortex_scan::selection::Selection::All,
            None,
            100,
        )?;
        try_join_all(tasks).await
    })?;
    let output = outputs
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| vortex_err!("row-index projection produced no rows"))?;
    let expected = StructArray::try_new(
        ["idx"].into(),
        vec![PrimitiveArray::from_iter(100u64..112).into_array()],
        12,
        Validity::NonNullable,
    )?
    .into_array();
    assert_arrays_eq!(output, expected, &mut session.create_execution_ctx());
    Ok(())
}

#[rstest]
fn executor_filters_on_row_idx_with_offset(
    #[values(false, true)] frontier_io: bool,
) -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..12).collect();
    let fixture = block_on(|handle| {
        let runtime_session = session.clone().with_handle(handle);
        async move {
            write_fixture(
                vec![Column::new("a", i32_chunks(&values, &[12]))],
                &runtime_session,
            )
            .await
        }
    })?;
    let projection = select(vec!["a"], root()).bind(fixture.layout.dtype())?;
    let filter = gt_eq(row_idx(), lit(105u64)).bind(fixture.layout.dtype())?;
    let executor =
        PushMorselScanExecutor::new(Arc::clone(&fixture.layout), Arc::clone(&fixture.segments))
            .with_frontier_io(frontier_io);
    let runtime_session = session.clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .map_err(|err| vortex_err!("failed to build test runtime: {err}"))?;
    let outputs = runtime.block_on(async move {
        let tasks = executor.build(
            runtime_session.with_tokio(),
            projection,
            Some(filter),
            None,
            vortex_scan::selection::Selection::All,
            None,
            100,
        )?;
        try_join_all(tasks).await
    })?;
    let output = outputs
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| vortex_err!("row-index filter produced no rows"))?;
    let expected = StructArray::try_new(
        ["a"].into(),
        vec![PrimitiveArray::from_iter(5i32..12).into_array()],
        7,
        Validity::NonNullable,
    )?
    .into_array();
    assert_arrays_eq!(output, expected, &mut session.create_execution_ctx());
    Ok(())
}

struct BackgroundCountingSource {
    inner: Arc<dyn SegmentSource>,
    requests: Arc<AtomicUsize>,
}

impl SegmentSource for BackgroundCountingSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.inner.request(id)
    }

    fn prefers_background_reads(&self) -> bool {
        true
    }
}

struct NowaitSegmentSource {
    buffers: Arc<[ByteBuffer]>,
    attempts: Arc<AtomicUsize>,
    fallbacks: Arc<AtomicUsize>,
    hit: bool,
}

impl SegmentSource for NowaitSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.fallbacks.fetch_add(1, Ordering::Relaxed);
        let buffer = self.buffers.get(*id as usize).cloned();
        async move {
            buffer
                .map(BufferHandle::new_host)
                .ok_or_else(|| vortex_err!("missing segment {id}"))
        }
        .boxed()
    }

    fn request_nowait(&self, id: SegmentId) -> VortexResult<ReadAtNowait> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if !self.hit {
            return Ok(ReadAtNowait::WouldBlock);
        }
        self.buffers
            .get(*id as usize)
            .cloned()
            .map(BufferHandle::new_host)
            .map(ReadAtNowait::Ready)
            .ok_or_else(|| vortex_err!("missing segment {id}"))
    }
}

#[test]
fn inline_nowait_hit_never_creates_a_background_future() -> VortexResult<()> {
    let session = session();
    let fixture = aligned_fixture(&session, 64)?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let fallbacks = Arc::new(AtomicUsize::new(0));
    let source: Arc<dyn SegmentSource> = Arc::new(NowaitSegmentSource {
        buffers: Arc::from(fixture.segment_buffers.clone()),
        attempts: Arc::clone(&attempts),
        fallbacks: Arc::clone(&fallbacks),
        hit: true,
    });
    let query = Query {
        name: "nowait-hit",
        projection: select(vec!["a"], root()),
        filter: None,
    };
    let v1 = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;
    let morsel = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &query,
        MorselConfig {
            ..Default::default()
        },
    )?;

    assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    assert_eq!(fallbacks.load(Ordering::Relaxed), 0);
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    assert_eq!(stats.nowait_attempts, 1);
    assert_eq!(stats.nowait_hits, 1);
    assert_eq!(stats.nowait_misses, 0);
    assert_eq!(stats.execute_io_blocks, 0);
    assert_eq!(stats.io_waits, 0);
    Ok(())
}

#[test]
fn inline_nowait_miss_falls_back_once() -> VortexResult<()> {
    let session = session();
    let fixture = aligned_fixture(&session, 64)?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let fallbacks = Arc::new(AtomicUsize::new(0));
    let source: Arc<dyn SegmentSource> = Arc::new(NowaitSegmentSource {
        buffers: Arc::from(fixture.segment_buffers.clone()),
        attempts: Arc::clone(&attempts),
        fallbacks: Arc::clone(&fallbacks),
        hit: false,
    });
    let query = Query {
        name: "nowait-miss",
        projection: select(vec!["a"], root()),
        filter: None,
    };
    let v1 = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;
    let morsel = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &query,
        MorselConfig {
            ..Default::default()
        },
    )?;

    assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    assert_eq!(fallbacks.load(Ordering::Relaxed), 1);
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    assert_eq!(stats.nowait_attempts, 1);
    assert_eq!(stats.nowait_hits, 0);
    assert_eq!(stats.nowait_misses, 1);
    assert_eq!(stats.nowait_unsupported, 0);
    assert!(stats.execute_io_blocks > 0);
    Ok(())
}

impl SegmentSource for CountingSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.inner.request(id)
    }
}

/// Raw request cells are shared scan-wide even when decoded-array sharing is disabled.
#[test]
fn scan_wide_io_cells_deduplicate_straddled_chunks() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let requests = Arc::new(AtomicUsize::new(0));
    let source: Arc<dyn SegmentSource> = Arc::new(CountingSegmentSource {
        inner: Arc::clone(&fixture.segments),
        requests: Arc::clone(&requests),
    });
    let query = Query {
        name: "scan-wide-io",
        projection: select(vec!["a", "b", "c"], root()),
        filter: None,
    };

    let run = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &query,
        MorselConfig {
            threads: 4,
            share_decodes: false,
            ..Default::default()
        },
    )?;
    let stats = run.stats.as_ref().expect("morsel runs report stats");

    assert_eq!(requests.load(Ordering::Relaxed), 15);
    assert_eq!(stats.io_requests, 15);
    assert!(stats.io_uses > stats.io_requests);
    assert!(stats.io_cells_live_max > 0);
    assert!(stats.io_retained_bytes_max > 0);
    assert_eq!(stats.io_cells_live, 0);
    assert_eq!(stats.io_retained_bytes, 0);
    Ok(())
}

#[test]
fn filtered_lookahead_refills_from_retired_frontier() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let requests = Arc::new(AtomicUsize::new(0));
    let source: Arc<dyn SegmentSource> = Arc::new(BackgroundCountingSource {
        inner: Arc::clone(&fixture.segments),
        requests: Arc::clone(&requests),
    });
    let query = Query {
        name: "sliding-lookahead",
        projection: select(vec!["a", "c"], root()),
        filter: Some(gt(get_item("a", root()), lit(400i32))),
    };
    let run = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &query,
        MorselConfig {
            threads: 1,

            lookahead_morsels: 1,
            ..Default::default()
        },
    )?;
    let stats = run.stats.as_ref().expect("morsel runs report stats");
    assert!(stats.lookahead_refills > 0);
    assert!(stats.demand_io_promotions > 0);
    assert_eq!(
        stats.io_requests,
        u64::try_from(requests.load(Ordering::Relaxed)).unwrap_or(u64::MAX)
    );
    assert!(requests.load(Ordering::Relaxed) > 0);
    Ok(())
}

/// Property: every read a node waits on was named by its own planning stream, so the number of
/// distinct segments read never exceeds the number of uses named.
#[test]
fn every_read_was_planned() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);

    for query in queries() {
        let run = run_morsel(
            &session,
            &fixture.layout,
            &segments,
            &query,
            MorselConfig::default(),
        )?;
        let stats = run.stats.as_ref().expect("morsel runs report stats");
        assert!(
            stats.io_requests <= stats.io_uses,
            "query {}: {} requests exceeds {} named uses",
            query.name,
            stats.io_requests,
            stats.io_uses
        );
    }
    Ok(())
}

/// Property: an all-false filter emits nothing and does not decode its projection columns.
#[test]
fn empty_filter_emits_nothing() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let segments: Arc<dyn SegmentSource> = Arc::clone(&fixture.segments);

    let query = Query {
        name: "empty",
        projection: select(vec!["a", "b", "c"], root()),
        filter: Some(gt(get_item("a", root()), lit(i32::MAX - 1))),
    };
    let run = run_morsel(
        &session,
        &fixture.layout,
        &segments,
        &query,
        MorselConfig::default(),
    )?;
    assert_eq!(run.rows, 0);
    assert!(run.batches.is_empty());
    let stats = run.stats.as_ref().expect("morsel runs report stats");
    assert_eq!(stats.morsels_empty, stats.morsels);
    Ok(())
}

#[derive(Default)]
struct PairedGate {
    polled: [bool; 2],
    wakers: [Option<Waker>; 2],
    watchdog_fired: bool,
}

struct PairedPendingSource {
    buffers: Arc<[ByteBuffer]>,
    gate: Arc<Mutex<PairedGate>>,
}

impl SegmentSource for PairedPendingSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let index = *id as usize;
        let buffer = self.buffers.get(index).cloned();
        let gate = Arc::clone(&self.gate);
        poll_fn(move |cx| {
            let Some(buffer) = buffer.as_ref() else {
                return Poll::Ready(Err(vortex_error::vortex_err!(
                    "missing gated segment {index}"
                )));
            };
            if index >= 2 {
                return Poll::Ready(Ok(BufferHandle::new_host(buffer.clone())));
            }

            let other = 1 - index;
            let mut gate = gate.lock();
            gate.polled[index] = true;
            if gate.polled[other] {
                if let Some(waker) = gate.wakers[other].take() {
                    waker.wake();
                }
                Poll::Ready(Ok(BufferHandle::new_host(buffer.clone())))
            } else {
                gate.wakers[index] = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .boxed()
    }
}

/// One CPU worker must submit every planned read before waiting for either one. Each of this
/// source's first two futures remains pending until the other has been polled, so the old inline
/// `block_on` driver reaches the watchdog while the continuation scheduler completes immediately.
#[rstest]
#[case::independent_sources(false)]
#[case::fused_segments(true)]
fn planned_reads_progress_together_without_parking_a_worker(
    #[case] fused: bool,
) -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..32).collect();
    let columns = if fused {
        vec![Column::new("a", i32_chunks(&values, &[16, 32]))]
    } else {
        vec![
            Column::new("a", i32_chunks(&values, &[32])),
            Column::new("b", i32_chunks(&values, &[32])),
        ]
    };
    let fixture = block_on(|_handle| async { write_fixture(columns, &session).await })?;

    let gate = Arc::new(Mutex::new(PairedGate::default()));
    let watchdog_gate = Arc::clone(&gate);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        let mut gate = watchdog_gate.lock();
        if gate.polled.iter().all(|polled| *polled) {
            return;
        }
        gate.watchdog_fired = true;
        gate.polled = [true; 2];
        for waker in gate.wakers.iter_mut().filter_map(Option::take) {
            waker.wake();
        }
    });

    let source: Arc<dyn SegmentSource> = Arc::new(PairedPendingSource {
        buffers: Arc::from(fixture.segment_buffers.clone()),
        gate: Arc::clone(&gate),
    });
    let query = Query {
        name: "paired-pending",
        projection: select(vec![if fused { "a" } else { "b" }], root()),
        filter: (!fused).then(|| gt(get_item("a", root()), lit(-1i32))),
    };
    let v1 = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;
    let morsel = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &query,
        MorselConfig {
            threads: 1,
            morsel_rows: 32,
            ..Default::default()
        },
    )?;

    assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
    let gate = gate.lock();
    assert_eq!(gate.polled, [true; 2]);
    assert!(!gate.watchdog_fired, "the CPU worker parked on one read");
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    assert!(stats.execute_io_blocks > 0);
    assert_eq!(stats.push_stale_wakes, 0);
    assert!(stats.push_inline_transfers > 0);
    assert!(stats.push_pipeline_runs > 0);
    assert!(stats.push_pipeline_stage_calls > 0);
    assert!(stats.push_pipeline_boundary_resumes >= 2);
    Ok(())
}

#[derive(Default)]
struct BurstGate {
    requests: [usize; 3],
    polls: [usize; 3],
    wakers: [Option<Waker>; 3],
    released: bool,
    watchdog_fired: bool,
}

struct BurstPendingSource {
    buffers: Arc<[ByteBuffer]>,
    gate: Arc<Mutex<BurstGate>>,
}

impl SegmentSource for BurstPendingSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let index = *id as usize;
        let buffer = self.buffers.get(index).cloned();
        if index < 3 {
            self.gate.lock().requests[index] += 1;
        }
        let gate = Arc::clone(&self.gate);
        poll_fn(move |cx| {
            let Some(buffer) = buffer.as_ref() else {
                return Poll::Ready(Err(vortex_error::vortex_err!(
                    "missing burst segment {index}"
                )));
            };
            if index >= 3 {
                return Poll::Ready(Ok(BufferHandle::new_host(buffer.clone())));
            }

            let wakes = {
                let mut gate = gate.lock();
                gate.polls[index] += 1;
                if gate.released {
                    return Poll::Ready(Ok(BufferHandle::new_host(buffer.clone())));
                }
                gate.wakers[index] = Some(cx.waker().clone());
                if gate.polls.iter().all(|polls| *polls > 0) {
                    gate.released = true;
                    gate.wakers.iter_mut().filter_map(Option::take).collect()
                } else {
                    Vec::new()
                }
            };
            for waker in wakes {
                waker.wake_by_ref();
                waker.wake_by_ref();
            }
            Poll::Pending
        })
        .boxed()
    }
}

/// Burst wakeups for several exact cells neither lose a wake nor poll a ready cell again from
/// execution.
#[test]
fn burst_wakes_are_coalesced_without_duplicate_polls() -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..32).collect();
    let fixture = block_on(|_handle| async {
        write_fixture(
            vec![
                Column::new("a", i32_chunks(&values, &[32])),
                Column::new("b", i32_chunks(&values, &[32])),
                Column::new("c", i32_chunks(&values, &[32])),
            ],
            &session,
        )
        .await
    })?;

    let gate = Arc::new(Mutex::new(BurstGate::default()));
    let watchdog_gate = Arc::clone(&gate);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        let wakes = {
            let mut gate = watchdog_gate.lock();
            if gate.released {
                return;
            }
            gate.watchdog_fired = true;
            gate.released = true;
            gate.wakers
                .iter_mut()
                .filter_map(Option::take)
                .collect::<Vec<_>>()
        };
        for waker in wakes {
            waker.wake();
        }
    });

    let source: Arc<dyn SegmentSource> = Arc::new(BurstPendingSource {
        buffers: Arc::from(fixture.segment_buffers.clone()),
        gate: Arc::clone(&gate),
    });
    let query = Query {
        name: "burst-pending",
        projection: select(vec!["a", "b", "c"], root()),
        filter: None,
    };
    let v1 = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;
    let morsel = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &query,
        MorselConfig {
            threads: 1,
            ..Default::default()
        },
    )?;

    assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
    let gate = gate.lock();
    assert_eq!(gate.requests, [1, 1, 1]);
    assert!(gate.polls.iter().all(|polls| (1..=2).contains(polls)));
    assert!(gate.polls.contains(&2));
    assert_eq!(gate.polls, [2, 2, 2]);
    assert!(!gate.watchdog_fired);
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    assert_eq!(stats.io_requests, 3);
    assert_eq!(stats.io_batches, 3);
    assert_eq!(stats.io_waits, 3);
    assert_eq!(stats.morsels_blocked_for_io, 1);
    assert!(stats.execute_io_blocks > 0);
    assert!(stats.io_blocks_per_morsel_max <= 3);
    assert_eq!(stats.push_stale_wakes, 0);
    assert!(stats.push_pipeline_runs > 0);
    assert!(stats.push_pipeline_stage_calls > 0);
    assert!(stats.push_pipeline_boundary_resumes >= 3);
    Ok(())
}

#[derive(Default)]
struct SpeculativeGate {
    polls: [usize; 2],
    projection_waker: Option<Waker>,
    released: bool,
    watchdog_fired: bool,
}

struct SlowSpeculativeSource {
    buffers: Arc<[ByteBuffer]>,
    gate: Arc<Mutex<SpeculativeGate>>,
}

struct FailingProjectionSource {
    buffers: Arc<[ByteBuffer]>,
    projection_polls: Arc<AtomicUsize>,
}

impl SegmentSource for FailingProjectionSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let index = *id as usize;
        if index == 1 {
            self.projection_polls.fetch_add(1, Ordering::Relaxed);
            return async move { Err(vortex_err!("injected projection read failure")) }.boxed();
        }
        let buffer = self.buffers.get(index).cloned();
        async move {
            buffer
                .map(BufferHandle::new_host)
                .ok_or_else(|| vortex_err!("missing segment {index}"))
        }
        .boxed()
    }

    fn prefers_background_reads(&self) -> bool {
        true
    }
}

impl SegmentSource for SlowSpeculativeSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let index = *id as usize;
        let buffer = self.buffers.get(index).cloned();
        let gate = Arc::clone(&self.gate);
        poll_fn(move |cx| {
            let Some(buffer) = buffer.as_ref() else {
                return Poll::Ready(Err(vortex_error::vortex_err!(
                    "missing speculative segment {index}"
                )));
            };
            if index >= 2 {
                return Poll::Ready(Ok(BufferHandle::new_host(buffer.clone())));
            }
            let mut gate = gate.lock();
            gate.polls[index] += 1;
            if index == 0 || gate.released {
                Poll::Ready(Ok(BufferHandle::new_host(buffer.clone())))
            } else {
                gate.projection_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .boxed()
    }
}

/// Required predicate IO resumes execution while speculative projection IO remains pending. An
/// empty predicate result retires the morsel without waiting for or consuming that projection.
#[test]
fn empty_filter_cancels_pending_speculative_io() -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..32).collect();
    let fixture = block_on(|_handle| async {
        write_fixture(
            vec![
                Column::new("a", i32_chunks(&values, &[32])),
                Column::new("b", i32_chunks(&values, &[32])),
            ],
            &session,
        )
        .await
    })?;

    let gate = Arc::new(Mutex::new(SpeculativeGate::default()));
    let watchdog_gate = Arc::clone(&gate);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        let wake = {
            let mut gate = watchdog_gate.lock();
            if gate.released {
                return;
            }
            gate.watchdog_fired = true;
            gate.released = true;
            gate.projection_waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    });

    let source: Arc<dyn SegmentSource> = Arc::new(SlowSpeculativeSource {
        buffers: Arc::from(fixture.segment_buffers.clone()),
        gate: Arc::clone(&gate),
    });
    let query = Query {
        name: "cancel-speculative",
        projection: select(vec!["b"], root()),
        filter: Some(gt(get_item("a", root()), lit(i32::MAX - 1))),
    };
    let v1 = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;
    let morsel = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &query,
        MorselConfig {
            threads: 1,
            ..Default::default()
        },
    )?;

    assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
    let gate = gate.lock();
    assert_eq!(gate.polls, [1, 1]);
    assert!(!gate.watchdog_fired, "execution waited for speculative IO");
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    assert!(stats.io_blocks_per_morsel_max <= 1);
    Ok(())
}

#[rstest]
fn speculative_projection_errors_are_authoritative_only(
    #[values(
        DemandHintDelivery::Immediate,
        DemandHintDelivery::Disabled,
        DemandHintDelivery::Delayed(usize::MAX)
    )]
    demand_hints: DemandHintDelivery,
) -> VortexResult<()> {
    let session = session();
    let values: Vec<i32> = (0..32).collect();
    let fixture = block_on(|_handle| async {
        write_fixture(
            vec![
                Column::new("a", i32_chunks(&values, &[32])),
                Column::new("b", i32_chunks(&values, &[32])),
            ],
            &session,
        )
        .await
    })?;
    let projection_polls = Arc::new(AtomicUsize::new(0));
    let source: Arc<dyn SegmentSource> = Arc::new(FailingProjectionSource {
        buffers: Arc::from(fixture.segment_buffers.clone()),
        projection_polls: Arc::clone(&projection_polls),
    });
    let empty = Query {
        name: "unused-failing-projection",
        projection: select(vec!["b"], root()),
        filter: Some(gt(get_item("a", root()), lit(i32::MAX - 1))),
    };
    let v1 = run_v1(&session, &fixture.layout, &fixture.segments, &empty)?;
    let morsel = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &empty,
        MorselConfig {
            demand_hints,
            ..Default::default()
        },
    )?;
    assert_same_rows(&session, &v1_dtype(&fixture.layout, &empty)?, &v1, &morsel)?;
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    if demand_hints == DemandHintDelivery::Immediate {
        assert!(stats.demand_io_suppressed > 0);
        assert!(stats.demand_io_candidates > 0);
        assert!(stats.demand_io_candidates <= stats.demand_hints_observed);
    }
    assert_eq!(stats.io_requests, 1);
    assert_eq!(projection_polls.load(Ordering::Relaxed), 0);

    let selected = Query {
        name: "used-failing-projection",
        projection: select(vec!["b"], root()),
        filter: Some(gt(get_item("a", root()), lit(-1_i32))),
    };
    let error = run_morsel(
        &session,
        &fixture.layout,
        &source,
        &selected,
        MorselConfig {
            demand_hints,
            ..Default::default()
        },
    )
    .err()
    .ok_or_else(|| vortex_err!("an authoritative projection read must surface its error"))?;
    assert!(format!("{error}").contains("injected projection read failure"));
    assert!(projection_polls.load(Ordering::Relaxed) > 0);
    Ok(())
}

/// Unsupported shapes are build errors, never silent fallbacks.
#[test]
fn rejects_unsupported_layouts() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, 32)?;
    // A non-struct root: take a column's chunked layout directly.
    let column = fixture
        .layout
        .slot(1)?
        .expect("the fixture root has a first field");
    let err = crate::build_plan(
        &column,
        &select(vec!["a"], root()),
        None,
        ConjunctMode::Cascade,
    )
    .err()
    .expect("a chunked root must be rejected");
    assert!(
        format!("{err}").contains("struct"),
        "unexpected error: {err}"
    );
    Ok(())
}

fn v1_dtype(layout: &LayoutRef, query: &Query) -> VortexResult<DType> {
    Ok(query.projection.bind(layout.dtype())?.dtype().clone())
}

/// A guard against the fixtures silently degenerating into a single chunk per column.
#[test]
fn fixture_is_actually_misaligned() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let plan = crate::build_plan(
        &fixture.layout,
        &select(vec!["a", "b", "c"], root()),
        None,
        ConjunctMode::Cascade,
    )?;
    // Three columns cut into 3, 5 and 7 chunks share only the final boundary.
    assert!(
        plan.natural_splits().len() > 7,
        "expected the union of three chunkings, got {:?}",
        plan.natural_splits()
    );
    Ok(())
}
