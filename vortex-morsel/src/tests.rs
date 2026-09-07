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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::poll_fn;
use parking_lot::Mutex;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::fns::all_non_distinct::all_non_distinct;
use vortex_array::array_session;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::expr::and;
use vortex_array::expr::eq;
use vortex_array::expr::get_item;
use vortex_array::expr::gt;
use vortex_array::expr::lit;
use vortex_array::expr::lt;
use vortex_array::expr::pack;
use vortex_array::expr::root;
use vortex_array::expr::select;
use vortex_array::validity::Validity;
use vortex_btrblocks::BtrBlocksCompressor;
use vortex_buffer::Buffer;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::runtime::single::block_on;
use vortex_io::session::RuntimeSession;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::LayoutRef;
use vortex_layout::LayoutStrategy;
use vortex_layout::layouts::dict::Dict;
use vortex_layout::layouts::dict::writer::DictLayoutOptions;
use vortex_layout::layouts::dict::writer::DictStrategy;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::segments::ReadAtNowait;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_layout::session::LayoutSession;
use vortex_mask::Mask;
use vortex_scan::selection::Selection;
use vortex_session::VortexSession;

use crate::IoAnswerer;
use crate::IoDemand;
use crate::LayoutCx;
use crate::LayoutPlanner;
use crate::LayoutPlanners;
use crate::MorselScan;
use crate::MorselScanExecutor;
use crate::ScanCancellation;
use crate::SegmentSourceDriver;
use crate::build_plan;
use crate::fixtures::Column;
use crate::fixtures::Fixture;
use crate::fixtures::write_fixture;
use crate::fixtures::write_fixture_with;
use crate::harness::MorselConfig;
use crate::harness::Query;
use crate::harness::assert_same_rows;
use crate::harness::concat;
use crate::harness::run_morsel;
use crate::harness::run_v1;
use crate::layouts::FlatPlanner;
use crate::morsels;
use crate::node::NodeId;
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

fn dict_strategy() -> Arc<dyn LayoutStrategy> {
    Arc::new(DictStrategy::new(
        FlatLayoutStrategy::default(),
        FlatLayoutStrategy::default(),
        FlatLayoutStrategy::default(),
        DictLayoutOptions::default(),
        Arc::new(BtrBlocksCompressor::default()),
    ))
}

fn scan_builder_batches(
    session: &VortexSession,
    fixture: &Fixture,
    query: &Query,
    pull: bool,
) -> VortexResult<Vec<ArrayRef>> {
    let projection = query.projection.bind(fixture.layout.dtype())?;
    let filter = query
        .filter
        .as_ref()
        .map(|filter| filter.bind(fixture.layout.dtype()))
        .transpose()?;
    let layout = Arc::clone(&fixture.layout);
    let segments = Arc::clone(&fixture.segments);

    let session = session.clone();
    block_on(move |handle| async move {
        let session = session.with_handle(handle);
        if pull {
            let executor = MorselScanExecutor::new(layout, segments)
                .with_threads(2)
                .with_target_rows(3);
            let tasks =
                executor.build(session, projection, filter, None, Selection::All, None, 0)?;
            let mut batches = Vec::new();
            for task in tasks {
                if let Some(batch) = task.await? {
                    batches.push(batch);
                }
            }
            Ok(batches)
        } else {
            let reader = layout.new_reader(
                "v1-scan-builder-test".into(),
                segments,
                &session,
                &Default::default(),
            )?;
            ScanBuilder::new(session, reader)
                .with_projection(projection)
                .with_some_filter(filter)
                .with_ordered(true)
                .into_stream()?
                .try_collect()
                .await
        }
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
        )?;
        assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)
            .map_err(|err| err.with_context(format!("query {}", query.name)))?;
    }
    Ok(())
}

/// Property: misaligned chunking is invisible. The same logical table stored with three
/// different per-column chunkings must produce byte-identical output to the single-chunk
/// reference.
#[rstest]
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
        )?;
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
#[rstest]
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
    let plan = build_plan(
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
    let executor = MorselScanExecutor::new(Arc::clone(&fixture.layout), Arc::clone(&segments));
    assert_eq!(
        executor.full_file_splits(&projection, filter.as_ref())?,
        [0, 3, 6, 10]
    );
    Ok(())
}

#[test]
fn scan_builder_pull_matches_v1_for_dictionary_layout_runs() -> VortexResult<()> {
    let session = session();
    let first = VarBinViewArray::from_iter_str([
        "alpha", "beta", "alpha", "gamma", "alpha", "beta", "gamma", "alpha",
    ])
    .into_array();
    let second = VarBinViewArray::from_iter_str([
        "delta", "alpha", "delta", "beta", "alpha", "delta", "alpha", "beta",
    ])
    .into_array();
    let fixture = block_on(|handle| async {
        let write_session = session.clone().with_handle(handle);
        write_fixture_with(
            vec![Column::new("label", vec![first, second])],
            dict_strategy(),
            &write_session,
        )
        .await
    })?;
    let column = fixture
        .layout
        .slot(1)?
        .expect("struct fixture has a label field");
    assert!(
        (0..2).all(|idx| column
            .slot(idx)
            .is_ok_and(|child| { child.is_some_and(|child| child.is::<Dict>()) })),
        "fixture must contain two dictionary layout runs"
    );

    let query = Query {
        name: "dict-scan-builder",
        projection: select(vec!["label"], root()),
        filter: Some(eq(get_item("label", root()), lit("alpha"))),
    };
    let v1_batches = scan_builder_batches(&session, &fixture, &query, false)?;
    let pull_batches = scan_builder_batches(&session, &fixture, &query, true)?;
    let dtype = query
        .projection
        .bind(fixture.layout.dtype())?
        .dtype()
        .clone();
    let outcome = |batches: Vec<ArrayRef>| crate::harness::RunOutcome {
        rows: batches.iter().map(|batch| batch.len()).sum(),
        batches,
        wall: Duration::ZERO,
        time_to_first_batch: None,
        stats: None,
        source_io_requests: None,
        source_io_bytes: None,
    };
    assert_same_rows(
        &session,
        &dtype,
        &outcome(v1_batches),
        &outcome(pull_batches),
    )
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
        )?;
        assert_same_rows(&session, &dtype, &v1, &morsel)
            .map_err(|err| err.with_context(format!("query {}", query.name)))?;
    }
    Ok(())
}

/// Property: cascade and parallel conjunct policies are observationally identical.
#[rstest]
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
#[rstest]
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

#[rstest]
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
        MorselConfig::default(),
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

#[rstest]
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
        MorselConfig::default(),
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
#[rstest]
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
    Ok(())
}

/// Property: every read a node waits on was named by its own planning stream, so the number of
/// distinct segments read never exceeds the number of uses named.
#[rstest]
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
#[rstest]
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

/// One CPU worker must drive every planned read together while waiting. Each of this source's first
/// two futures remains pending until the other has been polled, so waiting on only one future
/// reaches the watchdog while the planned-read driver completes immediately.
#[rstest]
fn worker_wait_drives_planned_reads_together() -> VortexResult<()> {
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
        projection: select(vec!["b"], root()),
        filter: Some(gt(get_item("a", root()), lit(-1i32))),
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
    assert_eq!(gate.polled, [true; 2]);
    assert!(!gate.watchdog_fired, "the CPU worker parked on one read");
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
#[rstest]
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
    assert_eq!(gate.polls, [2, 2, 2]);
    assert!(!gate.watchdog_fired);
    let stats = morsel.stats.as_ref().expect("morsel runs report stats");
    assert_eq!(stats.io_requests, 3);
    assert_eq!(stats.io_batches, 1);
    assert_eq!(stats.io_waits, 3);
    assert_eq!(stats.morsels_blocked_for_io, 1);
    assert!(stats.execute_io_blocks > 0);
    assert!(stats.io_blocks_per_morsel_max <= 3);
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
#[rstest]
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

/// Unsupported shapes are build errors, never silent fallbacks.
#[rstest]
fn rejects_unsupported_layouts() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, 32)?;
    // A non-struct root: take a column's chunked layout directly.
    let column = fixture
        .layout
        .slot(1)?
        .expect("the fixture root has a first field");
    let err = build_plan(
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
#[rstest]
fn fixture_is_actually_misaligned() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let plan = build_plan(
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

/// An unfiltered limit never reads the morsels past it and returns exactly the limit.
#[rstest]
fn unfiltered_limit_reads_only_the_morsels_it_needs() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let projection = select(vec!["a", "b"], root()).bind(fixture.layout.dtype())?;
    let layout = Arc::clone(&fixture.layout);
    let segments = Arc::clone(&fixture.segments);

    let (all_tasks, limited_tasks, rows) = block_on(move |handle| async move {
        let session = session.with_handle(handle);
        let executor = MorselScanExecutor::new(layout, segments)
            .with_threads(2)
            .with_target_rows(64);
        let all_tasks = executor
            .build(
                session.clone(),
                projection.clone(),
                None,
                None,
                Selection::All,
                None,
                0,
            )?
            .len();
        let tasks = executor.build(
            session,
            projection,
            None,
            None,
            Selection::All,
            Some(100),
            0,
        )?;
        let limited_tasks = tasks.len();
        let mut rows = 0;
        for task in tasks {
            if let Some(batch) = task.await? {
                rows += batch.len();
            }
        }
        VortexResult::Ok((all_tasks, limited_tasks, rows))
    })?;

    assert!(limited_tasks < all_tasks);
    assert_eq!(rows, 100);
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

/// Cancelling a scan whose reads never complete wakes its parked workers and ends the run.
#[rstest]
fn cancelling_a_stalled_scan_releases_its_workers() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let plan = Arc::new(build_plan(
        &fixture.layout,
        &select(vec!["a", "b", "c"], root()),
        None,
        ConjunctMode::Cascade,
    )?);
    let cut = morsels(&plan, 0);
    let cancellation = ScanCancellation::new();
    let scan = MorselScan::new(plan, session)
        .with_threads(2)
        .with_morsels(cut)
        .with_cancellation(Arc::clone(&cancellation));
    let scan = scan.connect_on_thread(&SegmentSourceDriver::new(Arc::new(NeverReadySource)))?;

    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        drop(done_tx.send(scan.run().map(|(batches, _)| batches.len())));
    });
    std::thread::sleep(Duration::from_millis(50));
    cancellation.cancel();

    let batches = done_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| vortex_err!("the cancelled scan did not stop"))??;
    assert_eq!(batches, 0);
    Ok(())
}

/// A planner registered ahead of the built-ins owns the layouts it handles.
struct CountingFlatPlanner {
    planned: Arc<AtomicUsize>,
}

impl LayoutPlanner for CountingFlatPlanner {
    fn handles(&self, layout: &LayoutRef) -> bool {
        FlatPlanner.handles(layout)
    }

    fn natural_splits(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut crate::SplitCx<'_>,
    ) -> VortexResult<()> {
        FlatPlanner.natural_splits(layout, root_offset, cx)
    }

    fn plan(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut LayoutCx<'_>,
    ) -> VortexResult<NodeId> {
        self.planned.fetch_add(1, Ordering::Relaxed);
        FlatPlanner.plan(layout, root_offset, cx)
    }
}

#[rstest]
fn registered_planners_take_precedence_and_missing_planners_are_errors() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let query = Query {
        name: "planner-registry",
        projection: select(vec!["a", "b", "c"], root()),
        filter: Some(gt(get_item("a", root()), lit(400i32))),
    };

    let planned = Arc::new(AtomicUsize::new(0));
    let planners = LayoutPlanners::default().with(Arc::new(CountingFlatPlanner {
        planned: Arc::clone(&planned),
    }));
    let plan = planners.build_plan(
        &fixture.layout,
        &query.projection,
        query.filter.as_ref(),
        ConjunctMode::Cascade,
    )?;
    let reference = build_plan(
        &fixture.layout,
        &query.projection,
        query.filter.as_ref(),
        ConjunctMode::Cascade,
    )?;
    assert_eq!(planned.load(Ordering::Relaxed), plan.flat_uses().count());
    assert_eq!(plan.flat_uses().count(), reference.flat_uses().count());
    assert_eq!(plan.natural_splits(), reference.natural_splits());
    // The same registry cuts morsels and scopes plans, so a custom layout works everywhere.
    assert_eq!(
        planners.natural_morsels_for(&fixture.layout, &query.projection, None, 0)?,
        crate::natural_morsels_for(&fixture.layout, &query.projection, None, 0)?
    );
    let scoped = planners.build_plan_for_ranges(
        &fixture.layout,
        &query.projection,
        None,
        ConjunctMode::Cascade,
        &[0..10, 20..30],
    )?;
    assert!(scoped.supports_ranges(&[0..10, 20..30]));
    assert!(!scoped.supports_ranges(&[10..20, 20..30]));

    let err = LayoutPlanners::empty()
        .build_plan(
            &fixture.layout,
            &query.projection,
            None,
            ConjunctMode::Cascade,
        )
        .err()
        .ok_or_else(|| vortex_err!("a plan without planners must fail"))?;
    assert!(err.to_string().contains("no planner for layout"));
    Ok(())
}

/// An answerer that serves bytes from memory with no `SegmentSource` at all.
struct MemoryAnswerer {
    buffers: Arc<[ByteBuffer]>,
    served: Arc<AtomicUsize>,
}

impl IoAnswerer for MemoryAnswerer {
    fn serve(
        &self,
        mut demand: crate::IoDemandStream,
        completions: crate::IoCompletions,
    ) -> futures::future::BoxFuture<'static, ()> {
        let buffers = Arc::clone(&self.buffers);
        let served = Arc::clone(&self.served);
        async move {
            while let Some(demand) = demand.next().await {
                let IoDemand::Start(requests) = demand else {
                    continue;
                };
                for request in requests {
                    let crate::IoKey::Segment(id) = request.key;
                    served.fetch_add(1, Ordering::Relaxed);
                    let result = buffers
                        .get(*id as usize)
                        .cloned()
                        .map(BufferHandle::new_host)
                        .ok_or_else(|| vortex_err!("missing segment {id}"));
                    if !completions.complete(request.key, result) {
                        return;
                    }
                }
            }
        }
        .boxed()
    }
}

#[rstest]
fn any_answerer_can_serve_a_scan() -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let query = Query {
        name: "custom-answerer",
        projection: select(vec!["a", "c"], root()),
        filter: Some(gt(get_item("b", root()), lit(50i32))),
    };
    let v1 = run_v1(&session, &fixture.layout, &fixture.segments, &query)?;

    let plan = Arc::new(build_plan(
        &fixture.layout,
        &query.projection,
        query.filter.as_ref(),
        ConjunctMode::Cascade,
    )?);
    let served = Arc::new(AtomicUsize::new(0));
    let answerer = MemoryAnswerer {
        buffers: Arc::from(fixture.segment_buffers.clone()),
        served: Arc::clone(&served),
    };
    let cut = morsels(&plan, 0);
    let scan = MorselScan::new(Arc::clone(&plan), session.clone())
        .with_threads(2)
        .with_morsels(cut)
        .connect_on_thread(&answerer)?;
    let (batches, stats) = scan.run()?;

    let rows = batches.iter().map(|batch| batch.len()).sum();
    let morsel = crate::harness::RunOutcome {
        rows,
        batches,
        wall: Duration::default(),
        time_to_first_batch: None,
        stats: Some(stats),
        source_io_requests: None,
        source_io_bytes: None,
    };
    assert_same_rows(&session, &v1_dtype(&fixture.layout, &query)?, &v1, &morsel)?;
    assert_eq!(served.load(Ordering::Relaxed), plan.flat_uses().count());
    Ok(())
}

/// The morsel cut a planner produces before any node exists matches the plan's natural splits.
#[rstest]
#[case::misaligned(true)]
#[case::aligned(false)]
fn natural_morsels_match_the_plan(#[case] misaligned: bool) -> VortexResult<()> {
    let session = session();
    let fixture = if misaligned {
        misaligned_fixture(&session, ROWS)?
    } else {
        aligned_fixture(&session, ROWS)?
    };
    for query in queries() {
        let plan = build_plan(
            &fixture.layout,
            &query.projection,
            query.filter.as_ref(),
            ConjunctMode::Cascade,
        )?;
        let cut = crate::natural_morsels_for(
            &fixture.layout,
            &query.projection,
            query.filter.as_ref(),
            0,
        )?;
        assert_eq!(cut, morsels(&plan, 0), "query {}", query.name);
    }
    Ok(())
}

/// The agreement also holds through a dictionary's lease scope, which cuts no morsels of its own.
#[rstest]
fn natural_morsels_match_the_plan_for_dictionary_layouts() -> VortexResult<()> {
    let session = session();
    let first = VarBinViewArray::from_iter_str([
        "alpha", "beta", "alpha", "gamma", "alpha", "beta", "gamma", "alpha",
    ])
    .into_array();
    let second = VarBinViewArray::from_iter_str([
        "delta", "alpha", "delta", "beta", "alpha", "delta", "alpha", "beta",
    ])
    .into_array();
    let fixture = block_on(|handle| async {
        let write_session = session.clone().with_handle(handle);
        write_fixture_with(
            vec![Column::new("label", vec![first, second])],
            dict_strategy(),
            &write_session,
        )
        .await
    })?;
    let projection = select(vec!["label"], root());
    let plan = build_plan(&fixture.layout, &projection, None, ConjunctMode::Cascade)?;
    let cut = crate::natural_morsels_for(&fixture.layout, &projection, None, 0)?;
    assert_eq!(cut, morsels(&plan, 0));
    assert!(cut.len() > 1, "each dictionary chunk is its own morsel");
    Ok(())
}

/// The row hint never drops a row on its own: leaves hand up what they hold, and the root
/// applies the actual selection, which is the caller's sparse demand intersected with the
/// predicate the conjuncts computed.
#[rstest]
fn leaves_never_apply_the_selection(#[values(false, true)] filtered: bool) -> VortexResult<()> {
    let session = session();
    let fixture = misaligned_fixture(&session, ROWS)?;
    let projection = select(vec!["a", "b", "c"], root());
    let predicate = gt(get_item("a", root()), lit(400i32));
    let filter = filtered.then(|| predicate.clone());
    let plan = Arc::new(build_plan(
        &fixture.layout,
        &projection,
        filter.as_ref(),
        ConjunctMode::Cascade,
    )?);

    // Every 37th row of each morsel: a sparse selection supplied by the caller.
    let cut = morsels(&plan, 0);
    let demands = cut
        .iter()
        .map(|range| {
            let len = usize::try_from(range.end - range.start)?;
            Ok((
                range.clone(),
                Mask::from_indices(len, (0..len).step_by(37).collect::<Vec<_>>()),
            ))
        })
        .collect::<VortexResult<Vec<_>>>()?;
    let sparse = Mask::concat(demands.iter().map(|(_, demand)| demand))?;

    let scan = MorselScan::new(Arc::clone(&plan), session.clone())
        .with_threads(2)
        .with_morsel_demands(demands)?
        .connect_on_thread(&SegmentSourceDriver::new(Arc::clone(&fixture.segments)))?;
    let (batches, stats) = scan.run()?;
    let dtype = plan.output_dtype().clone();
    let actual = concat(&batches, &dtype)?;

    // Expected: the unfiltered V1 result, cut by the sparse selection and the predicate.
    let full_query = Query {
        name: "sparse-reference",
        projection,
        filter: None,
    };
    let full = run_v1(&session, &fixture.layout, &fixture.segments, &full_query)?;
    let full = concat(&full.batches, &dtype)?;
    let mut selection = sparse;
    if filtered {
        let mut ctx = session.create_execution_ctx();
        let verdict: Mask = full
            .clone()
            .apply_bound(&predicate.bind(&dtype)?)?
            .null_as_false()
            .execute(&mut ctx)?;
        selection = &selection & &verdict;
    }
    let expected = full.filter(selection)?;
    let mut ctx = session.create_execution_ctx();
    assert_eq!(actual.len(), expected.len());
    assert!(all_non_distinct(&actual, &expected, &mut ctx)?);

    assert_eq!(stats.rows_selected, expected.len() as u64);
    assert!(
        stats.rows_materialized > stats.rows_selected,
        "leaves materialized {} rows for {} selected",
        stats.rows_materialized,
        stats.rows_selected
    );
    Ok(())
}
