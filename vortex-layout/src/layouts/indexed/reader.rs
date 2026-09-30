//! Read-time probing: answer or prune a conjunct from an index child, else defer to the data
//! child.

use std::any::Any;
use std::ops::BitAnd;
use std::ops::Range;
use std::sync::Arc;

use futures::FutureExt;
use futures::TryFutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use roaring::RoaringBitmap;
use tracing::trace;
use vortex_array::ArrayRef;
use vortex_array::MaskFuture;
use vortex_array::VortexSessionExecute;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldMask;
use vortex_array::expr::BoundExpression;
use vortex_array::stream::ArrayStreamExt;
use vortex_buffer::BitBufferMut;
use vortex_error::SharedVortexResult;
use vortex_error::VortexResult;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_utils::aliases::dash_map::DashMap;
use vortex_utils::aliases::dash_map::Entry;

use crate::ArrayFuture;
use crate::LayoutReader;
use crate::LayoutReaderContext;
use crate::LayoutReaderRef;
use crate::LazyReaderChildren;
use crate::RowSplits;
use crate::SplitRange;
use crate::layouts::indexed::IndexSpec;
use crate::layouts::indexed::IndexedLayout;
use crate::layouts::indexed::index::IndexExactness;
use crate::layouts::indexed::index::IndexResolve;
use crate::layouts::indexed::index::RowLocator;
use crate::scan::scan_builder::ScanBuilder;
use crate::segments::SegmentSource;

/// One partition's probe result, shared by every split that needs it.
type SharedProbe = Shared<BoxFuture<'static, SharedVortexResult<Arc<RowLocator>>>>;

/// A reader for the [`crate::layouts::indexed::Indexed`] layout.
///
/// Probes happen once per expression per index partition: each partition's result is a cached
/// shared future, and every split overlapping that partition slices its own rows out of it rather
/// than re-probing. When more than one spec claims the same expression, the first `Exact` claim
/// covering every partition wins outright; failing that, every claiming spec's locator is kept and
/// intersected at evaluation time.
pub struct IndexedReader {
    layout: IndexedLayout,
    name: Arc<str>,
    lazy_children: Arc<LazyReaderChildren>,
    session: VortexSession,
    /// Cached claims keyed by expression. `None` means no index claimed the expression, so the
    /// lookup is not retried.
    claims: DashMap<BoundExpression, Option<Claims>>,
}

/// The indexes that claimed one expression.
#[derive(Clone)]
struct Claims {
    /// An exact claim, which answers `filter_evaluation` for any split it fully covers.
    exact: Option<Arc<Claim>>,
    /// Every claim that prunes, intersected by `pruning_evaluation`. An exact claim covering every
    /// partition is the only entry, since nothing else can sharpen it.
    pruning: Vec<Arc<Claim>>,
}

impl IndexedReader {
    pub(crate) fn try_new(
        layout: IndexedLayout,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: VortexSession,
        ctx: LayoutReaderContext,
    ) -> VortexResult<Self> {
        let mut dtypes = Vec::with_capacity(1 + layout.indexes().len());
        let mut names = Vec::with_capacity(1 + layout.indexes().len());
        dtypes.push(layout.dtype().clone());
        names.push(Arc::clone(&name));
        for spec in layout.indexes().iter() {
            dtypes.push(spec.index_dtype().clone());
            names.push(format!("{}.index:{}", name, spec.id()).into());
        }

        let lazy_children = Arc::new(LazyReaderChildren::new(
            Arc::clone(layout.children()),
            dtypes,
            names,
            segment_source,
            session.clone(),
            ctx,
        ));

        Ok(Self {
            layout,
            name,
            lazy_children,
            session,
            claims: DashMap::default(),
        })
    }

    fn data_child(&self) -> VortexResult<&LayoutReaderRef> {
        self.lazy_children.get(0)
    }

    /// The claims on `expr`, planned once per expression per file.
    ///
    /// The vacant-entry insert holds the shard lock across planning so two splits racing on the
    /// same expression plan it only once; planning only touches the child readers, never this map,
    /// so it cannot re-enter.
    fn claims(&self, expr: &BoundExpression) -> VortexResult<Option<Claims>> {
        if let Some(cached) = self.claims.get(expr) {
            return Ok(cached.value().clone());
        }

        match self.claims.entry(expr.clone()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let claims = self.plan_claims(expr)?;
                entry.insert(claims.clone());
                Ok(claims)
            }
        }
    }

    fn plan_claims(&self, expr: &BoundExpression) -> VortexResult<Option<Claims>> {
        let mut exact = None;
        let mut pruning = Vec::new();

        for (idx, spec) in self.layout.indexes().iter().enumerate() {
            // Unregistered kinds are inert: their child is never read.
            let Some(vtable) = spec.vtable() else {
                trace!(index = %spec.id(), "index kind not registered, skipping");
                continue;
            };

            let Some(plan) = vtable.plan(expr, self.layout.dtype(), spec.options())? else {
                continue;
            };

            trace!(index = %spec.id(), %expr, filter = %plan.filter, "index claimed expression");

            let index_reader = Arc::clone(self.lazy_children.get(idx + 1)?);
            // The index child's dtype is only known once its layout child is materialized, so the
            // plan's filter is bound here rather than by the index kind that produced it.
            let filter = plan.filter.bind(index_reader.dtype())?;
            let claim = Arc::new(Claim {
                partitions: Partitions::new(
                    spec,
                    self.layout.row_count(),
                    index_reader.row_count(),
                ),
                index_reader,
                filter,
                resolve: plan.resolve,
                probes: DashMap::default(),
                session: self.session.clone(),
            });

            if plan.exactness == IndexExactness::Exact {
                if claim.partitions.declined.is_empty() {
                    // Already the best possible answer everywhere: no other spec's claim on this
                    // expression, exact or not, can sharpen it or needs combining with it.
                    return Ok(Some(Claims {
                        exact: Some(Arc::clone(&claim)),
                        pruning: vec![claim],
                    }));
                }
                // Declined partitions leave gaps only other claims can prune, so keep them all.
                exact.get_or_insert_with(|| Arc::clone(&claim));
            }
            pruning.push(claim);
        }

        if pruning.is_empty() {
            return Ok(None);
        }
        Ok(Some(Claims { exact, pruning }))
    }
}

/// Where each of an index's partitions lives, in the data child and in the index child.
///
/// An unpartitioned index is a single partition spanning both children entirely.
struct Partitions {
    len: u64,
    data_row_count: u64,
    index_ends: Arc<[u64]>,
    declined: RoaringBitmap,
}

impl Partitions {
    fn new(spec: &IndexSpec, data_row_count: u64, index_row_count: u64) -> Self {
        match spec.partitioning() {
            Some(partitioning) => Self {
                len: partitioning.partition_len(),
                data_row_count,
                index_ends: partitioning.index_ends().into(),
                declined: partitioning.declined().clone(),
            },
            None => Self {
                // Clamped so an empty data child still divides cleanly.
                len: data_row_count.max(1),
                data_row_count,
                index_ends: Arc::new([index_row_count]),
                declined: RoaringBitmap::new(),
            },
        }
    }

    /// The partitions overlapping `row_range` of the data child.
    fn overlapping(&self, row_range: &Range<u64>) -> Range<usize> {
        let count = self.index_ends.len();
        // Clamped to the partition count, which already fits in memory.
        let clamp = |partition: u64| usize::try_from(partition).map_or(count, |p| p.min(count));
        let end = clamp(row_range.end.div_ceil(self.len));
        clamp(row_range.start / self.len).min(end)..end
    }

    fn data_rows(&self, partition: usize) -> Range<u64> {
        let start = partition as u64 * self.len;
        start..(start + self.len).min(self.data_row_count)
    }

    fn index_rows(&self, partition: usize) -> Range<u64> {
        let start = partition
            .checked_sub(1)
            .map_or(0, |prev| self.index_ends[prev]);
        start..self.index_ends[partition]
    }

    fn is_declined(&self, partition: usize) -> bool {
        u32::try_from(partition).is_ok_and(|partition| self.declined.contains(partition))
    }

    /// Whether every partition overlapping `row_range` was actually built.
    fn covers(&self, row_range: &Range<u64>) -> bool {
        !self
            .overlapping(row_range)
            .any(|partition| self.is_declined(partition))
    }
}

/// One index's claim on one expression, probed a partition at a time as splits need them.
struct Claim {
    index_reader: LayoutReaderRef,
    filter: BoundExpression,
    resolve: Arc<dyn IndexResolve>,
    partitions: Partitions,
    probes: DashMap<usize, SharedProbe>,
    session: VortexSession,
}

impl Claim {
    /// This claim's mask over `row_range` of the data child, stitched from each overlapping
    /// partition's locator. A declined partition proves nothing, so its rows stay set.
    ///
    /// Probes start here rather than when the future is polled, so a split registers its index IO
    /// as early as its data IO.
    fn mask(&self, row_range: &Range<u64>) -> VortexResult<BoxFuture<'static, VortexResult<Mask>>> {
        let mut parts = Vec::new();
        for partition in self.partitions.overlapping(row_range) {
            let rows = self.partitions.data_rows(partition);
            let local = row_range.start.max(rows.start) - rows.start
                ..row_range.end.min(rows.end) - rows.start;
            let probe = if self.partitions.is_declined(partition) {
                None
            } else {
                Some(self.probe(partition)?)
            };
            parts.push((local, probe));
        }

        let len = usize::try_from(row_range.end - row_range.start)?;
        Ok(async move {
            let mut bits = BitBufferMut::with_capacity(len);
            for (local, probe) in parts {
                match probe {
                    Some(probe) => probe.await?.append_to(&local, &mut bits)?,
                    None => bits.append_n(true, usize::try_from(local.end - local.start)?),
                }
            }
            Ok(Mask::from(bits.freeze()))
        }
        .boxed())
    }

    /// Start (or reuse) the probe of one partition.
    fn probe(&self, partition: usize) -> VortexResult<SharedProbe> {
        match self.probes.entry(partition) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let probe = probe_partition(
                    Arc::clone(&self.index_reader),
                    self.filter.clone(),
                    self.partitions.index_rows(partition),
                    Arc::clone(&self.resolve),
                    self.partitions.data_rows(partition),
                    self.session.clone(),
                )?;
                entry.insert(probe.clone());
                Ok(probe)
            }
        }
    }
}

/// Run a claim's filter as a real scan over one partition's rows of the index child, then fold
/// the surviving posting rows into a partition-local locator.
///
/// Going through [`ScanBuilder`] rather than calling the reader's evaluations directly is what
/// makes the probe cheap. The row range confines the scan to the partition's slice of the index
/// child, and within it the scan splits at the child's natural chunk boundaries and prunes each
/// split before projecting it, so the sorted key column's zone map narrows the probe to the few
/// chunks that can hold the query's keys and no other posting bytes are ever fetched.
fn probe_partition(
    index_reader: LayoutReaderRef,
    filter: BoundExpression,
    index_rows: Range<u64>,
    resolve: Arc<dyn IndexResolve>,
    data_rows: Range<u64>,
    session: VortexSession,
) -> VortexResult<SharedProbe> {
    let postings = ScanBuilder::new(session.clone(), index_reader)
        .with_filter(filter)
        .with_row_range(index_rows)
        .into_array_stream()?;

    Ok(async move {
        // Only the rows matching the plan's key predicate survive, one per query term, so
        // collecting them into a single array is cheap regardless of index size.
        let postings: ArrayRef = postings.read_all().await?;
        let mut ctx = session.create_execution_ctx();
        let locator = resolve.resolve(&postings, data_rows.end - data_rows.start, &mut ctx)?;
        Ok(Arc::new(locator))
    }
    .map_err(Arc::new)
    .boxed()
    .shared())
}

impl LayoutReader for IndexedReader {
    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dtype(&self) -> &DType {
        self.layout.dtype()
    }

    fn row_count(&self) -> u64 {
        self.layout.row_count()
    }

    fn register_splits(
        &self,
        field_mask: &[FieldMask],
        split_range: &SplitRange,
        splits: &mut RowSplits,
    ) -> VortexResult<()> {
        self.data_child()?
            .register_splits(field_mask, split_range, splits)
    }

    fn pruning_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &BoundExpression,
        mask: Mask,
    ) -> VortexResult<MaskFuture> {
        let data_eval = self
            .data_child()?
            .pruning_evaluation(row_range, expr, mask.clone())?;

        let Some(claims) = self.claims(expr)? else {
            return Ok(data_eval);
        };
        let masks = claims
            .pruning
            .iter()
            .map(|claim| claim.mask(row_range))
            .collect::<VortexResult<Vec<_>>>()?;

        let name = Arc::clone(&self.name);
        let expr = expr.clone();

        Ok(MaskFuture::new(mask.len(), async move {
            // Every claim's mask only narrows what's proven non-matching, so intersect them all;
            // stop as soon as nothing is left alive, rather than awaiting a probe whose answer can
            // no longer change the result.
            let mut result = mask;
            for claim_mask in masks {
                if result.all_false() {
                    break;
                }
                result = result.bitand(&claim_mask.await?);
            }

            // Only bother the data child if the index left anything alive.
            if !result.all_false() {
                result = result.bitand(&data_eval.await?);
            }

            trace!(%name, %expr, density = result.density(), "index pruning evaluation");
            Ok(result)
        }))
    }

    fn filter_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<MaskFuture> {
        // Only an exact claim answers the conjunct outright, so the data child is never decoded
        // for it — and only where every overlapping partition was built, since a declined one
        // knows nothing about its rows. A superset claim, however many specs contributed to it,
        // can only prune, so the real predicate always re-checks through the data child. Either
        // way this reuses the cached probes, so a superset conjunct costs no extra IO here.
        if let Some(exact) = self.claims(expr)?.and_then(|claims| claims.exact)
            && exact.partitions.covers(row_range)
        {
            let index_mask = exact.mask(row_range)?;
            let len = mask.len();
            return Ok(MaskFuture::new(len, async move {
                let index_mask = index_mask.await?;
                // Post-condition: the result must be intersected with the input mask.
                Ok(mask.await?.bitand(&index_mask))
            }));
        }

        self.data_child()?.filter_evaluation(row_range, expr, mask)
    }

    fn projection_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<ArrayFuture> {
        self.data_child()?
            .projection_evaluation(row_range, expr, mask)
    }
}
