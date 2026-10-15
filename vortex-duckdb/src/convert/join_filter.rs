// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use tracing::debug;
use vortex::array::ArrayRef;
use vortex::array::ExecutionCtx;
use vortex::array::IntoArray;
use vortex::array::arrays::BoolArray;
use vortex::array::arrays::ConstantArray;
use vortex::array::arrays::bool::BoolArrayExt;
use vortex::array::builtins::ArrayBuiltins;
use vortex::array::stats::StatsSessionExt;
use vortex::array::stats::rewrite::StatsRewriteRule;
use vortex::array::validity::Validity;
use vortex::buffer::BitBuffer;
use vortex::buffer::Buffer;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::error::VortexResult;
use vortex::error::vortex_ensure;
use vortex::expr::BoundExpression;
use vortex::expr::Expression;
use vortex::expr::bound;
use vortex::expr::dynamic;
use vortex::expr::lit;
use vortex::mask::Mask;
use vortex::scalar_fn::Arity;
use vortex::scalar_fn::ChildName;
use vortex::scalar_fn::ExecutionArgs;
use vortex::scalar_fn::ScalarFnId;
use vortex::scalar_fn::ScalarFnVTable;
use vortex::scalar_fn::ScalarFnVTableExt;
use vortex::scalar_fn::fns::operators::CompareOperator;
use vortex::session::VortexSession;
use vortex::session::registry::CachedId;

use super::dtype::FromLogicalType;
use crate::cpp;
use crate::duckdb::BloomSnapshot;
use crate::duckdb::DataChunk;
use crate::duckdb::LogicalType;
use crate::duckdb::OptionalFilterPolicy;
use crate::duckdb::duckdb_vector_size;
use crate::exporter::new_bloom_key_exporter;

#[derive(Clone)]
struct BloomFilter;

#[derive(Clone, Debug)]
struct BloomOptions {
    snapshot: Arc<BloomSnapshot>,
    dtype: DType,
}

impl PartialEq for BloomOptions {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.snapshot, &other.snapshot) && self.dtype == other.dtype
    }
}
impl Eq for BloomOptions {}
impl Hash for BloomOptions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.snapshot).hash(state);
        self.dtype.hash(state);
    }
}
impl Display for BloomOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.dtype)
    }
}

pub(super) fn bloom_filter(snapshot: BloomSnapshot, col: Expression) -> VortexResult<Expression> {
    let dtype = DType::from_logical_type(&snapshot.key_type(), Nullability::Nullable)?;
    Ok(BloomFilter.new_expr(
        BloomOptions {
            snapshot: Arc::new(snapshot),
            dtype,
        },
        [col],
    ))
}

impl ScalarFnVTable for BloomFilter {
    type Options = BloomOptions;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.duckdb.bloom_filter");
        *ID
    }

    fn arity(&self, _options: &Self::Options) -> Arity {
        Arity::Exact(1)
    }

    fn child_name(&self, _options: &Self::Options, _child_idx: usize) -> ChildName {
        ChildName::from("key")
    }

    fn return_dtype(&self, options: &Self::Options, args: &[DType]) -> VortexResult<DType> {
        vortex_ensure!(
            options.dtype.eq_ignore_nullability(&args[0]),
            "Bloom key type does not match DuckDB's hash type"
        );
        Ok(DType::Bool(Nullability::NonNullable))
    }

    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let array = args.get(0)?;
        let len = array.len();
        let exporter = new_bloom_key_exporter(array, ctx)?;
        let batch_size = duckdb_vector_size();
        let mut chunk = DataChunk::new([
            options.snapshot.key_type(),
            LogicalType::new(cpp::DUCKDB_TYPE::DUCKDB_TYPE_UBIGINT),
        ]);
        let mut bits = vec![0u8; len.div_ceil(8)];
        // DataChunk's vector cache reuses both key and hash buffers across native-size batches.
        for offset in (0..len).step_by(batch_size) {
            let count = batch_size.min(len - offset);
            chunk.reset();
            exporter.export(offset, count, chunk.get_vector_mut(0), ctx)?;
            // SAFETY: the snapshot is immutable; the chunk owns distinct key and hash vectors
            // with capacity for `count` rows, and the key vector contains `count` keys.
            // Each full batch is byte-aligned and `bits` has ceil(len / 8) bytes.
            unsafe {
                cpp::duckdb_vx_bloom_snapshot_apply(
                    options.snapshot.as_ptr(),
                    chunk.get_vector(0).as_ptr(),
                    chunk.get_vector(1).as_ptr(),
                    count as _,
                    bits[offset / 8..].as_mut_ptr(),
                );
            }
        }
        Ok(BoolArray::new(
            BitBuffer::new(Buffer::from(bits), len),
            Validity::NonNullable,
        )
        .into_array())
    }

    fn is_infallible(&self, _options: &Self::Options) -> bool {
        true
    }
}

#[derive(Clone)]
struct AdaptiveFilter;

#[derive(Debug, Default)]
struct Sample {
    accepted: u64,
    processed: u64,
}

#[derive(Debug)]
struct AdaptiveState {
    policy: OptionalFilterPolicy,
    sample: Mutex<Sample>,
    paused: AtomicBool,
    sampling_complete: AtomicBool,
}

#[derive(Clone, Debug)]
struct AdaptiveOptions(Arc<AdaptiveState>);

impl PartialEq for AdaptiveOptions {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for AdaptiveOptions {}
impl Hash for AdaptiveOptions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}
impl Display for AdaptiveOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "threshold={}, sample_rows={}",
            self.0.policy.threshold, self.0.policy.sample_rows
        )
    }
}

pub(super) fn adaptive_filter(expr: Expression, policy: OptionalFilterPolicy) -> Expression {
    let options = AdaptiveOptions(Arc::new(AdaptiveState {
        policy,
        sample: Mutex::new(Sample::default()),
        paused: AtomicBool::new(false),
        sampling_complete: AtomicBool::new(policy.sample_rows == 0),
    }));
    let state = Arc::clone(&options.0);
    // Keep the active flag in the expression tree so zone pruning observes policy changes.
    let active = dynamic(
        CompareOperator::Eq,
        move || Some((!state.paused.load(Ordering::Relaxed)).into()),
        DType::Bool(Nullability::NonNullable),
        false,
        lit(true),
    );
    AdaptiveFilter.new_expr(options, [expr, active])
}

/// Registers pruning support for DuckDB's adaptive join filters.
pub(crate) fn initialize_join_filters(session: &VortexSession) {
    session.stats().register_rewrite(AdaptiveFilterPrune);
}

#[derive(Debug)]
struct AdaptiveFilterPrune;

impl StatsRewriteRule for AdaptiveFilterPrune {
    fn scalar_fn_id(&self) -> ScalarFnId {
        AdaptiveFilter.id()
    }

    fn falsify(
        &self,
        expr: &BoundExpression,
        session: &VortexSession,
    ) -> VortexResult<Option<BoundExpression>> {
        // The child's bounds prune only while the filter is active. A paused filter passes
        // every row, so its child alone is no longer a sound falsifier.
        Ok(expr
            .child(0)
            .falsify(session)?
            .map(|proof| bound::and(expr.child(1).clone(), proof)))
    }
}

impl AdaptiveState {
    fn observe(&self, mask: &Mask) {
        if self.sampling_complete.load(Ordering::Relaxed) {
            return;
        }
        let mut sample = self.sample.lock();
        let remaining = self.policy.sample_rows.saturating_sub(sample.processed);
        let count = mask
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        if count == 0 {
            return;
        }
        sample.accepted += mask.slice(0..count).true_count() as u64;
        sample.processed += count as u64;
        if sample.processed >= self.policy.sample_rows {
            if sample.accepted as f64 / sample.processed as f64 >= f64::from(self.policy.threshold)
            {
                self.paused.store(true, Ordering::Relaxed);
                debug!(
                    accepted = sample.accepted,
                    processed = sample.processed,
                    threshold = self.policy.threshold,
                    "paused optional join filter"
                );
            }
            self.sampling_complete.store(true, Ordering::Relaxed);
        }
    }
}

impl ScalarFnVTable for AdaptiveFilter {
    type Options = AdaptiveOptions;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.duckdb.adaptive_filter");
        *ID
    }

    fn arity(&self, _options: &Self::Options) -> Arity {
        Arity::Exact(2)
    }

    fn child_name(&self, _options: &Self::Options, child_idx: usize) -> ChildName {
        ChildName::from(if child_idx == 0 {
            "predicate"
        } else {
            "active"
        })
    }

    fn return_dtype(&self, _options: &Self::Options, args: &[DType]) -> VortexResult<DType> {
        vortex_ensure!(
            args.iter().all(|dtype| matches!(dtype, DType::Bool(_))),
            "optional filter and active flag must be boolean"
        );
        Ok(DType::Bool(Nullability::NonNullable))
    }

    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        if options.0.paused.load(Ordering::Relaxed) {
            return Ok(ConstantArray::new(true, args.row_count()).into_array());
        }
        if options.0.sampling_complete.load(Ordering::Relaxed) {
            return args.get(0)?.fill_null(false);
        }
        let mask = args
            .get(0)?
            .execute::<BoolArray>(ctx)?
            .to_mask_fill_null_false(ctx);
        options.0.observe(&mask);
        Ok(BoolArray::new(mask.into_bit_buffer(), Validity::NonNullable).into_array())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex::array::VortexSessionExecute;
    use vortex::array::arrays::Dict;
    use vortex::array::arrays::DictArray;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::assert_arrays_eq;
    use vortex::expr::root;
    use vortex::scalar_fn::VecExecutionArgs;
    use vortex::scalar_fn::fns::dynamic::DynamicExprUpdates;

    use super::*;
    use crate::SESSION;

    #[test]
    fn sampled_selective_filter_preserves_dictionary_encoding() -> VortexResult<()> {
        let expr = adaptive_filter(
            root(),
            OptionalFilterPolicy {
                threshold: 0.75,
                sample_rows: 8,
            },
        );
        let options = expr.as_::<AdaptiveFilter>();
        let input = DictArray::try_new(
            PrimitiveArray::from_iter([1u8, 0, 0, 0, 1, 0, 0, 0]).into_array(),
            BoolArray::from_iter([false, true]).into_array(),
        )?
        .into_array();
        let args = VecExecutionArgs::new(
            vec![input.clone(), ConstantArray::new(true, 8).into_array()],
            8,
        );
        let mut ctx = SESSION.create_execution_ctx();
        AdaptiveFilter.execute(options, &args, &mut ctx)?;
        let output = AdaptiveFilter.execute(options, &args, &mut ctx)?;
        assert!(
            output.is::<Dict>(),
            "sampling should not expand subsequent dictionary masks"
        );
        assert_arrays_eq!(output, input, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case::weak([true, true, true, true, true, true, false, false], true)]
    #[case::selective([true, false, false, false, false, false, false, false], false)]
    fn optional_filter_adapts_after_sampling(
        #[case] values: [bool; 8],
        #[case] paused: bool,
    ) -> VortexResult<()> {
        let options = AdaptiveOptions(Arc::new(AdaptiveState {
            policy: OptionalFilterPolicy {
                threshold: 0.75,
                sample_rows: 8,
            },
            sample: Mutex::new(Sample::default()),
            paused: AtomicBool::new(false),
            sampling_complete: AtomicBool::new(false),
        }));
        let mut ctx = SESSION.create_execution_ctx();
        let input = BoolArray::from_iter(values).into_array();
        let active = ConstantArray::new(true, 8).into_array();
        let args = VecExecutionArgs::new(vec![input.clone(), active.clone()], 8);
        let output = AdaptiveFilter.execute(&options, &args, &mut ctx)?;
        assert_arrays_eq!(output, input, &mut ctx);
        assert_eq!(options.0.paused.load(Ordering::Relaxed), paused);

        let input = BoolArray::from_iter([false; 8]).into_array();
        let args = VecExecutionArgs::new(vec![input, active], 8);
        let output = AdaptiveFilter.execute(&options, &args, &mut ctx)?;
        assert_arrays_eq!(output, BoolArray::from_iter([paused; 8]), &mut ctx);
        Ok(())
    }

    #[test]
    fn sampling_uses_only_the_budgeted_prefix() {
        let state = AdaptiveState {
            policy: OptionalFilterPolicy {
                threshold: 0.75,
                sample_rows: 8,
            },
            sample: Mutex::new(Sample::default()),
            paused: AtomicBool::new(false),
            sampling_complete: AtomicBool::new(false),
        };
        state.observe(&Mask::from_buffer(BitBuffer::from_iter([
            true, true, true, true, true, true, false, false, false, false, false, false,
        ])));
        assert!(state.paused.load(Ordering::Relaxed));
        assert_eq!(state.sample.lock().processed, 8);
    }

    #[test]
    fn adaptive_filter_pruning_tracks_paused_state() -> VortexResult<()> {
        let dtype = DType::Bool(Nullability::NonNullable);
        let expr = adaptive_filter(
            lit(false),
            OptionalFilterPolicy {
                threshold: 0.75,
                sample_rows: 8,
            },
        )
        .bind(&dtype)?;
        let proof = expr
            .falsify(&SESSION)?
            .ok_or_else(|| vortex::error::vortex_err!("missing optional-filter pruning proof"))?;
        let updates = DynamicExprUpdates::new(&expr)
            .ok_or_else(|| vortex::error::vortex_err!("missing optional-filter updates"))?;
        let mut ctx = SESSION.create_execution_ctx();
        let input = BoolArray::from_iter([false; 8]).into_array();
        let output = input
            .clone()
            .apply_bound(&proof)?
            .execute::<BoolArray>(&mut ctx)?;
        assert!(output.to_mask_fill_null_false(&mut ctx).all_true());
        assert_eq!(updates.version(), 0);
        expr.as_::<AdaptiveFilter>()
            .0
            .paused
            .store(true, Ordering::Relaxed);
        assert_eq!(updates.version(), 1);
        let output = input.apply_bound(&proof)?.execute::<BoolArray>(&mut ctx)?;
        assert!(output.to_mask_fill_null_false(&mut ctx).all_false());
        Ok(())
    }

    #[test]
    fn paused_filter_skips_predicate_execution() -> VortexResult<()> {
        struct UnavailablePredicate;
        impl ExecutionArgs for UnavailablePredicate {
            fn get(&self, _index: usize) -> VortexResult<ArrayRef> {
                Err(vortex::error::vortex_err!(
                    "predicate should not be executed"
                ))
            }
            fn num_inputs(&self) -> usize {
                2
            }
            fn row_count(&self) -> usize {
                8
            }
        }
        let options = AdaptiveOptions(Arc::new(AdaptiveState {
            policy: OptionalFilterPolicy {
                threshold: 0.75,
                sample_rows: 8,
            },
            sample: Mutex::new(Sample::default()),
            paused: AtomicBool::new(true),
            sampling_complete: AtomicBool::new(true),
        }));
        let mut ctx = SESSION.create_execution_ctx();
        let output = AdaptiveFilter.execute(&options, &UnavailablePredicate, &mut ctx)?;
        assert_arrays_eq!(output, BoolArray::from_iter([true; 8]), &mut ctx);
        Ok(())
    }
}
