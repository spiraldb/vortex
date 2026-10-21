// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Join filters a V2 scan evaluates on the keys it reads, before decoding the other columns.
//!
//! A hash join publishes a dynamic filter on the keys of its probe side once it has built its
//! table: `bounds AND key IN (...)` when the build side is collected into one table, or
//! `CASE hash(key) % N WHEN i THEN bounds_i AND lookup_i ... ELSE false END` when it is hash
//! partitioned. Vortex cannot express the lookups, so the scan hands the rows its own filter
//! kept to a [`DynamicRowFilter`], which reads only the key columns. The filter is advisory:
//! the join still checks every row, so passing more rows than the filter would is correct.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use arrow_array::Array;
use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_schema::Field;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::BooleanBufferBuilder;
use datafusion_common::arrow::array::UInt32Array;
use datafusion_common::arrow::compute::take_record_batch;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::expressions::CaseExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::expressions::InListExpr;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr_common::physical_expr::snapshot_physical_expr;
use datafusion_physical_plan::joins::HashTableLookupExpr;
use vortex::array::ArrayRef;
use vortex::array::VortexSessionExecute;
use vortex::array::expr::BoundExpression;
use vortex::buffer::BitBuffer;
use vortex::error::VortexResult;
use vortex::error::vortex_err;
use vortex::layout::scan::v2::RowFilter;
use vortex::mask::Mask;
use vortex::session::VortexSession;
use vortex_arrow::ArrowSessionExt;

/// Rows a filter sees before it may decide it is not worth evaluating.
const MIN_ROWS: u64 = 1 << 18;

/// The largest fraction of rows a filter may keep and still be evaluated.
const MAX_KEPT: f64 = 0.8;

/// A DataFusion dynamic filter evaluated on the key columns a V2 scan reads for it.
pub(crate) struct DynamicRowFilter {
    /// The key columns, bound to the file's dtype.
    projection: BoundExpression,
    /// The Arrow type the key columns are read as.
    target: Field,
    /// The dynamic filter, over the key columns alone.
    expr: Arc<dyn PhysicalExpr>,
    session: VortexSession,
    /// Rows evaluated, and rows kept, across the scan's splits.
    seen: AtomicU64,
    kept: AtomicU64,
}

impl DynamicRowFilter {
    /// A filter evaluating `expr`, whose columns index `target`'s fields, on the rows
    /// `projection` reads.
    pub(crate) fn new(
        projection: BoundExpression,
        target: Field,
        expr: Arc<dyn PhysicalExpr>,
        session: VortexSession,
    ) -> Self {
        Self {
            projection,
            target,
            expr,
            session,
            seen: AtomicU64::new(0),
            kept: AtomicU64::new(0),
        }
    }

    fn evaluate_keys(&self, keys: &RecordBatch) -> DFResult<BooleanArray> {
        // The filter's current state, without the bounds a membership test already implies.
        let expr = strip_bounds(&snapshot_physical_expr(Arc::clone(&self.expr))?)?;
        if let Some(case) = expr.downcast_ref::<CaseExpr>()
            && let Some(mask) = partitioned_lookup(case, keys)?
        {
            return Ok(mask);
        }
        let mask = expr.evaluate(keys)?.into_array(keys.num_rows())?;
        Ok(mask.as_boolean().clone())
    }
}

impl RowFilter for DynamicRowFilter {
    fn projection(&self) -> &BoundExpression {
        &self.projection
    }

    fn is_active(&self) -> bool {
        let seen = self.seen.load(Ordering::Relaxed);
        seen < MIN_ROWS || (self.kept.load(Ordering::Relaxed) as f64) <= seen as f64 * MAX_KEPT
    }

    fn evaluate(&self, array: ArrayRef) -> VortexResult<Mask> {
        let len = array.len();
        let mut ctx = self.session.create_execution_ctx();
        let keys = self
            .session
            .arrow()
            .execute_arrow(array, Some(&self.target), &mut ctx)?;
        let keys = RecordBatch::from(keys.as_struct().clone());
        let mask = self
            .evaluate_keys(&keys)
            .map_err(|e| vortex_err!("Failed to evaluate dynamic filter {}: {e}", self.expr))?;
        // A null result removes the row, as it does in a filter.
        let mask = match mask.nulls() {
            Some(nulls) => mask.values() & nulls.inner(),
            None => mask.values().clone(),
        };
        let mask = Mask::from_buffer(BitBuffer::from(mask));
        self.seen.fetch_add(len as u64, Ordering::Relaxed);
        self.kept
            .fetch_add(mask.true_count() as u64, Ordering::Relaxed);
        Ok(mask)
    }
}

/// Whether `expr` only compares columns with literals, as a join's bounds on its keys do.
fn is_bound(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<BinaryExpr>()
        .is_some_and(|binary| match binary.op() {
            Operator::And => is_bound(binary.left()) && is_bound(binary.right()),
            Operator::GtEq | Operator::LtEq | Operator::Gt | Operator::Lt => {
                binary.left().downcast_ref::<Column>().is_some()
                    && binary.right().downcast_ref::<Literal>().is_some()
            }
            _ => false,
        })
}

/// Whether `expr` tests the membership of keys in a join's build side.
fn has_membership(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<HashTableLookupExpr>().is_some()
        || expr.downcast_ref::<InListExpr>().is_some()
        || expr.children().into_iter().any(has_membership)
}

/// Drops bounds conjoined with a membership test: a key in the build side lies within its
/// bounds, so they remove no further rows and only cost their comparisons.
fn strip_bounds(expr: &Arc<dyn PhysicalExpr>) -> DFResult<Arc<dyn PhysicalExpr>> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && *binary.op() == Operator::And
    {
        let left = strip_bounds(binary.left())?;
        let right = strip_bounds(binary.right())?;
        if is_bound(&left) && has_membership(&right) {
            return Ok(right);
        }
        if is_bound(&right) && has_membership(&left) {
            return Ok(left);
        }
        return Ok(Arc::new(BinaryExpr::new(left, Operator::And, right)));
    }
    let children = expr
        .children()
        .into_iter()
        .map(strip_bounds)
        .collect::<DFResult<Vec<_>>>()?;
    if children.is_empty() {
        return Ok(Arc::clone(expr));
    }
    Arc::clone(expr).with_new_children(children)
}

fn index(value: u64) -> DFResult<usize> {
    usize::try_from(value).map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Evaluates `CASE hash % N WHEN i THEN filter_i ... ELSE false END`, the filter of a hash
/// partitioned join, by hashing every row once and evaluating each partition's filter on that
/// partition's rows alone. `None` when `case` has another shape.
fn partitioned_lookup(case: &CaseExpr, keys: &RecordBatch) -> DFResult<Option<BooleanArray>> {
    let Some(modulo) = case
        .expr()
        .and_then(|base| base.downcast_ref::<BinaryExpr>())
        .filter(|base| *base.op() == Operator::Modulo)
    else {
        return Ok(None);
    };
    let Some(partitions) =
        modulo
            .right()
            .downcast_ref::<Literal>()
            .and_then(|literal| match literal.value() {
                ScalarValue::UInt64(Some(partitions)) if *partitions > 0 => Some(*partitions),
                _ => None,
            })
    else {
        return Ok(None);
    };
    if case
        .else_expr()
        .and_then(|other| other.downcast_ref::<Literal>())
        .is_none_or(|other| other.value() != &ScalarValue::Boolean(Some(false)))
    {
        return Ok(None);
    }
    let mut filters = vec![None; index(partitions)?];
    for (when, then) in case.when_then_expr() {
        let Some(ScalarValue::UInt64(Some(partition))) = when
            .downcast_ref::<Literal>()
            .map(|literal| literal.value())
        else {
            return Ok(None);
        };
        let Some(filter) = filters.get_mut(index(*partition)?) else {
            return Ok(None);
        };
        *filter = Some(Arc::clone(then));
    }

    let len = keys.num_rows();
    let hashes = modulo.left().evaluate(keys)?.into_array(len)?;
    let mut rows = vec![Vec::new(); filters.len()];
    for (row, hash) in hashes
        .as_primitive::<UInt64Type>()
        .values()
        .iter()
        .enumerate()
    {
        rows[index(hash % partitions)?]
            .push(u32::try_from(row).map_err(|e| DataFusionError::External(Box::new(e)))?);
    }
    let mut result = BooleanBufferBuilder::new(len);
    result.append_n(len, false);
    for (rows, filter) in rows.into_iter().zip(filters) {
        // A partition without a filter has no build rows: its probe rows match nothing.
        let Some(filter) = filter.filter(|_| !rows.is_empty()) else {
            continue;
        };
        let rows = UInt32Array::from(rows);
        let partition = take_record_batch(keys, &rows)?;
        let mask = filter
            .evaluate(&partition)?
            .into_array(partition.num_rows())?;
        let mask = mask.as_boolean();
        for (index, row) in rows.values().iter().enumerate() {
            if mask.is_valid(index) && mask.value(index) {
                result.set_bit(*row as usize, true);
            }
        }
    }
    Ok(Some(BooleanArray::new(result.finish(), None)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_array::UInt64Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::BinaryExpr;
    use datafusion_physical_expr::expressions::CaseExpr;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::expressions::in_list;
    use datafusion_physical_expr::expressions::lit;
    use rstest::rstest;

    use super::partitioned_lookup;
    use super::strip_bounds;

    fn keys() -> anyhow::Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("k", DataType::UInt64, false)]);
        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from_iter_values(0..64))],
        )?)
    }

    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    /// `k >= low AND k <= high`.
    fn bounds(batch: &RecordBatch, low: u64, high: u64) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
        let k = col("k", batch.schema_ref())?;
        Ok(binary(
            binary(Arc::clone(&k), Operator::GtEq, lit(low)),
            Operator::And,
            binary(k, Operator::LtEq, lit(high)),
        ))
    }

    fn members(batch: &RecordBatch, values: &[u64]) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
        Ok(in_list(
            col("k", batch.schema_ref())?,
            values.iter().map(|value| lit(*value)).collect(),
            &false,
            batch.schema_ref(),
        )?)
    }

    #[test]
    fn bounds_are_dropped_only_beside_a_membership_test() -> anyhow::Result<()> {
        let batch = keys()?;
        let membership = members(&batch, &[3, 7])?;
        let filter = binary(
            bounds(&batch, 3, 7)?,
            Operator::And,
            Arc::clone(&membership),
        );
        assert_eq!(strip_bounds(&filter)?.to_string(), membership.to_string());

        let alone = bounds(&batch, 3, 7)?;
        assert_eq!(strip_bounds(&alone)?.to_string(), alone.to_string());
        Ok(())
    }

    /// The partitioned evaluation agrees with DataFusion's evaluation of the same `CASE`,
    /// including partitions without a branch, which keep no rows.
    #[rstest]
    #[case::every_partition(&[0, 1, 2])]
    #[case::missing_partition(&[0, 2])]
    fn partitioned_lookup_matches_case(#[case] partitions: &[u64]) -> anyhow::Result<()> {
        let batch = keys()?;
        let branches = partitions
            .iter()
            .map(|&partition| {
                let values = (partition..64).step_by(5).collect::<Vec<_>>();
                let then = binary(
                    bounds(&batch, partition, 60)?,
                    Operator::And,
                    members(&batch, &values)?,
                );
                Ok((lit(partition), then))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let case = CaseExpr::try_new(
            Some(binary(
                col("k", batch.schema_ref())?,
                Operator::Modulo,
                lit(3u64),
            )),
            branches,
            Some(lit(false)),
        )?;
        let expected = case.evaluate(&batch)?.into_array(batch.num_rows())?;
        let actual = partitioned_lookup(&case, &batch)?
            .ok_or_else(|| anyhow::anyhow!("a partitioned CASE takes the fast path"))?;
        assert_eq!(&actual, expected.as_boolean());
        assert!(actual.true_count() > 0);
        Ok(())
    }
}
