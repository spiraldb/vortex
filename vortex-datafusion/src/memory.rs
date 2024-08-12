use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_expr::{TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{ExecutionMode, ExecutionPlan, Partitioning, PlanProperties};
use itertools::Itertools;
use vortex::array::ChunkedArray;
use vortex::{Array, ArrayDType as _};

use crate::datatype::infer_schema;
use crate::plans::{RowSelectorExec, TakeRowsExec};
use crate::{can_be_pushed_down, get_filter_projection, VortexScanExec};

/// A [`TableProvider`] that exposes an existing Vortex Array to the DataFusion SQL engine.
///
/// Only arrays that have a top-level [struct type](vortex_dtype::StructDType) can be exposed as
/// a table to DataFusion.
#[derive(Debug, Clone)]
pub struct VortexMemTable {
    array: ChunkedArray,
    schema_ref: SchemaRef,
    options: VortexMemTableOptions,
}

impl VortexMemTable {
    /// Build a new table provider from an existing [struct type](vortex_dtype::StructDType) array.
    ///
    /// # Panics
    ///
    /// Creation will panic if the provided array is not of `DType::Struct` type.
    pub fn new(array: Array, options: VortexMemTableOptions) -> Self {
        let arrow_schema = infer_schema(array.dtype());
        let schema_ref = SchemaRef::new(arrow_schema);

        let array = match ChunkedArray::try_from(&array) {
            Ok(a) => a,
            _ => {
                let dtype = array.dtype().clone();
                ChunkedArray::try_new(vec![array], dtype).unwrap()
            }
        };

        Self {
            array,
            schema_ref,
            options,
        }
    }
}

#[async_trait]
impl TableProvider for VortexMemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema_ref)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Plan an array scan.
    ///
    /// Currently, projection pushdown is supported, but not filter pushdown.
    /// The array is flattened directly into the nearest Arrow-compatible encoding.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let filter_exprs = if filters.is_empty() {
            None
        } else {
            Some(filters)
        };

        let output_projection: Vec<usize> = match projection {
            None => (0..self.schema_ref.fields().len()).collect(),
            Some(proj) => proj.clone(),
        };

        match filter_exprs {
            // If there is a filter expression, we execute in two phases, first performing a filter
            // on the input to get back row indices, and then taking the remaining struct columns
            // using the calculated indices from the filter.
            Some(filter_exprs) => {
                let filter_projection =
                    get_filter_projection(filter_exprs, self.schema_ref.clone())
                    .map_err(DataFusionError::from)?;

                Ok(make_filter_then_take_plan(
                    self.schema_ref.clone(),
                    filter_exprs,
                    filter_projection,
                    self.array.clone(),
                    output_projection.clone(),
                    state,
                ))
            }

            // If no filters were pushed down, we materialize the entire StructArray into a
            // RecordBatch and let DataFusion process the entire query.
            _ => {
                let output_schema = Arc::new(
                    self.schema_ref
                        .project(output_projection.as_slice())
                        .expect("project output schema"),
                );
                let plan_properties = PlanProperties::new(
                    EquivalenceProperties::new(output_schema),
                    // non-pushdown scans execute in single partition, where the partition
                    // yields one RecordBatch per chunk in the input ChunkedArray
                    Partitioning::UnknownPartitioning(1),
                    ExecutionMode::Bounded,
                );

                Ok(Arc::new(VortexScanExec {
                    array: self.array.clone(),
                    scan_projection: output_projection.clone(),
                    plan_properties,
                }))
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // In the case the caller has configured this provider with filter pushdown disabled,
        // do not attempt to apply any filters at scan time.
        if !self.options.enable_pushdown {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }

        filters
            .iter()
            .map(|expr| {
                if can_be_pushed_down(expr) {
                    Ok(TableProviderFilterPushDown::Exact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .try_collect()
    }
}

/// Optional configurations to pass when loading a [VortexMemTable].
#[derive(Debug, Clone)]
pub struct VortexMemTableOptions {
    pub enable_pushdown: bool,
}

impl Default for VortexMemTableOptions {
    fn default() -> Self {
        Self {
            enable_pushdown: true,
        }
    }
}

impl VortexMemTableOptions {
    pub fn with_pushdown(mut self, enable_pushdown: bool) -> Self {
        self.enable_pushdown = enable_pushdown;
        self
    }
}

/// Construct an operator plan that executes in two stages.
///
/// The first plan stage only materializes the columns related to the provided set of filter
/// expressions. It evaluates the filters into a row selection.
///
/// The second stage receives the row selection above and dispatches a `take` on the remaining
/// columns.
fn make_filter_then_take_plan(
    schema: SchemaRef,
    filter_exprs: &[Expr],
    filter_projection: Vec<usize>,
    chunked_array: ChunkedArray,
    output_projection: Vec<usize>,
    _session_state: &dyn Session,
) -> Arc<dyn ExecutionPlan> {
    let row_selector_op = Arc::new(RowSelectorExec::new(
        filter_exprs,
        filter_projection,
        &chunked_array,
    ));

    Arc::new(TakeRowsExec::new(
        schema.clone(),
        &output_projection,
        row_selector_op.clone(),
        &chunked_array,
    ))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::Int64Type;
    use datafusion::functions_aggregate::count::count_distinct;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{Column, TableReference};
    use datafusion_expr::{and, col, lit, BinaryExpr, Expr, Operator};
    use vortex::array::{PrimitiveArray, StructArray, VarBinArray};
    use vortex::validity::Validity;
    use vortex::{Array, IntoArray};
    use vortex_dtype::{DType, Nullability};

    use crate::memory::VortexMemTableOptions;
    use crate::{can_be_pushed_down, SessionContextExt as _};

    fn presidents_array() -> Array {
        let names = VarBinArray::from_vec(
            vec![
                "Washington",
                "Adams",
                "Jefferson",
                "Madison",
                "Monroe",
                "Adams",
            ],
            DType::Utf8(Nullability::NonNullable),
        );
        let term_start = PrimitiveArray::from_vec(
            vec![1789u16, 1797, 1801, 1809, 1817, 1825],
            Validity::NonNullable,
        );

        StructArray::from_fields(&[
            ("president", names.into_array()),
            ("term_start", term_start.into_array()),
        ])
        .into_array()
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_datafusion_pushdown() {
        let ctx = SessionContext::new();

        let df = ctx.read_mem_vortex(presidents_array()).unwrap();

        let distinct_names = df
            .filter(col("term_start").gt_eq(lit(1795)))
            .unwrap()
            .aggregate(vec![], vec![count_distinct(col("president"))])
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(distinct_names.len(), 1);

        assert_eq!(
            *distinct_names[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .first()
                .unwrap(),
            4i64
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_datafusion_no_pushdown() {
        let ctx = SessionContext::new();

        let df = ctx
            .read_mem_vortex_opts(
                presidents_array(),
                // Disable pushdown. We run this test to make sure that the naive codepath also
                // produces correct results and does not panic anywhere.
                VortexMemTableOptions::default().with_pushdown(false),
            )
            .unwrap();

        let distinct_names = df
            .filter(col("term_start").gt_eq(lit(1795)))
            .unwrap()
            .filter(col("term_start").lt(lit(2000)))
            .unwrap()
            .aggregate(vec![], vec![count_distinct(col("president"))])
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(distinct_names.len(), 1);

        assert_eq!(
            *distinct_names[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .first()
                .unwrap(),
            4i64
        );
    }

    #[test]
    fn test_can_be_pushed_down0() {
        let e = BinaryExpr {
            left: Box::new(
                Column {
                    relation: Some(TableReference::Bare {
                        table: "orders".into(),
                    }),
                    name: "o_orderstatus".to_string(),
                }
                .into(),
            ),
            op: Operator::Eq,
            right: Box::new(lit("F")),
        };
        let e = Expr::BinaryExpr(e);

        assert!(can_be_pushed_down(&e));
    }

    #[test]
    fn test_can_be_pushed_down1() {
        let e = lit("hello");

        assert!(can_be_pushed_down(&e));
    }

    #[test]
    fn test_can_be_pushed_down2() {
        let e = lit(3);

        assert!(can_be_pushed_down(&e));
    }

    #[test]
    fn test_can_be_pushed_down3() {
        let e = BinaryExpr {
            left: Box::new(col("nums")),
            op: Operator::Modulo,
            right: Box::new(lit(5)),
        };
        let e = Expr::BinaryExpr(e);

        assert!(!can_be_pushed_down(&e));
    }

    #[test]
    fn test_can_be_pushed_down4() {
        let e = and((col("a")).eq(lit(2u64)), col("b").eq(lit(true)));
        assert!(can_be_pushed_down(&e));
    }
}
