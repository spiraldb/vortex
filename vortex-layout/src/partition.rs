// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::LazyLock;

use vortex_array::dtype::DType;
use vortex_array::dtype::FieldName;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::expr as unbound;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::Expression;
use vortex_array::expr::bound::get_item;
use vortex_array::expr::bound::pack;
use vortex_array::scalar_fn::ScalarFnId;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_utils::aliases::hash_map::HashMap;

pub type FieldIndex = usize;

/// The slots a rule-matched node reads and its replacement expression.
///
/// The replacement is bound against the scope of its reads: with no slots it
/// reads nothing, with one slot its scope is that slot's dtype, and with two
/// or more slots its scope is the positional struct of the slot dtypes built
/// by [`slot_read`].
pub struct NodeTouches {
    pub slots: Vec<FieldIndex>,
    pub replacement: BoundExpression,
}

/// The scope of a multi-slot [`NodeTouches`] replacement: the declared
/// fields packed into a struct under their positional names, with `dtypes`
/// in slot declaration order.
pub fn replacement_scope(dtypes: &[DType]) -> BoundExpression {
    BoundExpression::new_root(DType::Struct(
        StructFields::new(
            (0..dtypes.len()).map(positional_name).collect(),
            dtypes.to_vec(),
        ),
        Nullability::NonNullable,
    ))
}

/// The read of declared field `position` inside a multi-slot
/// [`NodeTouches`] replacement.
pub fn slot_read(dtypes: &[DType], position: usize) -> BoundExpression {
    get_item(positional_name(position), replacement_scope(dtypes))
}

/// The scope a replacement is bound to.
fn scope_of(expr: &BoundExpression) -> Option<&DType> {
    if expr.is_root() {
        return Some(expr.dtype());
    }
    expr.children().iter().find_map(scope_of)
}

pub trait PartitionRule: Debug + Send + Sync {
    fn apply(&self, node: &BoundExpression) -> VortexResult<Option<NodeTouches>>;
}

#[derive(Debug)]
pub struct PartitionRules {
    pub scope: &'static dyn PartitionRule,
    pub by_fn: HashMap<ScalarFnId, &'static dyn PartitionRule>,
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub slot: FieldIndex,
    pub expr: BoundExpression,
}

#[derive(Debug, Clone)]
pub struct PartitionedExpr {
    pub root: BoundExpression,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone)]
pub enum Partitioning {
    Mono(Option<FieldIndex>, BoundExpression),
    Many(PartitionedExpr),
}

pub fn partition_expr(
    expr: &BoundExpression,
    rules: &PartitionRules,
) -> VortexResult<Partitioning> {
    let mut order = linearize(expr, rules)?;
    let mut parts = Parts::default();
    let mut results: Vec<Option<Analyzed>> = Vec::with_capacity(order.len());
    for at in 0..order.len() {
        let analyzed = match order[at].touches.take() {
            Some(NodeTouches { slots, replacement }) => match slots.len() {
                0 | 1 => Analyzed::Bound(slots.first().copied(), Cow::Owned(replacement)),
                _ => {
                    let fields = scope_of(&replacement)
                        .and_then(DType::as_struct_fields_opt)
                        .ok_or_else(|| vortex_err!("A replacement must read a declared field"))?;
                    vortex_ensure!(fields.nfields() == slots.len());
                    let scope = unbound::pack(
                        slots.into_iter().enumerate().map(|(position, slot)| {
                            let read = BoundExpression::new_root(
                                fields
                                    .field_by_index(position)
                                    .vortex_expect("every declared slot has a scope field"),
                            );
                            (positional_name(position), parts.cut(slot, read))
                        }),
                        Nullability::NonNullable,
                    );
                    Analyzed::Root(to_unbound(&replacement, Some(&scope))?)
                }
            },
            None => analyze(at, &order, &mut results, &mut parts)?,
        };
        results.push(Some(analyzed));
    }

    let root = match results
        .pop()
        .flatten()
        .vortex_expect("the root node is analyzed")
    {
        Analyzed::Bound(slot, expr) => return Ok(Partitioning::Mono(slot, expr.into_owned())),
        Analyzed::Root(root) => root,
    };

    let mut partitions = Vec::with_capacity(parts.parts.len());
    let mut partition_dtypes = Vec::with_capacity(parts.parts.len());
    for part in parts.parts {
        let mut subs: Vec<Option<BoundExpression>> = vec![None; part.positions.len()];
        for (expr, sub) in part.positions {
            subs[sub] = Some(expr);
        }
        let expr = pack(
            subs.into_iter()
                .map(|sub| sub.vortex_expect("partition positions are dense"))
                .enumerate()
                .map(|(sub, expr)| (positional_name(sub), expr)),
            Nullability::NonNullable,
        );
        partition_dtypes.push(expr.dtype().clone());
        partitions.push(Partition {
            slot: part.slot,
            expr,
        });
    }

    let scope_names: FieldNames = (0..partitions.len()).map(positional_name).collect();
    let scope_dtype = DType::Struct(
        StructFields::new(scope_names, partition_dtypes),
        Nullability::NonNullable,
    );

    let root = root.bind(&scope_dtype)?.optimize_recursive()?;
    Ok(Partitioning::Many(PartitionedExpr { root, partitions }))
}

const INTERNED_NAMES: usize = 100;
static POSITIONAL_NAMES: LazyLock<[FieldName; INTERNED_NAMES]> =
    LazyLock::new(|| std::array::from_fn(|position| FieldName::from(format!("{position}"))));

pub(crate) fn positional_name(position: usize) -> FieldName {
    POSITIONAL_NAMES
        .get(position)
        .cloned()
        .unwrap_or_else(|| format!("{position}").into())
}

enum Analyzed<'a> {
    Bound(Option<FieldIndex>, Cow<'a, BoundExpression>),
    Root(Expression),
}

struct Entry<'a> {
    node: &'a BoundExpression,
    size: usize,
    touches: Option<NodeTouches>,
}

/// Lay the expression out in post-order, applying rules on the way down: a
/// rule-matched node enters as a leaf and its subtree is not descended.
/// `size` counts a node's own order entries, so children are recoverable by
/// walking backwards.
fn linearize<'a>(
    expr: &'a BoundExpression,
    rules: &PartitionRules,
) -> VortexResult<Vec<Entry<'a>>> {
    let mut order = Vec::new();
    let mut stack = vec![(expr, 0usize, 0usize)];
    while let Some((node, child, mark)) = stack.pop() {
        if child == 0 {
            if let Some(rule) = rule_for(rules, node)
                && let Some(touches) = rule.apply(node)?
            {
                order.push(Entry {
                    node,
                    size: 1,
                    touches: Some(touches),
                });
                continue;
            }
            vortex_ensure!(!node.is_root());
        }
        if child < node.children().len() {
            stack.push((node, child + 1, mark));
            stack.push((&node.children()[child], 0, order.len()));
        } else {
            order.push(Entry {
                node,
                size: order.len() - mark + 1,
                touches: None,
            });
        }
    }
    Ok(order)
}

fn analyze<'a>(
    at: usize,
    order: &[Entry<'a>],
    results: &mut [Option<Analyzed<'a>>],
    parts: &mut Parts,
) -> VortexResult<Analyzed<'a>> {
    let node = order[at].node;
    let scalar_fn = node
        .as_scalar()
        .vortex_expect("non-root expressions are scalar functions");

    let mut indices = Vec::with_capacity(node.children().len());
    let mut next = at;
    for _ in 0..node.children().len() {
        let child = next - 1;
        indices.push(child);
        next = child + 1 - order[child].size;
    }
    indices.reverse();

    let mut slot: Option<FieldIndex> = None;
    let mut many = false;
    let mut changed = false;
    let mut children = Vec::with_capacity(indices.len());
    for index in indices {
        let analyzed = results[index].take().vortex_expect("children are analyzed");
        match &analyzed {
            Analyzed::Bound(child_slot, expr) => {
                changed |= matches!(expr, Cow::Owned(_));
                if let Some(child_slot) = child_slot {
                    match slot {
                        None => slot = Some(*child_slot),
                        Some(slot) if slot == *child_slot => {}
                        Some(_) => many = true,
                    }
                }
            }
            Analyzed::Root(_) => many = true,
        }
        children.push(analyzed);
    }

    Ok(if many {
        let children = children
            .into_iter()
            .map(|child| match child {
                Analyzed::Bound(None, expr) => to_unbound(&expr, None),
                Analyzed::Bound(Some(slot), expr) => Ok(parts.cut(slot, expr.into_owned())),
                Analyzed::Root(expr) => Ok(expr),
            })
            .collect::<VortexResult<Vec<_>>>()?;
        Analyzed::Root(Expression::try_new(scalar_fn.clone(), children)?)
    } else if !changed {
        Analyzed::Bound(slot, Cow::Borrowed(node))
    } else {
        let exprs = children
            .into_iter()
            .map(|child| match child {
                Analyzed::Bound(_, expr) => Ok(expr.into_owned()),
                Analyzed::Root(_) => {
                    Err(vortex_err!("A single-coordinate node has a spanning child"))
                }
            })
            .collect::<VortexResult<Vec<_>>>()?;
        Analyzed::Bound(slot, Cow::Owned(BoundExpression::try_new(scalar_fn.clone(), exprs)?))
    })
}

struct PartBuild {
    slot: FieldIndex,
    positions: HashMap<BoundExpression, usize>,
}

impl PartBuild {
    fn push_dedup(&mut self, expr: BoundExpression) -> usize {
        let next = self.positions.len();
        *self.positions.entry(expr).or_insert(next)
    }
}

#[derive(Default)]
struct Parts {
    index: HashMap<FieldIndex, usize>,
    parts: Vec<PartBuild>,
}

impl Parts {
    fn cut(&mut self, slot: FieldIndex, expr: BoundExpression) -> Expression {
        let part = *self.index.entry(slot).or_insert_with(|| {
            self.parts.push(PartBuild {
                slot,
                positions: HashMap::default(),
            });
            self.parts.len() - 1
        });
        let sub = self.parts[part].push_dedup(expr);
        unbound::get_item(
            positional_name(sub),
            unbound::get_item(positional_name(part), unbound::root()),
        )
    }
}

fn rule_for<'a>(
    rules: &'a PartitionRules,
    node: &BoundExpression,
) -> Option<&'a dyn PartitionRule> {
    if node.is_root() {
        return Some(rules.scope);
    }
    let scalar_fn = node.as_scalar()?;
    rules.by_fn.get(&scalar_fn.id()).copied()
}

fn to_unbound(expr: &BoundExpression, scope: Option<&Expression>) -> VortexResult<Expression> {
    let Some(scalar_fn) = expr.as_scalar() else {
        return scope.cloned().ok_or_else(|| vortex_err!("scope reference"));
    };
    let children = expr
        .children()
        .iter()
        .map(|child| to_unbound(child, scope))
        .collect::<VortexResult<Vec<_>>>()?;
    Expression::try_new(scalar_fn.clone(), children)
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;
    use vortex_array::dtype::PType;
    use vortex_array::expr::bound::get_item;
    use vortex_array::expr::bound::is_not_null;

    use super::*;
    use crate::plan::physical::struct_partition_rules;

    fn i32() -> DType {
        DType::Primitive(PType::I32, Nullability::NonNullable)
    }

    fn struct_of(fields: &[(&str, DType)], nullability: Nullability) -> DType {
        DType::Struct(
            StructFields::from_iter(fields.iter().map(|(name, dtype)| (*name, dtype.clone()))),
            nullability,
        )
    }

    fn two_field_struct() -> DType {
        struct_of(&[("a", i32()), ("b", i32())], Nullability::NonNullable)
    }

    fn fmt_slot(slot: Option<FieldIndex>) -> String {
        match slot {
            None => "none".to_string(),
            Some(FieldIndex::MAX) => "validity".to_string(),
            Some(slot) => slot.to_string(),
        }
    }

    fn line(label: &str, expr: &BoundExpression) -> String {
        let scope = scope_of(expr)
            .map(|dtype| dtype.to_string())
            .unwrap_or_else(|| "-".to_string());
        format!("{label} scope={scope}: {expr}\n")
    }

    fn snap(partitioning: Partitioning) -> String {
        match partitioning {
            Partitioning::Mono(slot, expr) => line(&format!("mono {}", fmt_slot(slot)), &expr),
            Partitioning::Many(PartitionedExpr { root, partitions }) => {
                let mut out = String::new();
                for partition in &partitions {
                    out.push_str(&line(
                        &format!("partition {}", fmt_slot(Some(partition.slot))),
                        &partition.expr,
                    ));
                }
                out.push_str(&line("root", &root));
                out
            }
        }
    }

    fn partition(expr: Expression, scope: &DType) -> VortexResult<String> {
        Ok(snap(partition_expr(
            &expr.bind(scope)?,
            struct_partition_rules(),
        )?))
    }

    #[test]
    fn is_not_null_of_non_nullable() -> VortexResult<()> {
        let root = BoundExpression::new_root(two_field_struct());
        let expr = pack(
            [
                ("v", is_not_null(root.clone())),
                ("a", get_item("a", root.clone())),
                ("b", get_item("b", root)),
            ],
            Nullability::NonNullable,
        );
        assert_snapshot!(snap(partition_expr(&expr, struct_partition_rules())?), @"
        partition 0 scope=i32: pack(0: $)
        partition 1 scope=i32: pack(0: $)
        root scope={0={0=i32}, 1={0=i32}}: pack(v: true, a: $.0.0, b: $.1.0)
        ");
        Ok(())
    }

    #[test]
    fn equal_cuts() -> VortexResult<()> {
        let root = BoundExpression::new_root(two_field_struct());
        let a = get_item("a", root.clone());
        let b = get_item("b", root);
        let expr = pack(
            [("x", a.clone()), ("y", b), ("z", a)],
            Nullability::NonNullable,
        );
        assert_snapshot!(snap(partition_expr(&expr, struct_partition_rules())?), @"
        partition 0 scope=i32: pack(0: $)
        partition 1 scope=i32: pack(0: $)
        root scope={0={0=i32}, 1={0=i32}}: pack(x: $.0.0, y: $.1.0, z: $.0.0)
        ");
        Ok(())
    }

    #[test]
    fn is_not_null_one_partition() -> VortexResult<()> {
        let expr = unbound::is_not_null(unbound::get_item("a", unbound::root()));
        assert_snapshot!(partition(expr, &two_field_struct())?, @"mono 0 scope=i32: is_not_null($)");
        Ok(())
    }

    #[test]
    fn is_not_null_two_fields() -> VortexResult<()> {
        let scope = struct_of(&[("a", i32()), ("b", i32())], Nullability::Nullable);
        let expr = unbound::is_not_null(unbound::get_item("a", unbound::root()));
        assert_snapshot!(partition(expr, &scope)?, @"
        partition validity scope=bool: pack(0: $)
        partition 0 scope=i32: pack(0: $)
        root scope={0={0=bool}, 1={0=i32}}: $.0.0
        ");
        Ok(())
    }

    #[test]
    fn test_pack() -> VortexResult<()> {
        let expr = unbound::get_item(
            "a",
            unbound::get_item(
                "a",
                unbound::pack([("a", unbound::root())], Nullability::NonNullable),
            ),
        );
        assert_snapshot!(partition(expr, &two_field_struct())?, @"
        partition 0 scope=i32: pack(0: $)
        partition 1 scope=i32: pack(0: $)
        root scope={0={0=i32}, 1={0=i32}}: $.0.0
        ");
        Ok(())
    }

    #[test]
    fn test_ab_greater_c() -> VortexResult<()> {
        let inner = struct_of(&[("b", i32())], Nullability::NonNullable);
        let scope = struct_of(&[("a", inner), ("c", i32())], Nullability::NonNullable);
        let expr = unbound::gt(
            unbound::get_item("b", unbound::get_item("a", unbound::root())),
            unbound::get_item("c", unbound::root()),
        );
        assert_snapshot!(partition(expr, &scope)?, @"
        partition 0 scope={b=i32}: pack(0: $.b)
        partition 1 scope=i32: pack(0: $)
        root scope={0={0=i32}, 1={0=i32}}: ($.0.0 > $.1.0)
        ");
        Ok(())
    }

    #[test]
    fn test_x_plus_1() -> VortexResult<()> {
        let scope = struct_of(&[("x", i32()), ("y", i32())], Nullability::NonNullable);
        let expr = unbound::gt(
            unbound::checked_add(unbound::get_item("x", unbound::root()), unbound::lit(1i32)),
            unbound::get_item("y", unbound::root()),
        );
        assert_snapshot!(partition(expr, &scope)?, @"
        partition 0 scope=i32: pack(0: ($ + 1i32))
        partition 1 scope=i32: pack(0: $)
        root scope={0={0=i32}, 1={0=i32}}: ($.0.0 > $.1.0)
        ");
        Ok(())
    }

    #[test]
    fn test_aa_greater_ba() -> VortexResult<()> {
        let inner = struct_of(&[("a", i32())], Nullability::NonNullable);
        let scope = struct_of(
            &[("a", inner.clone()), ("b", inner)],
            Nullability::NonNullable,
        );
        let expr = unbound::gt(
            unbound::get_item("a", unbound::get_item("a", unbound::root())),
            unbound::get_item("a", unbound::get_item("b", unbound::root())),
        );
        assert_snapshot!(partition(expr, &scope)?, @"
        partition 0 scope={a=i32}: pack(0: $.a)
        partition 1 scope={a=i32}: pack(0: $.a)
        root scope={0={0=i32}, 1={0=i32}}: ($.0.0 > $.1.0)
        ");
        Ok(())
    }

    #[test]
    fn test_nested_and() -> VortexResult<()> {
        let a = unbound::get_item("a", unbound::root());
        let b = unbound::get_item("b", unbound::root());
        let expr = unbound::and(
            unbound::and(
                unbound::gt(a.clone(), unbound::lit(5i32)),
                unbound::gt(b, unbound::lit(5i32)),
            ),
            unbound::lt(a, unbound::lit(1i32)),
        );
        assert_snapshot!(partition(expr, &two_field_struct())?, @"
        partition 0 scope=i32: pack(0: ($ > 5i32), 1: ($ < 1i32))
        partition 1 scope=i32: pack(0: ($ > 5i32))
        root scope={0={0=bool, 1=bool}, 1={0=bool}}: (($.0.0 and $.1.0) and $.0.1)
        ");
        Ok(())
    }

    #[test]
    fn test_duplicate_predicate() -> VortexResult<()> {
        let a = unbound::get_item("a", unbound::root());
        let b = unbound::get_item("b", unbound::root());
        let gt5_a = unbound::gt(a, unbound::lit(5i32));
        let expr = unbound::and(
            unbound::and(gt5_a.clone(), unbound::gt(b, unbound::lit(5i32))),
            gt5_a,
        );
        assert_snapshot!(partition(expr, &two_field_struct())?, @"
        partition 0 scope=i32: pack(0: ($ > 5i32))
        partition 1 scope=i32: pack(0: ($ > 5i32))
        root scope={0={0=bool}, 1={0=bool}}: (($.0.0 and $.1.0) and $.0.0)
        ");
        Ok(())
    }

    #[test]
    fn test_abc_greater_abd() -> VortexResult<()> {
        let leaf = struct_of(&[("c", i32()), ("d", i32())], Nullability::NonNullable);
        let middle = struct_of(&[("b", leaf)], Nullability::NonNullable);
        let scope = struct_of(&[("a", middle)], Nullability::NonNullable);
        let expr = unbound::gt(
            unbound::get_item(
                "c",
                unbound::get_item("b", unbound::get_item("a", unbound::root())),
            ),
            unbound::get_item(
                "d",
                unbound::get_item("b", unbound::get_item("a", unbound::root())),
            ),
        );
        assert_snapshot!(partition(expr, &scope)?, @"mono 0 scope={b={c=i32, d=i32}}: ($.b.c > $.b.d)");
        Ok(())
    }
}
