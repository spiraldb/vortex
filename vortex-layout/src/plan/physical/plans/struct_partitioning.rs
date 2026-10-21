// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::LazyLock;

use vortex_array::dtype::DType;
use vortex_array::dtype::FieldName;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::bound::lit;
use vortex_array::expr::bound::mask;
use vortex_array::expr::bound::not;
use vortex_array::expr::bound::pack;
use vortex_array::scalar_fn::ScalarFnVTable;
use vortex_array::scalar_fn::fns::get_item::GetItem;
use vortex_array::scalar_fn::fns::is_not_null::IsNotNull;
use vortex_array::scalar_fn::fns::is_null::IsNull;
use vortex_array::scalar_fn::fns::select::Select;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_utils::aliases::hash_map::HashMap;

use crate::partition::FieldIndex;
use crate::partition::NodeTouches;
use crate::partition::PartitionRule;
use crate::partition::PartitionRules;
use crate::partition::slot_read;

/// The virtual validity coordinate of a struct scope.
pub(crate) const VALIDITY_SLOT: FieldIndex = FieldIndex::MAX;

static RULES: LazyLock<PartitionRules> = LazyLock::new(|| {
    let mut by_fn = HashMap::new();
    by_fn.insert(
        GetItem.id(),
        &StructGetItemRule as &'static dyn PartitionRule,
    );
    by_fn.insert(Select.id(), &StructSelectRule as _);
    by_fn.insert(IsNotNull.id(), &StructNullCheckRule { negated: false } as _);
    by_fn.insert(IsNull.id(), &StructNullCheckRule { negated: true } as _);
    PartitionRules {
        scope: &StructScopeRule,
        by_fn,
    }
});

/// The partition rules of a struct scope.
pub(crate) fn struct_partition_rules() -> &'static PartitionRules {
    &RULES
}

fn struct_fields(scope: &DType) -> VortexResult<&StructFields> {
    scope
        .as_struct_fields_opt()
        .ok_or_else(|| vortex_err!("Struct partition rules require a struct scope"))
}

fn field_dtype(fields: &StructFields, index: FieldIndex) -> VortexResult<DType> {
    fields
        .field_by_index(index)
        .ok_or_else(|| vortex_err!("No field {index} in the struct scope"))
}

fn validity_dtype() -> DType {
    DType::Bool(Nullability::NonNullable)
}

fn anchored_scope(node: &BoundExpression) -> Option<&DType> {
    node.children()
        .first()
        .filter(|child| child.is_root())
        .map(BoundExpression::dtype)
}

fn packed_replacement(names: FieldNames, dtypes: &[DType], nullable: bool) -> BoundExpression {
    let offset = usize::from(nullable);
    let packed = pack(
        names
            .iter()
            .enumerate()
            .map(|(position, name)| (name.clone(), slot_read(dtypes, position + offset))),
        Nullability::NonNullable,
    );
    if nullable {
        mask(packed, slot_read(dtypes, 0))
    } else {
        packed
    }
}

fn select_fields(
    scope: &DType,
    fields: &StructFields,
    names: FieldNames,
    indices: Vec<FieldIndex>,
) -> VortexResult<NodeTouches> {
    if !scope.is_nullable() {
        if let [index] = indices.as_slice() {
            let field = BoundExpression::new_root(field_dtype(fields, *index)?);
            let packed = pack([(names[0].clone(), field)], Nullability::NonNullable);
            return Ok(NodeTouches {
                slots: indices,
                replacement: packed,
            });
        }
        if indices.is_empty() {
            let packed = pack(
                Vec::<(FieldName, BoundExpression)>::new(),
                Nullability::NonNullable,
            );
            return Ok(NodeTouches {
                slots: indices,
                replacement: packed,
            });
        }
    }

    let mut slots = Vec::with_capacity(indices.len() + 1);
    let mut dtypes = Vec::with_capacity(indices.len() + 1);
    if scope.is_nullable() {
        slots.push(VALIDITY_SLOT);
        dtypes.push(validity_dtype());
    }
    for index in indices {
        slots.push(index);
        dtypes.push(field_dtype(fields, index)?);
    }
    let replacement = packed_replacement(names, &dtypes, scope.is_nullable());
    Ok(NodeTouches { slots, replacement })
}

#[derive(Debug)]
struct StructScopeRule;

impl PartitionRule for StructScopeRule {
    fn apply(&self, node: &BoundExpression) -> VortexResult<Option<NodeTouches>> {
        let scope = node.dtype();
        let fields = struct_fields(scope)?;
        select_fields(
            scope,
            fields,
            fields.names().clone(),
            (0..fields.nfields()).collect(),
        )
        .map(Some)
    }
}

#[derive(Debug)]
struct StructGetItemRule;

impl PartitionRule for StructGetItemRule {
    fn apply(&self, node: &BoundExpression) -> VortexResult<Option<NodeTouches>> {
        let Some(scope) = anchored_scope(node) else {
            return Ok(None);
        };
        let field_name = node.as_::<GetItem>();
        let fields = struct_fields(scope)?;
        let index = fields
            .find(field_name)
            .ok_or_else(|| vortex_err!("Field {field_name} not found in the struct scope"))?;
        let dtype = field_dtype(fields, index)?;
        if !scope.is_nullable() {
            return Ok(Some(NodeTouches {
                slots: vec![index],
                replacement: BoundExpression::new_root(dtype),
            }));
        }

        let dtypes = [validity_dtype(), dtype];
        let replacement = mask(slot_read(&dtypes, 1), slot_read(&dtypes, 0));
        Ok(Some(NodeTouches {
            slots: vec![VALIDITY_SLOT, index],
            replacement,
        }))
    }
}

#[derive(Debug)]
struct StructSelectRule;

impl PartitionRule for StructSelectRule {
    fn apply(&self, node: &BoundExpression) -> VortexResult<Option<NodeTouches>> {
        let Some(scope) = anchored_scope(node) else {
            return Ok(None);
        };
        let selection = node.as_::<Select>();
        let fields = struct_fields(scope)?;
        let names = selection.normalize_to_included_fields(fields.names())?;
        let indices = names
            .iter()
            .map(|name| {
                fields
                    .find(name)
                    .ok_or_else(|| vortex_err!("Selected field {name} not found in scope"))
            })
            .collect::<VortexResult<Vec<_>>>()?;
        select_fields(scope, fields, names, indices).map(Some)
    }
}

#[derive(Debug)]
struct StructNullCheckRule {
    negated: bool,
}

impl PartitionRule for StructNullCheckRule {
    fn apply(&self, node: &BoundExpression) -> VortexResult<Option<NodeTouches>> {
        let Some(scope) = anchored_scope(node) else {
            return Ok(None);
        };
        Ok(Some(if scope.is_nullable() {
            let read = BoundExpression::new_root(validity_dtype());
            NodeTouches {
                slots: vec![VALIDITY_SLOT],
                replacement: if self.negated { not(read) } else { read },
            }
        } else {
            NodeTouches {
                slots: Vec::new(),
                replacement: lit(!self.negated),
            }
        }))
    }
}
