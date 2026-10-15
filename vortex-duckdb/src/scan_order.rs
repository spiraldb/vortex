// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use tracing::debug;
use vortex::array::ArrayRef;
use vortex::array::Canonical;
use vortex::array::ExecutionCtx;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::extension::ExtensionArrayExt;
use vortex::array::match_each_decimal_value_type;
use vortex::array::match_each_native_ptype;
use vortex::buffer::BufferString;
use vortex::dtype::Nullability;
use vortex::error::VortexResult;
use vortex::expr::stats::Stat;
use vortex::file::VortexFile;
use vortex::layout::LayoutChildType;
use vortex::layout::layouts::zoned::LegacyStats;
use vortex::layout::layouts::zoned::Zoned;
use vortex::mask::AllOr;
use vortex::mask::Mask;
use vortex::scalar::Scalar;

use crate::SESSION;
use crate::table_function::Split;

#[derive(Clone, Debug)]
pub(crate) struct ScanOrder {
    pub column: String,
    pub descending: bool,
    pub nulls_first: bool,
}

struct Zone {
    rows: Range<u64>,
    value: Option<Scalar>,
    may_have_nulls: bool,
}

#[derive(Debug)]
struct Priority {
    value: Option<Scalar>,
    may_have_nulls: bool,
}

fn for_each_valid_index(mask: &Mask, mut visit: impl FnMut(usize)) {
    match mask.bit_buffer() {
        AllOr::All => (0..mask.len()).for_each(visit),
        AllOr::None => {}
        AllOr::Some(bits) => bits.for_each_set_index(&mut visit),
    }
}

impl ScanOrder {
    fn compare(&self, lhs: &Priority, rhs: &Priority) -> Ordering {
        let category = |priority: &Priority| {
            if self.nulls_first && priority.may_have_nulls {
                if priority.value.is_none() { 0 } else { 1 }
            } else if priority.value.is_some() {
                2
            } else {
                3
            }
        };
        category(lhs)
            .cmp(&category(rhs))
            .then_with(|| match (&lhs.value, &rhs.value) {
                (Some(lhs), Some(rhs)) => {
                    let ordering = lhs.partial_cmp(rhs).unwrap_or(Ordering::Equal);
                    if self.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    }
                }
                _ => Ordering::Equal,
            })
    }
}

/// Schedules promising row ranges first; it never discards rows or substitutes for SQL ordering.
pub(crate) async fn order_splits(
    file: &VortexFile,
    order: &ScanOrder,
    ranges: Vec<Range<u64>>,
    splits: &mut Vec<Split>,
) -> VortexResult<()> {
    if splits.len() < 2 {
        return Ok(());
    }
    let mut pending = vec![(Arc::clone(file.footer().layout()), 0, false)];
    let mut zones = Vec::new();
    let mut ctx = SESSION.create_execution_ctx();
    while let Some((layout, offset, selected)) = pending.pop() {
        let zone_map = if !selected {
            None
        } else if let Some(zoned) = layout.as_opt::<Zoned>() {
            zoned
                .read_zone_map(file.segment_source(), &SESSION)
                .await?
                .map(|map| (map, zoned.zone_len() as u64))
        } else if let Some(zoned) = layout.as_opt::<LegacyStats>() {
            zoned
                .read_zone_map(file.segment_source(), &SESSION)
                .await?
                .map(|map| (map, zoned.zone_len() as u64))
        } else {
            None
        };
        if let Some((map, zone_len)) = zone_map {
            let stat = if order.descending {
                Stat::Max
            } else {
                Stat::Min
            };
            if let Some(values) = map.statistic(stat)?
                && let Some(values) = scalar_values(values, &mut ctx)?
            {
                let null_counts = map
                    .statistic(Stat::NullCount)?
                    .map(|array| scalar_values(array, &mut ctx))
                    .transpose()?
                    .flatten();
                for (index, value) in values.into_iter().enumerate() {
                    let start = offset + index as u64 * zone_len;
                    let end = (start + zone_len).min(offset + layout.row_count());
                    let may_have_nulls = null_counts.as_ref().is_none_or(|counts| {
                        counts[index]
                            .as_ref()
                            .is_none_or(|count| count.as_primitive().is_zero() != Some(true))
                    });
                    zones.push(Zone {
                        rows: start..end,
                        value,
                        may_have_nulls,
                    });
                }
                continue;
            }
        }
        for slot in 0..layout.nslots() {
            let Some(kind) = layout.slot_type(slot) else {
                continue;
            };
            let child_selected = match &kind {
                LayoutChildType::Transparent(_) | LayoutChildType::Chunk(_) => selected,
                LayoutChildType::Field(name) if !selected && name.as_ref() == order.column => true,
                _ => continue,
            };
            if let Some(child) = layout.slot(slot)? {
                pending.push((
                    child,
                    offset + kind.row_offset().unwrap_or(0),
                    child_selected,
                ));
            }
        }
    }
    if zones.is_empty() {
        return Ok(());
    }
    zones.sort_unstable_by_key(|zone| zone.rows.start);
    let priorities = ranges
        .iter()
        .map(|range| {
            let mut priority = Priority {
                value: None,
                may_have_nulls: false,
            };
            let first = zones.partition_point(|zone| zone.rows.end <= range.start);
            for zone in zones[first..]
                .iter()
                .take_while(|zone| zone.rows.start < range.end)
            {
                priority.may_have_nulls |= zone.may_have_nulls;
                if let Some(value) = &zone.value {
                    let replace = priority.value.as_ref().is_none_or(|previous| {
                        if order.descending {
                            value > previous
                        } else {
                            value < previous
                        }
                    });
                    if replace {
                        priority.value = Some(value.clone());
                    }
                }
            }
            priority
        })
        .collect::<Vec<_>>();
    let mut indexed = std::mem::take(splits)
        .into_iter()
        .enumerate()
        .collect::<Vec<_>>();
    indexed.sort_unstable_by(|(lhs, _), (rhs, _)| {
        order
            .compare(&priorities[*lhs], &priorities[*rhs])
            .then_with(|| lhs.cmp(rhs))
    });
    debug!(column = order.column, descending = order.descending, nulls_first = order.nulls_first,
        zones = zones.len(), first_range = ?ranges[indexed[0].0], "ordered Vortex scan splits");
    splits.extend(indexed.into_iter().map(|(_, split)| split));
    Ok(())
}

// Zone statistics are small, but decode their backing buffers and validity once rather than
// invoking the scalar execution stack separately for every zone.
fn scalar_values(
    array: ArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<Vec<Option<Scalar>>>> {
    let dtype = array.dtype().clone();
    let values = match array.execute::<Canonical>(ctx)? {
        Canonical::Primitive(array) => {
            let validity = array.validity()?.execute_mask(array.len(), ctx)?;
            match_each_native_ptype!(array.ptype(), |T| {
                let buffer = array.to_buffer::<T>();
                let mut values = vec![None; array.len()];
                for_each_valid_index(&validity, |index| {
                    let value = Scalar::primitive(buffer[index], Nullability::Nullable);
                    if value.partial_cmp(&value).is_some() {
                        values[index] = Some(value);
                    }
                });
                values
            })
        }
        Canonical::Decimal(array) => {
            let validity = array.validity()?.execute_mask(array.len(), ctx)?;
            match_each_decimal_value_type!(array.values_type(), |T| {
                let buffer = array.buffer::<T>();
                let mut values = vec![None; array.len()];
                for_each_valid_index(&validity, |index| {
                    values[index] = Some(Scalar::decimal(
                        buffer[index].into(),
                        array.decimal_dtype(),
                        Nullability::Nullable,
                    ));
                });
                values
            })
        }
        Canonical::Extension(array) => {
            return scalar_values(array.storage_array().clone(), ctx)?
                .map(|values| {
                    values
                        .into_iter()
                        .map(|value| {
                            value
                                .map(|value| Scalar::try_new(dtype.clone(), value.into_value()))
                                .transpose()
                        })
                        .collect::<VortexResult<Vec<_>>>()
                })
                .transpose();
        }
        Canonical::VarBinView(array) if dtype.is_utf8() => {
            let validity = array.validity()?.execute_mask(array.len(), ctx)?;
            let mut values = vec![None; array.len()];
            let mut status: VortexResult<()> = Ok(());
            for_each_valid_index(&validity, |index| {
                match BufferString::try_from(array.bytes_at(index)) {
                    Ok(value) => values[index] = Some(Scalar::utf8(value, Nullability::Nullable)),
                    Err(error) => status = Err(error),
                }
            });
            status?;
            values
        }
        _ => return Ok(None),
    };
    Ok(Some(values))
}
