// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Display;
use std::fmt::Formatter;

use smallvec::smallvec;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use crate::ArrayRef;
use crate::array::Array;
use crate::array::ArrayParts;
use crate::array::TypedArrayRef;
use crate::array::child_to_validity;
use crate::array::validity_to_child;
use crate::array_slots;
use crate::arrays::Bool;
use crate::arrays::Masked;
use crate::arrays::bool::BoolArrayExt;
use crate::expr::stats::Precision;
use crate::expr::stats::Stat;
use crate::expr::stats::StatsProviderExt;
use crate::validity::Validity;

#[array_slots(Masked)]
pub struct MaskedSlots {
    /// The underlying child array being masked.
    #[slot(0)]
    pub child: ArrayRef,
    /// The validity bitmap defining which elements are non-null.
    #[slot(1)]
    pub validity: Option<ArrayRef>,
}

#[derive(Clone, Debug)]
pub struct MaskedData;

impl Display for MaskedData {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

pub trait MaskedArrayExt: TypedArrayRef<Masked> + MaskedArraySlotsExt {
    fn masked_validity(&self) -> Validity {
        child_to_validity(
            self.slots()[MaskedSlots::VALIDITY].as_ref(),
            self.dtype().nullability(),
        )
    }
}
impl<T: TypedArrayRef<Masked>> MaskedArrayExt for T {}

impl MaskedData {
    pub(crate) fn try_new(
        child_len: usize,
        child_all_valid: bool,
        validity: Validity,
    ) -> VortexResult<Self> {
        if matches!(validity, Validity::NonNullable) {
            vortex_bail!("MaskedArray must have nullable validity, got {validity:?}")
        }

        if !child_all_valid {
            vortex_bail!("MaskedArray children must not have nulls");
        }

        if let Some(validity_len) = validity.maybe_len()
            && validity_len != child_len
        {
            vortex_bail!("Validity must be the same length as a MaskedArray's child");
        }

        // MaskedArray's nullability is determined solely by its validity, not the child's dtype.
        // The child can have nullable dtype but must not have any actual null values.
        Ok(Self)
    }
}

/// Returns `false` when `child` is known to contain nulls.
///
/// Construction never executes the child, so this only inspects its validity variant, a cached
/// null count, or a canonical host-resident validity bitmap. Nulls behind any other validity
/// array are not detected here.
pub(super) fn child_may_be_all_valid(child: &ArrayRef) -> VortexResult<bool> {
    if child.is_empty() {
        return Ok(true);
    }

    Ok(match child.validity()? {
        Validity::NonNullable | Validity::AllValid => true,
        Validity::AllInvalid => false,
        Validity::Array(validity) => {
            if let Precision::Exact(null_count) =
                child.statistics().get_as::<usize>(Stat::NullCount)
            {
                null_count == 0
            } else if validity.is_host()
                && let Some(bools) = validity.as_opt::<Bool>()
            {
                bools.to_bit_buffer().true_count() == bools.len()
            } else {
                true
            }
        }
    })
}

impl Array<Masked> {
    /// Constructs a new `MaskedArray`.
    pub fn try_new(child: ArrayRef, validity: Validity) -> VortexResult<Self> {
        let dtype = child.dtype().as_nullable();
        let len = child.len();
        let validity_slot = validity_to_child(&validity, len);
        let data = MaskedData::try_new(len, child_may_be_all_valid(&child)?, validity)?;
        Ok(unsafe {
            Array::from_parts_unchecked(ArrayParts::new(
                Masked,
                dtype,
                len,
                data,
                smallvec![Some(child), validity_slot],
            ))
        })
    }
}
