// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::cell::Cell;
use std::rc::Rc;

use rstest::rstest;
use vortex_error::VortexResult;

use super::ProbeCtx;
use super::ProbeStorage;
use super::ProbeUsage;
use crate::IntoArray;
use crate::VortexSessionExecute;
use crate::arrays::PrimitiveArray;
use crate::arrays::StructArray;

#[derive(Default)]
struct Tracked {
    drops: Rc<Cell<usize>>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[derive(Default)]
struct Large {
    tracked: Tracked,
    padding: [[u8; 32]; 8],
}

#[repr(align(64))]
#[derive(Default)]
struct Aligned(Tracked);

#[derive(Default)]
struct Borrowed<'a>(Option<&'a usize>);

#[test]
fn inline_state_survives_moves_and_drops_once() {
    let mut storage = ProbeStorage::new();
    let initialized = Cell::new(0);
    // SAFETY: every access to this storage uses Tracked.
    let drops = Rc::clone(
        &unsafe {
            storage.get_or_init(|| {
                initialized.set(initialized.get() + 1);
                Tracked::default()
            })
        }
        .drops,
    );
    assert!(storage.heap.is_none());
    let mut moved = (storage, 1);
    // SAFETY: the moved storage still contains Tracked.
    assert!(Rc::ptr_eq(
        &unsafe {
            moved.0.get_or_init(|| {
                initialized.set(initialized.get() + 1);
                Tracked::default()
            })
        }
        .drops,
        &drops
    ));
    drop(moved);
    assert_eq!(initialized.get(), 1);
    assert_eq!(drops.get(), 1);
}

#[test]
fn children_are_lazy_reused_and_bound_to_their_source_slots() -> VortexResult<()> {
    let leaf = PrimitiveArray::from_iter([10i32, 20]).into_array();
    let array = StructArray::from_fields(&[("left", leaf.clone()), ("right", leaf)])?.into_array();
    let mut ctx = crate::array_session().create_execution_ctx();
    let mut probe = ProbeCtx::<usize>::new(&array);
    assert_eq!(probe.children.slots.capacity(), 0);
    // Slot zero is the absent struct validity; invalid requests must not allocate.
    assert!(probe.child(0).is_err());
    assert!(probe.child(99).is_err());
    assert_eq!(probe.children.slots.capacity(), 0);

    let first = std::ptr::from_mut(probe.child(1)?);
    assert!(probe.children.slots[2].is_none());
    let (state, children) = probe.parts();
    let child = children.child(1)?;
    assert!(
        child
            .state
            .as_ref()
            .is_some_and(|state| state.drop_fn.is_none())
    );
    assert!(child.scalar_at(2, &mut ctx).is_err());
    assert!(
        child
            .state
            .as_ref()
            .is_some_and(|state| state.drop_fn.is_none())
    );
    assert_eq!(child.scalar_at(0, &mut ctx)?, 10i32.into());
    *state += 1;
    assert_eq!(*probe.state_mut(), 1);
    assert_eq!(std::ptr::from_mut(probe.child(1)?), first);
    assert!(
        probe
            .child(1)?
            .state
            .as_ref()
            .is_some_and(|state| state.drop_fn.is_some())
    );

    // Identical sources in different slots still get independent probe state.
    assert_ne!(std::ptr::from_mut(probe.child(2)?), first);
    let source = array.slots()[1]
        .as_ref()
        .ok_or_else(|| vortex_error::vortex_err!("missing fixture slot"))?;
    assert!(std::ptr::eq(probe.child(1)?.array(), source));
    assert!(
        probe
            .child(2)?
            .state
            .as_ref()
            .is_some_and(|state| state.drop_fn.is_none())
    );
    let mut other = ProbeCtx::<usize>::new(&array);
    assert!(
        other
            .child(1)?
            .state
            .as_ref()
            .is_some_and(|state| state.drop_fn.is_none())
    );

    let mut moved = (probe, ());
    assert_eq!(std::ptr::from_mut(moved.0.child(1)?), first);
    assert_eq!(moved.0.child(1)?.scalar_at(1, &mut ctx)?, 20i32.into());
    Ok(())
}

#[test]
fn oversized_state_spills_and_drops_once() {
    let mut storage = ProbeStorage::new();
    // SAFETY: every access to this storage uses Large.
    let state = unsafe { storage.get_or_init::<Large>(Large::default) };
    state.padding[7][31] = 42;
    let drops = Rc::clone(&state.tracked.drops);
    assert!(storage.heap.is_some());
    let mut moved = (storage, 1);
    // SAFETY: the moved storage still contains Large.
    assert_eq!(
        unsafe { moved.0.get_or_init::<Large>(Large::default) }.padding[7][31],
        42
    );
    drop(moved);
    assert_eq!(drops.get(), 1);
}

#[test]
fn over_aligned_state_spills() {
    let mut storage = ProbeStorage::new();
    // SAFETY: this storage is only used for Aligned.
    let state = unsafe { storage.get_or_init::<Aligned>(Aligned::default) };
    assert_eq!(std::ptr::from_ref(state).addr() % 64, 0);
    let drops = Rc::clone(&state.0.drops);
    assert!(storage.heap.is_some());
    drop(storage);
    assert_eq!(drops.get(), 1);
}

#[test]
fn state_can_borrow_and_unit_needs_no_allocation() {
    let value = 42;
    let mut borrowed = ProbeStorage::new();
    // SAFETY: the storage always contains Borrowed with the same source lifetime.
    unsafe { borrowed.get_or_init::<Borrowed<'_>>(Borrowed::default) }.0 = Some(&value);
    // SAFETY: same type and source lifetime as initialization.
    assert_eq!(
        unsafe { borrowed.get_or_init::<Borrowed<'_>>(Borrowed::default) }.0,
        Some(&42)
    );
    let mut unit = ProbeStorage::new();
    // SAFETY: this separate storage is only used for ().
    unsafe { unit.get_or_init::<()>(Default::default) };
    assert!(unit.heap.is_none());
}

#[rstest]
#[case(ProbeUsage::Once)]
#[case(ProbeUsage::Repeated)]
fn access_checks_bounds_and_nulls(#[case] usage: ProbeUsage) -> VortexResult<()> {
    let array = PrimitiveArray::from_option_iter([Some(10i32), None, Some(30)]).into_array();
    let mut ctx = crate::array_session().create_execution_ctx();
    let mut probe = array.probe(usage);
    assert!(
        probe
            .state
            .as_ref()
            .is_none_or(|state| state.drop_fn.is_none())
    );
    assert!(probe.scalar_at(3, &mut ctx).is_err());
    assert!(
        probe
            .state
            .as_ref()
            .is_none_or(|state| state.drop_fn.is_none())
    );
    for index in [2, 1, 0, 2] {
        assert_eq!(
            probe.scalar_at(index, &mut ctx)?,
            array.execute_scalar(index, &mut ctx)?
        );
    }
    if usage == ProbeUsage::Once {
        assert!(probe.state.is_none());
    }
    Ok(())
}
