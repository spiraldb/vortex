// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Random scalar access with optional, encoding-specific retained state.

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::scalar::Scalar;

/// Whether scalar access should retain preparation for subsequent lookups.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeUsage {
    /// Use temporary resources only, without initializing retained probe state.
    Once,
    /// Allow the encoding to retain preparation and decoded data between lookups.
    Repeated,
}

/// A borrowed scalar accessor that retains encoding-specific state until it is dropped.
///
/// `'a` is the borrow of the root array. Its [`ProbeCtx`] retains a child probe for each
/// requested slot, so one lifetime covers the whole tree and no array handles are cloned.
/// Anything an encoding builds itself, such as a decoded page or validity mask, is owned
/// by its state.
///
/// Construction never allocates or executes the array. Repeated access initializes the
/// encoding's context on the first in-bounds lookup. Small contexts live inline; larger or
/// more aligned contexts use a heap allocation. Child storage and decoding may allocate
/// separately, when first needed.
///
/// `Once` is a caching hint, not a restriction on how many times the probe may be called.
/// Returned scalars own their values and can outlive the probe. Probes are local to a thread.
pub struct ArrayProbe<'a> {
    array: &'a ArrayRef,
    state: Option<ProbeStorage<'a>>,
}

/// Local encoding state and lazy child probes retained for one source array.
///
/// The framework initializes this context once for repeated access and passes it to
/// [`OperationsVTable::probe_scalar`](crate::vtable::OperationsVTable::probe_scalar).
/// One-off access receives `None` instead. `S` is the encoding's associated state type;
/// it may borrow the source tree for `'a` and own any prepared resources.
pub struct ProbeCtx<'a, S> {
    state: S,
    children: ProbeChildren<'a>,
}

impl<'a, S: Default> ProbeCtx<'a, S> {
    pub(crate) fn new(array: &'a ArrayRef) -> Self {
        Self {
            state: S::default(),
            children: ProbeChildren {
                array,
                slots: Vec::new(),
            },
        }
    }
}

impl<'a, S> ProbeCtx<'a, S> {
    /// Access the encoding's retained local state.
    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    /// Get the retained probe for a source slot, creating it on the first request.
    ///
    /// Returns an error for an absent or out-of-bounds slot. Creating a child probe does
    /// not execute the child or initialize its encoding state.
    pub fn child(&mut self, slot: usize) -> VortexResult<&mut ArrayProbe<'a>> {
        self.children.child(slot)
    }

    /// Borrow local state and child access together, allowing disjoint mutable access.
    pub fn parts(&mut self) -> (&mut S, &mut ProbeChildren<'a>) {
        (&mut self.state, &mut self.children)
    }
}

/// Lazy child probes bound to the slots of one source array.
///
/// Obtain this through [`ProbeCtx::parts`] when retaining a mutable borrow of local state
/// while accessing children. Each slot has independent state, even if two slots reference
/// the same array. The slot table allocates on its first valid request; unrequested slots
/// remain empty. Dropping the parent context drops every created child probe.
pub struct ProbeChildren<'a> {
    array: &'a ArrayRef,
    slots: Vec<Option<ArrayProbe<'a>>>,
}

impl<'a> ProbeChildren<'a> {
    /// Get or create a repeated-access probe for the given source slot.
    ///
    /// Returns an error for an absent or out-of-bounds slot without allocating a slot table.
    pub fn child(&mut self, slot: usize) -> VortexResult<&mut ArrayProbe<'a>> {
        let child = self
            .array
            .slots()
            .get(slot)
            .and_then(Option::as_ref)
            .ok_or_else(|| vortex_err!("Probe child slot {slot} is missing"))?;
        if self.slots.is_empty() {
            self.slots.resize_with(self.array.slots().len(), || None);
        }
        Ok(self.slots[slot].get_or_insert_with(|| child.probe(ProbeUsage::Repeated)))
    }
}

impl ArrayRef {
    /// Create an accessor with the requested policy for retaining state between scalar lookups.
    ///
    /// ```
    /// use vortex_array::{IntoArray, ProbeUsage, VortexSessionExecute};
    /// use vortex_array::arrays::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_iter([10i32, 20, 30]).into_array();
    /// let mut ctx = vortex_array::array_session().create_execution_ctx();
    /// let mut probe = array.probe(ProbeUsage::Repeated);
    /// assert_eq!(probe.scalar_at(2, &mut ctx)?, 30i32.into());
    /// assert_eq!(probe.scalar_at(0, &mut ctx)?, 10i32.into());
    /// # Ok::<(), vortex_error::VortexError>(())
    /// ```
    pub fn probe(&self, usage: ProbeUsage) -> ArrayProbe<'_> {
        ArrayProbe {
            array: self,
            state: match usage {
                ProbeUsage::Once => None,
                ProbeUsage::Repeated => Some(ProbeStorage::new()),
            },
        }
    }
}

impl<'a> ArrayProbe<'a> {
    /// The array this probe reads from.
    pub fn array(&self) -> &'a ArrayRef {
        self.array
    }

    /// Read a scalar, including its nullness, preparing and reusing state as appropriate.
    pub fn scalar_at(&mut self, index: usize, ctx: &mut ExecutionCtx) -> VortexResult<Scalar> {
        vortex_ensure!(index < self.array.len(), OutOfBounds: index, 0, self.array.len());
        let scalar =
            self.array
                .dyn_array()
                .probe_scalar(self.array, index, self.state.as_mut(), ctx)?;
        debug_assert_eq!(scalar.dtype(), self.array.dtype(), "Scalar dtype mismatch");
        Ok(scalar)
    }
}

// An open set of external state types cannot be represented by a fixed Rust enum. Erasing
// them inline avoids a mandatory Box allocation; all raw storage operations are confined here.
#[repr(align(16))]
struct InlineStorage(MaybeUninit<[u8; 128]>);

pub(crate) struct ProbeStorage<'a> {
    inline: InlineStorage,
    heap: Option<NonNull<u8>>,
    drop_fn: Option<unsafe fn(*mut u8)>,
    // The erased state may borrow the root array tree, and the type system cannot see that
    // borrow once erased. This marker ties the storage to `'a` (invariantly) so it cannot
    // outlive the borrow. NonNull also keeps the erased storage !Send and !Sync, since we
    // impose neither bound on state types.
    _lifetime: PhantomData<&'a mut &'a ()>,
}

impl<'a> ProbeStorage<'a> {
    fn new() -> Self {
        Self {
            inline: InlineStorage(MaybeUninit::uninit()),
            heap: None,
            drop_fn: None,
            _lifetime: PhantomData,
        }
    }

    /// # Safety
    ///
    /// Every call on this storage must use the same `T`, including its lifetime parameters.
    /// The owning ArrayProbe fixes its source array, and only that array's erased adapter
    /// may initialize/access this storage using its associated ProbeCtx type.
    pub(crate) unsafe fn get_or_init<T: 'a>(&mut self, init: impl FnOnce() -> T) -> &mut T {
        if self.drop_fn.is_none() {
            let value = init();
            if size_of::<T>() <= size_of::<InlineStorage>()
                && align_of::<T>() <= align_of::<InlineStorage>()
            {
                // SAFETY: the size/alignment checks make the inline region suitable for T,
                // and no value has been initialized in this storage yet.
                unsafe { self.inline.0.as_mut_ptr().cast::<T>().write(value) };
                self.drop_fn = Some(drop_inline::<T>);
            } else {
                self.heap = Some(NonNull::from(Box::leak(Box::new(value))).cast());
                self.drop_fn = Some(drop_heap::<T>);
            }
        }
        // SAFETY: initialization above, or the caller's same-T invariant, establishes a
        // live T at this pointer. The exclusive storage borrow provides exclusive access.
        unsafe { &mut *self.as_mut_ptr().cast::<T>() }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        match self.heap {
            Some(ptr) => ptr.as_ptr(),
            None => self.inline.0.as_mut_ptr().cast(),
        }
    }
}

impl Drop for ProbeStorage<'_> {
    fn drop(&mut self) {
        if let Some(drop_fn) = self.drop_fn {
            // SAFETY: the drop shim was installed only after its matching T was initialized.
            // No pointer into inline storage is retained across moves of ProbeStorage.
            unsafe { drop_fn(self.as_mut_ptr()) };
        }
    }
}

unsafe fn drop_inline<T>(ptr: *mut u8) {
    // SAFETY: the storage installed this shim for an initialized inline T.
    unsafe { ptr.cast::<T>().drop_in_place() };
}

unsafe fn drop_heap<T>(ptr: *mut u8) {
    // SAFETY: the storage installed this shim for a T allocated by Box::new and leaked once.
    drop(unsafe { Box::from_raw(ptr.cast::<T>()) });
}

#[cfg(test)]
mod tests;
