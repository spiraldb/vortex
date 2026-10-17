// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Host types and batch capabilities, separate from typed input and output bindings.

use std::fmt::Debug;

use rowfn_kernels::bit::BitmapView;

/// The native types retained at a host boundary.
pub trait Host: Sized + 'static {
    /// An owned or reference-counted column.
    type Column: Clone;
    /// Native semantic metadata, including outer nullability.
    type NativeType: Clone + Debug + PartialEq;
    /// Invocation resources, including the host allocator.
    type Context;
    /// Terminal invocation errors and observable row errors.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Construct an invocation-contract error.
    fn error(message: &str) -> Self::Error;
}

/// A result in the host's error domain.
pub type HostResult<H, T> = Result<T, <H as Host>::Error>;

/// An explicit scalar or array operand. A length-one array is still an array.
pub struct Operand<H: Host> {
    /// Native storage retained for the invocation.
    pub column: H::Column,
    /// Input metadata supplied by the caller.
    pub dtype: H::NativeType,
    /// Whether the operand represents one value repeated over the logical batch.
    pub scalar: bool,
}

impl<H: Host> Clone for Operand<H> {
    fn clone(&self) -> Self {
        Self { column: self.column.clone(), dtype: self.dtype.clone(), scalar: self.scalar }
    }
}

/// Native type operations required by planning.
pub trait TypeBinding: Host {
    /// Outer nullability only. Nested nullability remains part of the native type.
    fn nullable(dtype: &Self::NativeType) -> bool;
    /// Change only the outer nullability.
    fn with_nullable(dtype: &Self::NativeType, nullable: bool) -> Self::NativeType;
    /// Validate a value-preserving output label. This must not authorize a conversion.
    fn validate_label(storage: &Self::NativeType, output: &Self::NativeType) -> HostResult<Self, ()>;
}

/// Cheap knowledge about input validity, without materializing a lazy mask.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValiditySummary {
    /// Every logical row is valid.
    All,
    /// Every logical row is null.
    None,
    /// Determining valid rows requires host work.
    Unknown,
}

/// A retained selection used only when validity must be resolved.
///
/// # Safety
/// Length and count must stay fixed across shared access. Traversal must visit exactly `count`
/// distinct indices below `len`, in increasing order, unless the callback returns an error.
/// No callback may run after its first error. Violating the index contract can cause undefined
/// behavior in collectors that use the selection as their bounds proof.
pub unsafe trait Selection {
    /// Number of logical rows.
    fn len(&self) -> usize;
    /// Whether the logical row domain is empty.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Number of selected rows.
    fn count(&self) -> usize;
    /// Visit selected indices in increasing order, exactly once each.
    fn for_each(&self, mut visit: impl FnMut(usize)) {
        let Ok(()) = self.try_for_each::<std::convert::Infallible>(|index| {
            visit(index);
            Ok(())
        });
    }
    /// Visit selected indices until the first error, which is returned unchanged.
    fn try_for_each<E>(&self, visit: impl FnMut(usize) -> Result<(), E>) -> Result<(), E>;
}

// SAFETY: validated bitmap chunks retain their logical length and expose only set in-range bits.
unsafe impl Selection for BitmapView<'_> {
    fn len(&self) -> usize { self.len() }
    fn count(&self) -> usize {
        let chunks = self.chunks();
        chunks.iter().map(|word| word.count_ones() as usize).sum::<usize>()
            + chunks.remainder_bits().count_ones() as usize
    }
    fn try_for_each<E>(&self, mut visit: impl FnMut(usize) -> Result<(), E>) -> Result<(), E> {
        let chunks = self.chunks();
        for (chunk, mut word) in chunks.iter().chain([chunks.remainder_bits()]).enumerate() {
            if word == u64::MAX {
                for index in 0..64 { visit(chunk * 64 + index)?; }
            } else {
                while word != 0 {
                    visit(chunk * 64 + word.trailing_zeros() as usize)?;
                    word &= word - 1;
                }
            }
        }
        Ok(())
    }
}

/// Host validity and column operations. Row loops do not call this capability.
pub trait BatchBinding: TypeBinding {
    /// A lazy conjunction of input validity.
    type Validity;
    /// An owned selection whose indices remain stable for its lifetime.
    type Selection: Selection;

    /// Validate column length, scalar representation, and native metadata before dispatch.
    fn validate_operand(operand: &Operand<Self>, rows: usize) -> HostResult<Self, ()>;
    /// Conjoin strict input validity without forcing lazy masks.
    fn validity(inputs: &[Operand<Self>], rows: usize, ctx: &mut Self::Context)
        -> HostResult<Self, Self::Validity>;
    /// Inspect only already-known validity facts.
    fn validity_summary(validity: &Self::Validity) -> ValiditySummary;
    /// Resolve the valid row domain once when a policy requires it.
    fn selection(validity: &Self::Validity, rows: usize, ctx: &mut Self::Context)
        -> HostResult<Self, Self::Selection>;
    /// Filter an operand to the selected domain while preserving its scalar marker and metadata.
    fn filter(input: &Operand<Self>, selection: &Self::Selection, ctx: &mut Self::Context)
        -> HostResult<Self, Operand<Self>>;
    /// Construct an all-null output with the complete planned metadata.
    fn all_null(dtype: &Self::NativeType, rows: usize, ctx: &mut Self::Context)
        -> HostResult<Self, Self::Column>;
    /// Repeat a validated one-row output to the logical batch length.
    fn broadcast(column: Self::Column, rows: usize, ctx: &mut Self::Context)
        -> HostResult<Self, Self::Column>;
    /// Validate all-valid storage, attach input validity, and apply the planned output label.
    /// Dense success must retain lazy validity when the host supports it.
    fn publish(column: Self::Column, storage: &Self::NativeType, output: &Self::NativeType,
        validity: &Self::Validity, rows: usize, ctx: &mut Self::Context)
        -> HostResult<Self, Self::Column>;
}
