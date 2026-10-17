// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Constant specialization and retained typed input tuples.

use rowfn_kernels::lane_kernels::IndexedSource;

use crate::{Host, HostResult, InputBinding, Operand, RowKind, RowView};

/// A supported tuple of row kinds, with owners retained for the entire invocation.
pub trait InputTuple<H: Host>: private::Sealed + 'static {
    /// Decoded owners, including scalar markers.
    type Columns;
    /// The callback's typed values.
    type Elems<'a>;
    /// Optional batch constants in argument order.
    type ConstElems<'a>;
    /// Validated indexed source, borrowed from the decoded owners.
    type Source<'a>: IndexedSource<Item = Self::Elems<'a>>;
    /// Source without scalar branches, used by the dense retry collector.
    type RowsSource<'a>: IndexedSource<Item = Self::Elems<'a>>;
    /// Exact arity.
    const ARITY: usize;
    /// Every input permits dense reads of null payloads.
    const DENSE_SAFE: bool;
    /// Every input decoder is semantically infallible.
    const DECODE_INFALLIBLE: bool;
    /// Validate each input's semantic contract.
    fn validate(types: &[H::NativeType]) -> HostResult<H, ()>;
    /// Decode each argument once. Null-tolerant decode must first be accepted by `can_decode`.
    fn decode(inputs: &[Operand<H>], tolerant: bool, ctx: &mut H::Context)
        -> HostResult<H, Self::Columns>;
    /// Determine whether original nullable inputs support valid-only access before decoding any.
    fn can_decode(inputs: &[Operand<H>]) -> HostResult<H, bool>;
    /// Validate the exact retained views and construct an indexed source.
    fn source(columns: &Self::Columns, rows: usize) -> HostResult<H, Self::Source<'_>>;
    /// Borrow and validate full-length views only when every argument is an array.
    ///
    /// Keep this validation in the non-constant retry branch. A shared length proof can obscure
    /// constant specialization in the multi-CGU, no-LTO collector.
    fn rows_source(columns: &Self::Columns, rows: usize)
        -> HostResult<H, Option<Self::RowsSource<'_>>>;
    /// Borrow current batch constants for preparation.
    fn constants(columns: &Self::Columns) -> Self::ConstElems<'_>;
}

/// Retained input storage, classified once at the batch boundary.
pub struct Column<H: InputBinding<K>, K: RowKind> {
    decoded: H::Decoded,
    scalar: bool,
}

impl<H: InputBinding<K>, K: RowKind> Column<H, K> {
    fn decode(input: &Operand<H>, tolerant: bool, ctx: &mut H::Context) -> HostResult<H, Self> {
        // Selected traversal has already excluded a null scalar. Its ordinary decoder can retain
        // the constant without requiring null-tolerant array decoding support.
        let decoded = if tolerant && !input.scalar {
            H::decode_null_tolerant(&input.column, input.scalar, ctx)?
        } else {
            H::decode(&input.column, input.scalar, ctx)?
        };
        if input.scalar && H::view(&decoded).len() != 1 {
            return Err(H::error("decoded scalar must contain exactly one row"));
        }

        Ok(Self { decoded, scalar: input.scalar })
    }

    fn constant(&self) -> Option<K::Value<'_>> {
        if !self.scalar { return None; }
        let view = H::view(&self.decoded);
        assert_eq!(view.len(), 1, "decoded scalar view must retain its length");
        // SAFETY: this retained view has one row, as checked above.
        Some(unsafe { view.get_unchecked(0) })
    }
}

/// Per-argument constant specialization, kept visible to loop unswitching.
pub struct Source<'a, H: InputBinding<K>, K: RowKind>(SourceKind<'a, H, K>);

enum SourceKind<'a, H: InputBinding<K>, K: RowKind> {
    Rows(H::View<'a>),
    Scalar(H::View<'a>, usize),
}

impl<'a, H: InputBinding<K>, K: RowKind> Source<'a, H, K> {
    fn new(column: &'a Column<H, K>, rows: usize) -> HostResult<H, Self> {
        let view = H::view(&column.decoded);
        if column.scalar {
            if view.len() != 1 { return Err(H::error("scalar view must retain exactly one row")); }
            return Ok(Self(SourceKind::Scalar(view, rows)));
        }
        if view.len() != rows { return Err(H::error("decoded input must address the logical row count")); }

        Ok(Self(SourceKind::Rows(view)))
    }
}

// SAFETY: validated RowView instances retain their index domain for the entire borrow.
unsafe impl<'a, H: InputBinding<K>, K: RowKind> IndexedSource for Source<'a, H, K> {
    type Item = K::Value<'a>;
    const PREFER_LINEAR: bool = H::PREFER_LINEAR_OUTPUT;
    fn len(&self) -> usize {
        match &self.0 { SourceKind::Rows(view) => view.len(), SourceKind::Scalar(_, rows) => *rows }
    }
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        match &self.0 {
            // SAFETY: the caller bounds the index by the length validated in `new`.
            SourceKind::Rows(view) => unsafe { view.get_unchecked(index) },
            // SAFETY: `new` validated the scalar view has exactly one row.
            SourceKind::Scalar(view, _) => unsafe { view.get_unchecked(0) },
        }
    }
}

/// A full-length view with no per-row scalar branch.
pub struct ViewSource<'a, H: InputBinding<K>, K: RowKind>(H::View<'a>);

impl<'a, H: InputBinding<K>, K: RowKind> ViewSource<'a, H, K> {
    fn new(column: &'a Column<H, K>, rows: usize) -> HostResult<H, Self> {
        let view = H::view(&column.decoded);
        if view.len() != rows {
            return Err(H::error("decoded array must address the logical row count"));
        }
        Ok(Self(view))
    }
}

// SAFETY: the unsafe RowView contract preserves this validated view's length and initialized values.
unsafe impl<'a, H: InputBinding<K>, K: RowKind> IndexedSource for ViewSource<'a, H, K> {
    type Item = K::Value<'a>;
    const PREFER_LINEAR: bool = H::PREFER_LINEAR_OUTPUT;
    fn len(&self) -> usize { self.0.len() }
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        // SAFETY: the constructor validated this exact view against the traversal length.
        unsafe { self.0.get_unchecked(index) }
    }
}

/// Indexed traversal of validated argument sources.
pub struct TupleSource<T> {
    sources: T,
    rows: usize,
}

// SAFETY: the private row count is immutable, and unit reads do not address storage.
unsafe impl IndexedSource for TupleSource<()> {
    type Item = ();
    fn len(&self) -> usize { self.rows }
    unsafe fn get_unchecked(&self, _index: usize) {}
}

impl private::Sealed for () {}
impl<H: Host> InputTuple<H> for () {
    type Columns = ();
    type Elems<'a> = ();
    type ConstElems<'a> = ();
    type Source<'a> = TupleSource<()>;
    type RowsSource<'a> = TupleSource<()>;
    const ARITY: usize = 0;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(types: &[H::NativeType]) -> HostResult<H, ()> {
        if !types.is_empty() { return Err(H::error("nullary function requires no inputs")); }
        Ok(())
    }
    fn decode(_inputs: &[Operand<H>], _tolerant: bool, _ctx: &mut H::Context) -> HostResult<H, ()> { Ok(()) }
    fn can_decode(_inputs: &[Operand<H>]) -> HostResult<H, bool> { Ok(true) }
    fn source(_columns: &(), rows: usize) -> HostResult<H, Self::Source<'_>> {
        Ok(TupleSource { sources: (), rows })
    }
    fn rows_source(_columns: &(), rows: usize) -> HostResult<H, Option<Self::RowsSource<'_>>> {
        Ok(Some(TupleSource { sources: (), rows }))
    }
    fn constants(_columns: &()) {}
}

macro_rules! tuple {
    ($arity:literal; $($kind:ident: $index:tt),+) => {
        impl<$($kind: RowKind),+> private::Sealed for ($($kind,)+) {}
        impl<HostType, $($kind: RowKind),+> InputTuple<HostType> for ($($kind,)+)
        where HostType: Host $(+ InputBinding<$kind>)+ {
            type Columns = ($(Column<HostType, $kind>,)+);
            type Elems<'a> = ($($kind::Value<'a>,)+);
            type ConstElems<'a> = ($(Option<$kind::Value<'a>>,)+);
            type Source<'a> = TupleSource<($(Source<'a, HostType, $kind>,)+)>;
            type RowsSource<'a> = TupleSource<($(ViewSource<'a, HostType, $kind>,)+)>;
            const ARITY: usize = $arity;
            const DENSE_SAFE: bool = $(<HostType as InputBinding<$kind>>::DENSE_SAFE &&)+ true;
            const DECODE_INFALLIBLE: bool = $(<HostType as InputBinding<$kind>>::DECODE_INFALLIBLE &&)+ true;
            fn validate(types: &[HostType::NativeType]) -> HostResult<HostType, ()> {
                if types.len() != $arity { return Err(HostType::error("row signature arity mismatch")); }
                $(<HostType as InputBinding<$kind>>::validate(&types[$index])?;)+
                Ok(())
            }
            fn decode(inputs: &[Operand<HostType>], tolerant: bool, ctx: &mut HostType::Context)
                -> HostResult<HostType, Self::Columns> {
                Ok(($(Column::<HostType, $kind>::decode(&inputs[$index], tolerant, ctx)?,)+))
            }
            fn can_decode(inputs: &[Operand<HostType>]) -> HostResult<HostType, bool> {
                Ok($((inputs[$index].scalar || <HostType as InputBinding<$kind>>::can_decode_null_tolerant(&inputs[$index].column)?) &&)+ true)
            }
            fn source(columns: &Self::Columns, rows: usize) -> HostResult<HostType, Self::Source<'_>> {
                Ok(TupleSource { sources: ($(Source::new(&columns.$index, rows)?,)+), rows })
            }
            fn rows_source(columns: &Self::Columns, rows: usize)
                -> HostResult<HostType, Option<Self::RowsSource<'_>>> {
                if $(columns.$index.scalar ||)+ false { return Ok(None); }
                Ok(Some(TupleSource {
                    sources: ($(ViewSource::new(&columns.$index, rows)?,)+),
                    rows,
                }))
            }
            fn constants(columns: &Self::Columns) -> Self::ConstElems<'_> {
                ($(columns.$index.constant(),)+)
            }
        }
        // SAFETY: tuple construction validates every stable source against the private row count.
        unsafe impl<$($kind: IndexedSource),+> IndexedSource for TupleSource<($($kind,)+)> {
            type Item = ($($kind::Item,)+);
            const PREFER_LINEAR: bool = $($kind::PREFER_LINEAR &&)+ true;
            fn len(&self) -> usize { self.rows }
            unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
                // SAFETY: every retained source was validated against this tuple's row count.
                ($(unsafe { self.sources.$index.get_unchecked(index) },)+)
            }
        }
    };
}

tuple!(1; A: 0);
tuple!(2; A: 0, B: 1);
tuple!(3; A: 0, B: 1, C: 2);
tuple!(4; A: 0, B: 1, C: 2, D: 3);
tuple!(5; A: 0, B: 1, C: 2, D: 3, E: 4);
tuple!(6; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5);
tuple!(7; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6);
tuple!(8; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7);
tuple!(9; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8);
tuple!(10; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9);
tuple!(11; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10);
tuple!(12; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11);

mod private { pub trait Sealed {} }
