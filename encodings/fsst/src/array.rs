use std::fmt::Display;
use std::sync::Arc;

use fsst::{Decompressor, Symbol};
use serde::{Deserialize, Serialize};
use vortex::array::VarBinArray;
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity};
use vortex::variants::{ArrayVariants, BinaryArrayTrait, Utf8ArrayTrait};
use vortex::visitor::AcceptArrayVisitor;
use vortex::{impl_encoding, Array, ArrayDType, ArrayTrait, IntoCanonical};
use vortex_dtype::{DType, Nullability, PType};
use vortex_error::{vortex_bail, VortexExpect, VortexResult};

impl_encoding!("vortex.fsst", ids::FSST, FSST);

static SYMBOLS_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);
static SYMBOL_LENS_DTYPE: DType = DType::Primitive(PType::U8, Nullability::NonNullable);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FSSTMetadata {
    symbols_len: usize,
    codes_dtype: DType,
    uncompressed_lengths_dtype: DType,
}

impl Display for FSSTMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FSSTArray {
    /// Build an FSST array from a set of `symbols` and `codes`.
    ///
    /// Symbols are 8-bytes and can represent short strings, each of which is assigned
    /// a code.
    ///
    /// The `codes` array is a Binary array where each binary datum is a sequence of 8-bit codes.
    /// Each code corresponds either to a symbol, or to the "escape code",
    /// which tells the decoder to emit the following byte without doing a table lookup.
    pub fn try_new(
        dtype: DType,
        symbols: Array,
        symbol_lengths: Array,
        codes: Array,
        uncompressed_lengths: Array,
    ) -> VortexResult<Self> {
        // Check: symbols must be a u64 array
        if symbols.dtype() != &SYMBOLS_DTYPE {
            vortex_bail!(InvalidArgument: "symbols array must be of type u64")
        }

        if symbol_lengths.dtype() != &SYMBOL_LENS_DTYPE {
            vortex_bail!(InvalidArgument: "symbol_lengths array must be of type u8")
        }

        // Check: symbols must not have length > MAX_CODE
        if symbols.len() > 255 {
            vortex_bail!(InvalidArgument: "symbols array must have length <= 255");
        }

        if symbols.len() != symbol_lengths.len() {
            vortex_bail!(InvalidArgument: "symbols and symbol_lengths arrays must have same length");
        }

        if uncompressed_lengths.len() != codes.len() {
            vortex_bail!(InvalidArgument: "uncompressed_lengths must be same len as codes");
        }

        if !uncompressed_lengths.dtype().is_int() {
            vortex_bail!(InvalidArgument: "uncompressed_lengths must have integer type");
        }

        // Check: strings must be a Binary array.
        if !matches!(codes.dtype(), DType::Binary(_)) {
            vortex_bail!(InvalidArgument: "codes array must be DType::Binary type");
        }

        let symbols_len = symbols.len();
        let len = codes.len();
        let strings_dtype = codes.dtype().clone();
        let uncompressed_lengths_dtype = uncompressed_lengths.dtype().clone();
        let children = Arc::new([symbols, symbol_lengths, codes, uncompressed_lengths]);

        Self::try_from_parts(
            dtype,
            len,
            FSSTMetadata {
                symbols_len,
                codes_dtype: strings_dtype,
                uncompressed_lengths_dtype,
            },
            children,
            StatsSet::new(),
        )
    }

    /// Access the symbol table array
    pub fn symbols(&self) -> Array {
        self.as_ref()
            .child(0, &SYMBOLS_DTYPE, self.metadata().symbols_len)
            .vortex_expect("FSSTArray symbols child")
    }

    /// Access the symbol table array
    pub fn symbol_lengths(&self) -> Array {
        self.as_ref()
            .child(1, &SYMBOL_LENS_DTYPE, self.metadata().symbols_len)
            .vortex_expect("FSSTArray symbol_lengths child")
    }

    /// Access the codes array
    pub fn codes(&self) -> Array {
        self.as_ref()
            .child(2, &self.metadata().codes_dtype, self.len())
            .vortex_expect("FSSTArray codes child")
    }

    /// Get the uncompressed length for each element in the array.
    pub fn uncompressed_lengths(&self) -> Array {
        self.as_ref()
            .child(3, &self.metadata().uncompressed_lengths_dtype, self.len())
            .vortex_expect("FSST uncompressed_lengths child")
    }

    /// Get the validity for this array.
    pub fn validity(&self) -> Validity {
        VarBinArray::try_from(self.codes())
            .vortex_expect("FSSTArray must have a codes child array")
            .validity()
    }

    /// Build a [`Decompressor`][fsst::Decompressor] that can be used to decompress values from
    /// this array, and pass it to the given function.
    ///
    /// This is private to the crate to avoid leaking `fsst-rs` types as part of the public API.
    pub(crate) fn with_decompressor<F, R>(&self, apply: F) -> VortexResult<R>
    where
        F: FnOnce(Decompressor) -> VortexResult<R>,
    {
        // canonicalize the symbols child array, so we can view it contiguously
        let symbols_array = self
            .symbols()
            .into_canonical()
            .map_err(|err| err.with_context("Failed to canonicalize symbols array"))?
            .into_primitive()
            .map_err(|err| err.with_context("Symbols must be a Primitive Array"))?;
        let symbols = symbols_array.maybe_null_slice::<u64>();

        let symbol_lengths_array = self
            .symbol_lengths()
            .into_canonical()
            .map_err(|err| err.with_context("Failed to canonicalize symbol_lengths array"))?
            .into_primitive()
            .map_err(|err| err.with_context("Symbol lengths must be a Primitive Array"))?;
        let symbol_lengths = symbol_lengths_array.maybe_null_slice::<u8>();

        // Transmute the 64-bit symbol values into fsst `Symbol`s.
        // SAFETY: Symbol is guaranteed to be 8 bytes, guaranteed by the compiler.
        let symbols = unsafe { std::mem::transmute::<&[u64], &[Symbol]>(symbols) };

        // Build a new decompressor that uses these symbols.
        let decompressor = Decompressor::new(symbols, symbol_lengths);
        apply(decompressor)
    }
}

impl AcceptArrayVisitor for FSSTArray {
    fn accept(&self, visitor: &mut dyn vortex::visitor::ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("symbols", &self.symbols())?;
        visitor.visit_child("symbol_lengths", &self.symbol_lengths())?;
        visitor.visit_child("codes", &self.codes())?;
        visitor.visit_child("uncompressed_lengths", &self.uncompressed_lengths())
    }
}

impl ArrayStatisticsCompute for FSSTArray {}

impl ArrayValidity for FSSTArray {
    fn is_valid(&self, index: usize) -> bool {
        self.codes().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.codes().with_dyn(|a| a.logical_validity())
    }
}

impl ArrayVariants for FSSTArray {
    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        Some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        Some(self)
    }
}

impl Utf8ArrayTrait for FSSTArray {}

impl BinaryArrayTrait for FSSTArray {}

impl ArrayTrait for FSSTArray {}
