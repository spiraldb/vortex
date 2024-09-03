use std::sync::Arc;

use fsst::{Decompressor, Symbol};
use serde::{Deserialize, Serialize};
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, BinaryArrayTrait, Utf8ArrayTrait};
use vortex::visitor::AcceptArrayVisitor;
use vortex::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, IntoCanonical};
use vortex_dtype::{DType, Nullability, PType};
use vortex_error::{vortex_bail, VortexResult};

impl_encoding!("vortex.fsst", 24u16, FSST);

static SYMBOLS_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);
static SYMBOL_LENS_DTYPE: DType = DType::Primitive(PType::U8, Nullability::NonNullable);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FSSTMetadata {
    symbols_len: usize,
    codes_dtype: DType,
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

        // Check: strings must be a Binary array.
        if !matches!(codes.dtype(), DType::Binary(_)) {
            vortex_bail!(InvalidArgument: "codes array must be DType::Binary type");
        }

        let symbols_len = symbols.len();
        let len = codes.len();
        let strings_dtype = codes.dtype().clone();
        let children = Arc::new([symbols, symbol_lengths, codes]);

        Self::try_from_parts(
            dtype,
            len,
            FSSTMetadata {
                symbols_len,
                codes_dtype: strings_dtype,
            },
            children,
            StatsSet::new(),
        )
    }

    /// Access the symbol table array
    pub fn symbols(&self) -> Array {
        self.array()
            .child(0, &SYMBOLS_DTYPE, self.metadata().symbols_len)
            .expect("FSSTArray must have a symbols child array")
    }

    /// Access the symbol table array
    pub fn symbol_lengths(&self) -> Array {
        self.array()
            .child(1, &SYMBOL_LENS_DTYPE, self.metadata().symbols_len)
            .expect("FSSTArray must have a symbols child array")
    }

    /// Access the codes array
    pub fn codes(&self) -> Array {
        self.array()
            .child(2, &self.metadata().codes_dtype, self.len())
            .expect("FSSTArray must have a codes child array")
    }

    /// Build a [`Decompressor`][fsst::Decompressor] that can be used to decompress values from
    /// this array, and pass it to the given function.
    ///
    /// This is private to the crate to avoid leaking `fsst-rs` types as part of the public API.
    pub(crate) fn with_decompressor<F, R>(&self, apply: F) -> R
    where
        F: FnOnce(Decompressor) -> R,
    {
        // canonicalize the symbols child array, so we can view it contiguously
        let symbols_array = self
            .symbols()
            .into_canonical()
            .unwrap()
            .into_primitive()
            .expect("Symbols must be a Primitive Array");
        let symbols = symbols_array.maybe_null_slice::<u64>();

        let symbol_lengths_array = self
            .symbol_lengths()
            .into_canonical()
            .unwrap()
            .into_primitive()
            .unwrap();
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
        visitor.visit_child("codes", &self.codes())
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
    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        Some(self)
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        Some(self)
    }
}

impl Utf8ArrayTrait for FSSTArray {}

impl BinaryArrayTrait for FSSTArray {}

impl ArrayTrait for FSSTArray {}
