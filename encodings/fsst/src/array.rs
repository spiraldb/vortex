use std::sync::Arc;

use fsst::{Decompressor, Symbol, MAX_CODE};
use serde::{Deserialize, Serialize};
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::variants::{ArrayVariants, BinaryArrayTrait, Utf8ArrayTrait};
use vortex::visitor::AcceptArrayVisitor;
use vortex::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, IntoCanonical};
use vortex_dtype::{DType, Nullability, PType};
use vortex_error::{vortex_bail, vortex_panic, VortexResult};

impl_encoding!("vortex.fsst", 24u16, FSST);

static SYMBOLS_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

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
    pub fn try_new(dtype: DType, symbols: Array, codes: Array) -> VortexResult<Self> {
        // Check: symbols must be a u64 array
        if symbols.dtype() != &DType::Primitive(PType::U64, Nullability::NonNullable) {
            vortex_bail!(InvalidArgument: "symbols array must be of type u64")
        }

        // Check: symbols must not have length > MAX_CODE
        if symbols.len() > MAX_CODE as usize {
            vortex_bail!(InvalidArgument: "symbols array must have length <= 255")
        }

        // Check: strings must be a Binary array.
        if !matches!(codes.dtype(), DType::Binary(_)) {
            vortex_bail!(InvalidArgument: "strings array must be DType::Binary type");
        }

        let symbols_len = symbols.len();
        let len = codes.len();
        let strings_dtype = codes.dtype().clone();
        let children = Arc::new([symbols, codes]);

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
            .unwrap_or_else(|| panic!("FSSTArray must have a symbols child array"))
    }

    /// Access the codes array
    pub fn codes(&self) -> Array {
        self.array()
            .child(1, &self.metadata().codes_dtype, self.len())
            .unwrap_or_else(|| panic!("FSSTArray must have a codes child array"))
    }

    /// Build a [`Decompressor`][fsst::Decompressor] that can be used to decompress values from
    /// this array.
    ///
    /// This is private to the crate to avoid leaking `fsst` as part of the public API.
    pub(crate) fn decompressor(&self) -> Decompressor {
        // canonicalize the symbols child array, so we can view it contiguously
        let symbols_array = self
            .symbols()
            .into_canonical()
            .unwrap_or_else(|err| vortex_panic!(err))
            .into_primitive()
            .unwrap_or_else(|err| vortex_panic!(Context: "Symbols must be a Primitive Array", err));
        let symbols = symbols_array.maybe_null_slice::<u64>();

        // Transmute the 64-bit symbol values into fsst `Symbol`s.
        // SAFETY: Symbol is guaranteed to be 8 bytes, guaranteed by the compiler.
        let symbols = unsafe { std::mem::transmute::<&[u64], &[Symbol]>(symbols) };

        // Build a new decompressor that uses these symbols.
        Decompressor::new(symbols)
    }
}

impl AcceptArrayVisitor for FSSTArray {
    fn accept(&self, _visitor: &mut dyn vortex::visitor::ArrayVisitor) -> VortexResult<()> {
        todo!("implement this")
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
