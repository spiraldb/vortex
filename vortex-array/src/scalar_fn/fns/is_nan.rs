// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_buffer::BitBuffer;
use vortex_buffer::BufferAllocatorRef;
use vortex_buffer::BufferMut;
use vortex_compute::lane_kernels::IndexedSourceExt;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::arrays::BoolArray;
use crate::arrays::ConstantArray;
use crate::arrays::PrimitiveArray;
use crate::arrays::ScalarFnArray;
use crate::dtype::DType;
use crate::dtype::NativePType;
use crate::dtype::Nullability;
use crate::match_each_float_ptype;
use crate::scalar::Scalar;
use crate::scalar_fn::Arity;
use crate::scalar_fn::ChildName;
use crate::scalar_fn::EmptyOptions;
use crate::scalar_fn::ExecutionArgs;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::ScalarFnVTable;
use crate::scalar_fn::ScalarFnVTableExt;

/// Expression that checks for IEEE 754 NaN values.
///
/// Vortex orders and compares floats totally: `NaN` equals itself and sorts above every other
/// value. That makes NaN reachable by `min`/`max` and by equality, but it also means no
/// combination of comparison operators isolates it — `x = x` holds for every non-null `x`
/// including NaN, and `x != x` holds for none. This function is the only way to ask the question.
///
/// It is strict, so a null row yields null rather than false. `NOT NaN` is therefore
/// `not(is_nan(x))`, which is null for a null row and filters it out.
#[derive(Clone)]
pub struct IsNan;

impl IsNan {
    /// Creates a lazy NaN check over `input`.
    ///
    /// # Errors
    ///
    /// Returns an error if `input` is not a float array.
    pub fn try_new(input: ArrayRef) -> VortexResult<ScalarFnArray> {
        ScalarFnArray::try_new(IsNan.bind(EmptyOptions), vec![input])
    }
}

impl ScalarFnVTable for IsNan {
    type Options = EmptyOptions;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.is_nan");
        *ID
    }

    fn serialize(&self, _instance: &Self::Options) -> VortexResult<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }

    fn deserialize(
        &self,
        _metadata: &[u8],
        _session: &VortexSession,
    ) -> VortexResult<Self::Options> {
        Ok(EmptyOptions)
    }

    fn arity(&self, _options: &Self::Options) -> Arity {
        Arity::Exact(1)
    }

    fn child_name(&self, _instance: &Self::Options, child_idx: usize) -> ChildName {
        match child_idx {
            0 => ChildName::from("input"),
            _ => unreachable!("Invalid child index {} for IsNan expression", child_idx),
        }
    }

    fn return_dtype(&self, _options: &Self::Options, arg_dtypes: &[DType]) -> VortexResult<DType> {
        let input = &arg_dtypes[0];
        if !input.is_float() {
            vortex_bail!("is_nan requires a float argument, got {input}");
        }
        Ok(DType::Bool(input.nullability()))
    }

    fn execute(
        &self,
        _options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let child = args.get(0)?;
        let nullability = child.dtype().nullability();

        if let Some(scalar) = child.as_constant() {
            let result = match scalar.as_primitive().pvalue() {
                None => Scalar::null(DType::Bool(Nullability::Nullable)),
                Some(value) => Scalar::bool(value.is_nan(), nullability),
            };
            return Ok(ConstantArray::new(result, args.row_count()).into_array());
        }

        let primitive = child.execute::<PrimitiveArray>(ctx)?;
        // Strict, so the output validity is the input's: a null row stays null and its NaN bit is
        // never read.
        let validity = primitive.as_ref().validity()?;
        let bits = match_each_float_ptype!(primitive.ptype(), |F| {
            collect_nan_bits(primitive.as_slice::<F>(), ctx.allocator())
        });
        Ok(BoolArray::new(bits, validity).into_array())
    }

    fn is_strict(&self, _instance: &Self::Options) -> bool {
        true
    }

    fn is_infallible(&self, _instance: &Self::Options) -> bool {
        true
    }
}

/// Bit-pack `is_nan` over a float slice.
fn collect_nan_bits<T: NativePType>(values: &[T], allocator: &BufferAllocatorRef) -> BitBuffer {
    let len = values.len();
    let mut words = BufferMut::<u64>::zeroed_in(len.div_ceil(64), allocator.clone());
    values.map_bits_into(words.as_mut_slice(), |value| value.is_nan());
    let mut bytes = words.into_byte_buffer();
    bytes.truncate(len.div_ceil(8));
    BitBuffer::new(bytes.freeze(), len)
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::BoolArray;
    use crate::arrays::PrimitiveArray;
    use crate::assert_arrays_eq;
    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::dtype::StructFields;
    use crate::expr::col;
    use crate::expr::eq;
    use crate::expr::is_nan;
    use crate::expr::lit;
    use crate::expr::not;
    use crate::expr::or;
    use crate::expr::root;
    use crate::stats::StatsSession;
    use crate::stats::all_non_nan;
    use crate::stats::nan_count;
    use crate::validity::Validity;

    static STATS_SESSION: LazyLock<VortexSession> =
        LazyLock::new(|| VortexSession::empty().with::<StatsSession>());

    /// A one-field struct `{a: <ptype><nullability>}` to bind column expressions against.
    fn struct_dtype(ptype: PType, nullability: Nullability) -> DType {
        DType::Struct(
            StructFields::new(["a"].into(), vec![DType::Primitive(ptype, nullability)]),
            Nullability::NonNullable,
        )
    }

    fn float_struct_dtype() -> DType {
        struct_dtype(PType::F64, Nullability::Nullable)
    }

    #[test]
    fn dtype_follows_input_nullability() -> VortexResult<()> {
        assert_eq!(
            is_nan(col("a")).return_dtype(&float_struct_dtype())?,
            DType::Bool(Nullability::Nullable)
        );

        let non_nullable = struct_dtype(PType::F32, Nullability::NonNullable);
        assert_eq!(
            is_nan(col("a")).return_dtype(&non_nullable)?,
            DType::Bool(Nullability::NonNullable)
        );
        Ok(())
    }

    #[test]
    fn rejects_a_non_float_argument() {
        let dtype = struct_dtype(PType::I64, Nullability::Nullable);
        let error = is_nan(col("a")).return_dtype(&dtype).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("is_nan requires a float argument"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn isolates_nan_where_equality_cannot() -> VortexResult<()> {
        // The reason this function exists: with Vortex's total float ordering `x = x` is true for
        // every non-null row, NaN included, and `x != x` is true for none.
        let values = PrimitiveArray::new(
            buffer![1.0f64, f64::NAN, -f64::NAN, 0.0, f64::INFINITY],
            Validity::NonNullable,
        )
        .into_array();
        let mut ctx = array_session().create_execution_ctx();

        assert_arrays_eq!(
            values.clone().apply(&is_nan(root()))?,
            BoolArray::from_iter([false, true, true, false, false]),
            &mut ctx
        );
        assert_arrays_eq!(
            values.apply(&eq(root(), root()))?,
            BoolArray::from_iter([true, true, true, true, true]),
            &mut ctx
        );
        Ok(())
    }

    #[test]
    fn a_null_row_is_null_not_false() -> VortexResult<()> {
        let values =
            PrimitiveArray::from_option_iter([Some(f32::NAN), None, Some(1.0)]).into_array();
        let mut ctx = array_session().create_execution_ctx();

        assert_arrays_eq!(
            values.clone().apply(&is_nan(root()))?,
            BoolArray::from_iter([Some(true), None, Some(false)]),
            &mut ctx
        );
        // NOT NaN inherits the same null, so a null row survives neither predicate.
        assert_arrays_eq!(
            values.apply(&not(is_nan(root())))?,
            BoolArray::from_iter([Some(false), None, Some(true)]),
            &mut ctx
        );
        Ok(())
    }

    #[test]
    fn a_constant_input_folds() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let nan = PrimitiveArray::new(buffer![f64::NAN; 3], Validity::NonNullable).into_array();
        assert_arrays_eq!(
            nan.apply(&is_nan(root()))?,
            BoolArray::from_iter([true, true, true]),
            &mut ctx
        );
        Ok(())
    }

    #[test]
    fn display() {
        assert_eq!(is_nan(col("a")).to_string(), "vortex.is_nan($.a)");
    }

    #[test]
    fn falsifies_from_the_nan_stats() -> VortexResult<()> {
        // A zone with no NaN in it cannot satisfy `is_nan`, so the scan can skip it outright.
        let dtype = float_struct_dtype();
        assert_eq!(
            is_nan(col("a")).bind(&dtype)?.falsify(&STATS_SESSION)?,
            Some(or(eq(nan_count(col("a")), lit(0u64)), all_non_nan(col("a"))).bind(&dtype)?)
        );
        Ok(())
    }

    #[test]
    fn is_strict() {
        assert!(
            is_nan(col("a"))
                .as_scalar()
                .is_some_and(|f| f.signature().is_strict())
        );
    }
}
