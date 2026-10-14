// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The uncompressed size of union arrays.

use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::ConstantArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::UnionVariants;
use vortex_array::scalar::Scalar;
use vortex_error::VortexResult;

use crate::test_session;
use crate::uncompressed_size_in_bytes::UncompressedSizeOpts;
use crate::uncompressed_size_in_bytes::uncompressed_size_in_bytes;

fn variants() -> VortexResult<UnionVariants> {
    UnionVariants::try_new(
        ["number", "flag"].into(),
        vec![
            DType::Primitive(PType::I32, Nullability::NonNullable),
            DType::Bool(Nullability::NonNullable),
        ],
        vec![5, 9],
    )
}

#[test]
fn constant_union_reports_uncompressed_size() -> VortexResult<()> {
    let mut ctx = test_session().create_execution_ctx();
    let scalar = Scalar::union(variants()?, 5, 10i32.into(), Nullability::NonNullable)?;

    // Four `u8` type IDs, four `i32` rows, and four `bool` placeholder bits rounded up to a byte.
    let size = uncompressed_size_in_bytes(
        &ConstantArray::new(scalar, 4).into_array(),
        UncompressedSizeOpts::exact(),
        &mut ctx,
    )?;

    assert_eq!(size, 4 + 4 * 4 + 1);

    Ok(())
}
