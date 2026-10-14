// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Computing array statistics through the registered aggregate functions.

use enum_iterator::all;
use itertools::Itertools;
use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::expr::stats::Stat;
use vortex_array::expr::stats::StatsProviderExt;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;

#[test]
fn test_uncompressed_size_in_bytes() -> VortexResult<()> {
    let struct_array = StructArray::new(
        FieldNames::from(["integers"]),
        vec![ConstantArray::new(5, 1000).into_array()],
        1000,
        Validity::NonNullable,
    );

    let canonical_size = struct_array
        .clone()
        .into_array()
        .execute::<Canonical>(&mut crate::session().create_execution_ctx())?
        .into_array()
        .nbytes();
    let uncompressed_size = struct_array
        .statistics()
        .compute_uncompressed_size_in_bytes(&mut crate::session().create_execution_ctx());

    assert_eq!(canonical_size, 2);
    assert_eq!(uncompressed_size, Some(4000));
    Ok(())
}

#[test]
fn merge_unordered() {
    let array =
        PrimitiveArray::from_option_iter([Some(1), None, Some(2), Some(42), Some(10000), None]);
    let all_stats = all::<Stat>()
        .filter(|s| !matches!(s, Stat::Sum))
        .filter(|s| !matches!(s, Stat::NaNCount))
        .collect_vec();
    array
        .statistics()
        .compute_all(&all_stats, &mut crate::session().create_execution_ctx())
        .unwrap();

    let stats = array.statistics().to_owned();
    for stat in &all_stats {
        assert!(!stats.get(*stat).is_absent(), "Stat {stat} is missing");
    }

    let merged = stats.clone().merge_unordered(
        &stats,
        &DType::Primitive(PType::I32, Nullability::NonNullable),
    );
    for stat in &all_stats {
        assert_eq!(
            !merged.get(*stat).is_absent(),
            stat.is_commutative(),
            "Stat {stat} remains after merge_unordered despite not being commutative, or was removed despite being commutative"
        )
    }

    let merged_ref = merged.as_typed_ref(&DType::Primitive(PType::I32, Nullability::Nullable));
    let stats_ref = stats.as_typed_ref(&DType::Primitive(PType::I32, Nullability::Nullable));

    assert_eq!(
        merged_ref.get_as::<i32>(Stat::Min),
        stats_ref.get_as::<i32>(Stat::Min)
    );
    assert_eq!(
        merged_ref.get_as::<i32>(Stat::Max),
        stats_ref.get_as::<i32>(Stat::Max)
    );
    assert_eq!(
        merged_ref.get_as::<u64>(Stat::NullCount),
        stats_ref.get_as::<u64>(Stat::NullCount).map(|s| s * 2)
    );
}
