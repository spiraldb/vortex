// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! `is_constant` over struct arrays.

use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::StructArray;
use vortex_error::VortexExpect;

use crate::is_constant::is_constant;
use crate::test_session;

#[test]
fn test_empty_struct_is_constant() {
    let array = StructArray::new_fieldless_with_len(2);
    let mut ctx = test_session().create_execution_ctx();
    let result = is_constant(&array.into_array(), &mut ctx)
        .vortex_expect("operation should succeed in test");
    assert!(result);
}
