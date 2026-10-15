// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#pragma once

#include <stdint.h>

/// Comparison opcodes shared by CUDA kernels and the Rust launch code via bindgen.
enum CompareOp : uint32_t {
    COMPARE_EQ = 0,
    COMPARE_NOT_EQ = 1,
    COMPARE_GT = 2,
    COMPARE_GTE = 3,
    COMPARE_LT = 4,
    COMPARE_LTE = 5,
};
