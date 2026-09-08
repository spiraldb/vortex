// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "fixture.h"
extern "C" int native_cpp(int x) {
    // Intentional warning: Cargo dependencies must not inherit the parent's -Werror.
    int vendored_unused;
    return x + REQUIRED_CXX + PARENT_VALUE + CONFIG_VALUE + HEADER_VALUE;
}
