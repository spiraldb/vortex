// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

unsafe extern "C" {
    fn native_c(x: i32) -> i32;
    fn native_cpp(x: i32) -> i32;
}

/// Call both languages so a host build-script link cannot discard either object.
pub fn value() -> i32 {
    // SAFETY: The fixture's small input and macro values keep both C int additions in range.
    unsafe { native_c(5) + native_cpp(5) }
}
