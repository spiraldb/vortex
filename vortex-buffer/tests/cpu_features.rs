// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Guard for the CPU feature test matrix in CI.
//!
//! Each matrix entry runs the test suite on an emulated CPU and sets
//! `VORTEX_EXPECT_CPU_FEATURES` to a comma-separated list such as `avx2,bmi2,!avx512f`.
//! A plain name must be detected at runtime and a `!`-prefixed name must not be, so an
//! emulator or runner that silently exposes a different CPU fails the job instead of
//! testing the wrong kernels. Without the variable this test does nothing.

#![expect(clippy::tests_outside_test_module)]

#[cfg(target_arch = "aarch64")]
use std::arch::is_aarch64_feature_detected as detected;
#[cfg(target_arch = "x86_64")]
use std::arch::is_x86_feature_detected as detected;
use std::env;
use std::env::consts::ARCH;

const EXPECT_VAR: &str = "VORTEX_EXPECT_CPU_FEATURES";

#[cfg(target_arch = "x86_64")]
fn is_detected(feature: &str) -> Option<bool> {
    Some(match feature {
        "sse2" => detected!("sse2"),
        "popcnt" => detected!("popcnt"),
        "bmi1" => detected!("bmi1"),
        "bmi2" => detected!("bmi2"),
        "avx" => detected!("avx"),
        "avx2" => detected!("avx2"),
        "avx512f" => detected!("avx512f"),
        "avx512bw" => detected!("avx512bw"),
        "avx512vbmi2" => detected!("avx512vbmi2"),
        "avx512vpopcntdq" => detected!("avx512vpopcntdq"),
        _ => return None,
    })
}

#[cfg(target_arch = "aarch64")]
fn is_detected(feature: &str) -> Option<bool> {
    Some(match feature {
        "neon" => detected!("neon"),
        "sve" => detected!("sve"),
        "sve2" => detected!("sve2"),
        "sve2-bitperm" => detected!("sve2-bitperm"),
        _ => return None,
    })
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn is_detected(_feature: &str) -> Option<bool> {
    None
}

#[test]
fn expected_cpu_features_are_detected() {
    let Ok(expected) = env::var(EXPECT_VAR) else {
        return;
    };

    let mut failures = Vec::new();
    for entry in expected.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (want, feature) = match entry.strip_prefix('!') {
            Some(feature) => (false, feature),
            None => (true, entry),
        };
        match is_detected(feature) {
            Some(got) if got == want => {}
            Some(got) => failures.push(format!("{feature}: expected {want}, detected {got}")),
            None => failures.push(format!("{feature}: unknown feature for {ARCH}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{EXPECT_VAR}={expected} does not match this CPU:\n{}",
        failures.join("\n")
    );
}
