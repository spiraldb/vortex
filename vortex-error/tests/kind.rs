// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

// This seems to be a bug in the lint - https://github.com/rust-lang/rust-clippy/issues/11024
#![expect(clippy::tests_outside_test_module)]

use std::error::Error;
use std::io;
use std::sync::Arc;

use vortex_error::VortexError;
use vortex_error::VortexErrorKind;
use vortex_error::vortex_err;

#[test]
fn test_untagged_err_is_other() {
    assert_eq!(vortex_err!("boom").kind(), VortexErrorKind::Other);
}

#[test]
fn test_tagged_err_carries_kind() {
    assert_eq!(
        vortex_err!(InvalidArgument: "bad {}", 1).kind(),
        VortexErrorKind::InvalidArgument
    );
    assert_eq!(
        vortex_err!(OutOfBounds: 5usize, 0usize, 3usize).kind(),
        VortexErrorKind::OutOfBounds
    );
    assert_eq!(
        vortex_err!(MismatchedTypes: "i32", "u8").kind(),
        VortexErrorKind::MismatchedTypes
    );
    assert_eq!(
        vortex_err!(NotImplemented: "take", "SparseArray").kind(),
        VortexErrorKind::NotImplemented
    );
}

#[test]
fn test_structured_messages() {
    assert!(
        vortex_err!(OutOfBounds: 5usize, 0usize, 3usize)
            .to_string()
            .contains("index 5 out of bounds from 0 to 3")
    );
    assert!(
        vortex_err!(MismatchedTypes: "i32", "u8")
            .to_string()
            .contains("expected type: i32 but instead got u8")
    );
    assert!(
        vortex_err!(NotImplemented: "take", "SparseArray")
            .to_string()
            .contains("function take not implemented for SparseArray")
    );
}

#[test]
fn test_with_context_preserves_kind_and_source() {
    let err = VortexError::from(io::Error::other("disk on fire")).with_context("while reading");

    assert_eq!(err.kind(), VortexErrorKind::Io);
    assert!(
        err.source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some(),
        "context must not hide the underlying error"
    );

    let display = err.to_string();
    assert!(
        display.starts_with("IO error: while reading:\n  disk on fire"),
        "{display}"
    );
}

#[test]
fn test_nested_context() {
    let err = vortex_err!(Serde: "bad footer")
        .with_context("reading layout")
        .with_context("opening file");

    assert_eq!(err.kind(), VortexErrorKind::Serde);
    let display = err.to_string();
    assert!(
        display.starts_with("Serde error: opening file:\n  reading layout:\n  bad footer"),
        "{display}"
    );
}

#[test]
fn test_external_preserves_concrete_source() {
    let err = vortex_err!(External: io::Error::from(io::ErrorKind::NotFound));

    assert_eq!(err.kind(), VortexErrorKind::Other);
    assert_eq!(
        err.source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .map(io::Error::kind),
        Some(io::ErrorKind::NotFound)
    );
}

#[test]
fn test_clone_and_shared_round_trip() {
    let err = vortex_err!(Compute: "kernel exploded");
    let clone = err.clone();

    assert_eq!(clone.kind(), err.kind());
    assert_eq!(clone.to_string(), err.to_string());

    // An `Arc` is unwrapped when uniquely held and cloned otherwise; both keep the classification.
    let shared = Arc::new(err);
    let borrowed = VortexError::from(&shared);
    let owned = VortexError::from(shared);

    assert_eq!(borrowed.kind(), VortexErrorKind::Compute);
    assert_eq!(owned.kind(), VortexErrorKind::Compute);
    assert_eq!(borrowed.to_string(), owned.to_string());
}
