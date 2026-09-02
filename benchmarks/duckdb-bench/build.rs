// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::expect_used)]

/// Adds a dynamic linker runtime path pointing to the DuckDB dylib dir.
///
/// Setting an absolute rpath, if required by multiple binaries, is the most
/// robust solution compared to using relative paths in terms of
/// `-rpath,$ORIGIN` or `-rpath,@executable_path`.
///
/// Using an absolute rpath implies that binaries linking against the dynamic
/// DuckDB library are never published.
///
/// Note that the rpath set in vortex-duckdb's build.rs is not inherited by
/// crates linking against it which is why consumers must set a rpath on their end.
///
/// The dynamic DuckDB library is preferred over the static version, as DuckDB's
/// static lib is not self-contained. This means that it includes symbols which
/// are not defined as part of the static library.
fn main() {
    // The extension is linked into this binary, so it must share vortex-duckdb's DuckDB library.
    // DUCKDB_LIB_DIR is only a fallback for builds without that dependency information.
    println!("cargo:rerun-if-env-changed=DUCKDB_LIB_DIR");
    let duckdb_lib = std::env::var("DEP_DUCKDB_LIB_DIR")
        .or_else(|_| std::env::var("DUCKDB_LIB_DIR"))
        .expect("DEP_DUCKDB_LIB_DIR or DUCKDB_LIB_DIR must point at the DuckDB library directory");
    println!("cargo:rustc-link-search=native={duckdb_lib}");
    println!("cargo:rustc-link-lib=dylib=duckdb");

    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{duckdb_lib}");
    }

    #[cfg(not(target_os = "macos"))]
    {
        // Inline the dynamically exported symbol list for non-macos targets.
        const DLIST: &str = "{\
            custom_labels_abi_version;\
            custom_labels_current_set;\
            };";

        let dlist_path = format!("{}/dlist", std::env::var("OUT_DIR").unwrap());
        std::fs::write(&dlist_path, DLIST).unwrap();
        println!("cargo:rustc-link-arg=-Wl,-rpath,{duckdb_lib},--dynamic-list={dlist_path}")
    }
}
