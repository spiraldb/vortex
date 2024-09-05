// How many do we need

use std::env;
use std::path::Path;

fn main() {
    // Link against the C API library generated by the parent.
    // println!("cargo:rustc-link-lib=vortex_capi");
    println!("cargo:rerun-if-changed=build.rs");

    let root = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap());
    println!("cargo:warning=root is {root}");

    cc::Build::new()
        .file("simple.c")
        .include(root)
        .compile("simple");
}
