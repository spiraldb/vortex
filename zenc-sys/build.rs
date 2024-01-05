use std::env;
use std::path::PathBuf;

fn main() {
    let buildrs_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize CARGO_MANIFEST_DIR");
    let root_dir = buildrs_dir
        .join("../../")
        .canonicalize()
        .expect("Failed to canonicalize root dir");
    let zig_out_lib_path = root_dir.join("zig-out/lib");
    let zenc_header = root_dir
        .join("zig/zenc.h")
        .canonicalize()
        .expect("Failed to canonicalize zenc.h path");

    // Tell cargo to tell rustc to link zenc
    println!(
        "cargo:rustc-link-search={}",
        zig_out_lib_path.to_str().unwrap()
    );
    println!("cargo:rustc-link-lib=zenc");

    // Tell cargo to invalidate the built crate whenever the buildscript or the zig wrappers change
    println!(
        "cargo:rerun-if-changed={}",
        buildrs_dir.join("build.rs").to_str().unwrap()
    );
    println!("cargo:rerun-if-changed={}", zenc_header.to_str().unwrap());
    println!(
        "cargo:rerun-if-changed={}",
        root_dir.join("zig/zenc.zig").to_str().unwrap()
    );

    if !std::process::Command::new("zig")
        .arg("build")
        .current_dir(root_dir.clone())
        .output()
        .expect("could not spawn `clang`")
        .status
        .success()
    {
        // Panic if the command was not successful.
        panic!(
            "failed to successfully invoke `zig build` in {}",
            root_dir.to_str().unwrap()
        );
    }

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header(zenc_header.to_str().unwrap())
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
