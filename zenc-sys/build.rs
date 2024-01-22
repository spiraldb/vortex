use std::env;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

fn main() {
    let buildrs_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize CARGO_MANIFEST_DIR");
    let root_dir = buildrs_dir
        .join("../../")
        .canonicalize()
        .expect("Failed to canonicalize root dir");
    let zenc_header = root_dir
        .join("zig/c-abi/wrapper.h")
        .canonicalize()
        .expect("Failed to canonicalize wrapper.h path");

    // Tell cargo to tell rustc to link zenc
    println!(
        "cargo:rustc-link-search={}",
        root_dir.join("zig-out/lib").to_str().unwrap()
    );
    println!("cargo:rustc-link-lib=zenc");

    rerun_if_changed(&buildrs_dir.join("build.rs"));
    for entry in WalkDir::new(root_dir.join("zig"))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            !e.path()
                .components()
                .any(|c| c.as_os_str() == "zig-cache" || c.as_os_str() == "_deprecated_libenc")
        })
        .filter(|e| {
            e.path()
                .extension()
                .map(|e| e == "zig" || e == "h" || e == "c")
                .unwrap_or(false)
        })
    {
        rerun_if_changed(entry.path());
    }

    let is_dev = env::var("PROFILE").unwrap() == "dev";
    let zig_opt = if is_dev {
        "-Doptimize=Debug"
    } else {
        "-Doptimize=ReleaseSafe"
    };
    if !std::process::Command::new("zig")
        .arg("build")
        .arg(zig_opt)
        .args(["--summary", "all"])
        .current_dir(root_dir.clone())
        .spawn()
        .expect("Could not invoke `zig build`")
        .wait()
        .unwrap()
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

fn rerun_if_changed(path: &Path) {
    println!(
        "cargo:rerun-if-changed={}",
        path.canonicalize()
            .unwrap_or_else(|_| panic!("failed to canonicalize {}", path.to_str().unwrap()))
            .to_str()
            .unwrap()
    );
}
