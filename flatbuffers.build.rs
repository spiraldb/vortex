use flatc::flatc;
use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use walkdir::WalkDir;

fn main() {
    let flatbuffers_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize CARGO_MANIFEST_DIR")
        .join("flatbuffers");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize OUT_DIR");

    rerun_if_changed(&flatbuffers_dir.parent().unwrap().join("build.rs"));
    let fbs_files = WalkDir::new(&flatbuffers_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension() == Some(OsStr::new("fbs")))
        .map(|e| {
            rerun_if_changed(e.path());
            e.path().to_path_buf()
        })
        .collect::<Vec<_>>();

    if !Command::new(flatc())
        .arg("--rust")
        .arg("--filename-suffix")
        .arg("")
        .arg("-I")
        .arg(flatbuffers_dir.join("../../"))
        .arg("--include-prefix")
        .arg("flatbuffers_deps")
        .arg("-o")
        .arg(out_dir.join("flatbuffers"))
        .args(fbs_files)
        .status()
        .unwrap()
        .success()
    {
        panic!("Failed to run flatc");
    }
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
