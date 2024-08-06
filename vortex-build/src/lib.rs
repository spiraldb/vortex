use std::env;
use std::ffi::OsStr;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::process::Command;

use cargo_metadata::MetadataCommand;
use walkdir::WalkDir;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize CARGO_MANIFEST_DIR")
}

fn out_dir() -> PathBuf {
    PathBuf::from(env::var("OUT_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize OUT_DIR")
}

pub fn build() {
    // FlatBuffers
    if env::var("CARGO_FEATURE_FLATBUFFERS").ok().is_some() {
        build_flatbuffers();
    }

    // Proto (prost)
    if env::var("CARGO_FEATURE_PROTO").ok().is_some() {
        build_proto();
    }
}

pub fn build_proto() {
    let proto_dir = manifest_dir().join("proto");
    let proto_files = walk_files(&proto_dir, "proto");
    let proto_out = out_dir().join("proto");

    create_dir_all(&proto_out).expect("Failed to create proto output directory");

    // The proto include path contains all $CRATE/proto directories of ourself plus all of our
    // transitive dependencies.
    let metadata = MetadataCommand::new()
        .manifest_path(manifest_dir().join("Cargo.toml"))
        .exec()
        .unwrap();
    let proto_feature = "proto".to_string();
    let proto_includes = metadata
        .packages
        .iter()
        .filter(|&pkg| {
            pkg.features.contains_key(&proto_feature)
                || pkg.id == metadata.root_package().unwrap().id
        })
        .map(|pkg| pkg.manifest_path.parent().unwrap().join("proto"))
        .collect::<Vec<_>>();

    prost_build::Config::new()
        .out_dir(&proto_out)
        .compile_protos(&proto_files, proto_includes.as_slice())
        .expect("Failed to compile protos");
}

pub fn build_flatbuffers() {
    let flatbuffers_dir = manifest_dir().join("flatbuffers");
    let fbs_files = walk_files(&flatbuffers_dir, "fbs");
    check_call(
        Command::new("flatc")
            .arg("--rust")
            .arg("--filename-suffix")
            .arg("")
            .arg("-I")
            .arg(flatbuffers_dir.join("../../"))
            .arg("--include-prefix")
            .arg("flatbuffers::deps")
            .arg("-o")
            .arg(out_dir().join("flatbuffers"))
            .args(fbs_files),
    )
}

/// Recursively walk for files with the given extension, adding them to rerun-if-changed.
fn walk_files(dir: &Path, ext: &str) -> Vec<PathBuf> {
    WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension() == Some(OsStr::new(ext)))
        .map(|e| {
            rerun_if_changed(e.path());
            e.path().to_path_buf()
        })
        .collect()
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

fn check_call(command: &mut Command) {
    let name = command.get_program().to_str().unwrap().to_string();
    let Ok(status) = command.status() else {
        panic!("Failed to launch {}", &name)
    };
    if !status.success() {
        panic!("{} failed with status {}", &name, status.code().unwrap());
    }
}
