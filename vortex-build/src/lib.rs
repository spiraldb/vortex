use std::env;
use std::ffi::OsStr;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use cargo_metadata::MetadataCommand;
//use cargo_metadata::{CargoOpt, MetadataCommand};
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
        .no_deps()
        .exec()
        .unwrap();
    let proto_feature = "proto".to_string();
    let pkg_name = env::var("CARGO_PKG_NAME").unwrap();
    let proto_includes = metadata
        .packages
        .iter()
        .filter(|&pkg| pkg.features.contains_key(&proto_feature) || pkg.name == pkg_name)
        .map(|pkg| {
            println!("cargo:warning=using proto files from {:?}", pkg.name);
            pkg.manifest_path.parent().unwrap().join("proto")
        })
        .collect::<Vec<_>>();

    prost_build::Config::new()
        .out_dir(&proto_out)
        .compile_protos(&proto_files, proto_includes.as_slice())
        .expect("Failed to compile protos");
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
