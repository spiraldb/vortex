use std::process::Command;

use flatc::flatc;

fn main() {
    println!("cargo:rerun-if-changed=schema.fbs");
    println!("path {}", flatc().to_str().unwrap());
    if !Command::new(flatc())
        .args(["--filename-suffix", ""])
        .arg("--rust")
        .args(["-o", "./src/generated/"])
        .arg("./schema.fbs")
        .spawn()
        .expect("flatc")
        .wait()
        .unwrap()
        .success()
    {
        panic!("Failed to run flatc");
    }
    if !Command::new("cargo")
        .args(["fmt", "-p", "vortex-schema"])
        .spawn()
        .expect("cargo fmt")
        .wait()
        .unwrap()
        .success()
    {
        panic!("Failed to run cargo fmt")
    }
}
