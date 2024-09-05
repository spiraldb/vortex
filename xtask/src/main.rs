use cbindgen::Language;
use clap::Command;
use xshell::{cmd, Shell};

static FLATC_BIN: &str = "flatc";

fn execute_generate_fbs() -> anyhow::Result<()> {
    let sh = Shell::new()?;

    let files = vec![
        "./flatbuffers/vortex-dtype/dtype.fbs",
        "./flatbuffers/vortex-scalar/scalar.fbs",
        "./flatbuffers/vortex-array/array.fbs",
        "./flatbuffers/vortex-serde/footer.fbs",
        "./flatbuffers/vortex-serde/message.fbs",
    ];

    // CD to vortex-flatbuffers project
    sh.change_dir(std::env::current_dir()?.join("vortex-flatbuffers"));

    cmd!(
        sh,
        "{FLATC_BIN} --rust --filename-suffix '' -I ./flatbuffers/ -o ./src/generated {files...}"
    )
    .run()?;

    Ok(())
}

fn execute_generate_proto() -> anyhow::Result<()> {
    let vortex_proto = std::env::current_dir()?.join("vortex-proto");
    let proto_files = vec![
        vortex_proto.join("proto").join("dtype.proto"),
        vortex_proto.join("proto").join("scalar.proto"),
        vortex_proto.join("proto").join("expr.proto"),
    ];

    for file in &proto_files {
        if !file.exists() {
            anyhow::bail!("proto file not found: {file:?}");
        }
    }

    let out_dir = vortex_proto.join("src").join("generated");
    std::fs::create_dir_all(&out_dir)?;

    prost_build::Config::new()
        .out_dir(out_dir)
        .compile_protos(&proto_files, &[vortex_proto.join("proto")])?;

    Ok(())
}

const COPYRIGHT_NOTICE_C: &str = r#"
//   Copyright 2024 SpiralDB, Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
"#;

fn execute_cbindgen() -> anyhow::Result<()> {
    // Run cbindgen on the vortex-capi subproject
    let bindings = cbindgen::Builder::new()
        .with_language(Language::C)
        .with_crate("vortex-capi")
        .with_header(COPYRIGHT_NOTICE_C)
        .with_parse_expand(&["vortex-capi"])
        .generate()?;
    bindings.write_to_file("vortex-capi/vortex.h");

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Command::new("xtask")
        .subcommand(Command::new("generate-fbs"))
        .subcommand(Command::new("generate-proto"))
        .subcommand(Command::new("generate-headers"));
    let args = cli.get_matches();
    match args.subcommand() {
        Some(("generate-fbs", _)) => execute_generate_fbs()?,
        Some(("generate-proto", _)) => execute_generate_proto()?,
        Some(("generate-headers", _)) => execute_cbindgen()?,
        _ => anyhow::bail!("please use one of the recognized subcommands"),
    }

    Ok(())
}
