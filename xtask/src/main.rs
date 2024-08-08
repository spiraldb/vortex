use clap::Command;
use xshell::{cmd, Shell};

static FLATC_BIN: &str = "flatc";

fn flatbuffers_command() -> Command {
    Command::new("generate-fbs")
}

fn build_protos_command() -> Command {
    Command::new("build-protos")
}

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

fn execute_build_protos() -> anyhow::Result<()> {
    let vortex_proto = std::env::current_dir()?.join("vortex-proto");
    let proto_files = vec![
        vortex_proto.join("proto").join("dtype.proto"),
        vortex_proto.join("proto").join("scalar.proto"),
        vortex_proto.join("proto").join("expr.proto"),
    ];

    for file in &proto_files {
        assert!(file.exists(), "proto file not found: {file:?}");
    }

    let out_dir = vortex_proto.join("src").join("generated");
    std::fs::create_dir_all(&out_dir)?;

    prost_build::Config::new()
        .out_dir(out_dir)
        .compile_protos(&proto_files, &[vortex_proto.join("proto")])
        .unwrap();

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Command::new("xtask")
        .subcommand(flatbuffers_command())
        .subcommand(build_protos_command());
    let args = cli.get_matches();
    match args.subcommand() {
        Some(("generate-fbs", _)) => execute_generate_fbs()?,
        Some(("build-protos", _)) => execute_build_protos()?,
        _ => anyhow::bail!("please use one of the recognized subcommands"),
    }

    Ok(())
}
