use anyhow::bail;
use clap::{Arg, ArgMatches, Command};

static FLATC_BIN: &'static str = "flatc";

fn flatbuffers_command() -> Command {
    Command::new("generate-fbs")
        .about("Generate Flatbuffers rust code")
        .arg(
            Arg::new("input")
                .help("input .fbs file to generate")
                .required(true)
                .short('i')
                .value_name("FILE"),
        )
        .arg(
            Arg::new("outdir")
                .help("output directory for generate files")
                .required(true)
                .short('o')
                .value_name("OUTDIR"),
        )
        .arg(
            Arg::new("include")
                .help("Path to a file to include. Can be provided multiple times.")
                .short('I')
                .value_name("INCLUDE_PATH"),
        )
}

fn build_protos_command() -> Command {
    Command::new("build-protos")
}

fn execute_generate_fbs(args: &ArgMatches) -> anyhow::Result<()> {
    let input = args
        .get_one::<String>("input")
        .expect("input must be provided");
    let output = args
        .get_one::<String>("outdir")
        .expect("outdir must be provided");
    let includes = args
        .get_many::<String>("include")
        .map(|values| values.cloned().collect())
        .unwrap_or_else(Vec::new);

    let mut include_args = Vec::new();
    for include in includes {
        include_args.push("-I".to_string());
        include_args.push(include);
    }

    check_call(
        std::process::Command::new(FLATC_BIN)
            .arg("--rust")
            .arg("--rust-module-root-file")
            .arg("--filename-suffix")
            .arg("")
            .args(include_args)
            .arg("--include-prefix")
            .arg("flatbuffers::deps")
            .arg("-o")
            .arg(output)
            .arg(input),
    )?;

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
        .default_package_filename()
        .compile_protos(&proto_files, &[vortex_proto.join("proto")]).unwrap();

    Ok(())
}

fn check_call(command: &mut std::process::Command) -> anyhow::Result<()> {
    let name = command.get_program().to_str().unwrap().to_string();
    let Ok(status) = command.status() else {
        bail!("Failed to launch {}", &name)
    };
    if !status.success() {
        bail!("{} failed with status {}", &name, status.code().unwrap());
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Command::new("xtask")
        .subcommand(flatbuffers_command())
        .subcommand(build_protos_command());
    let args = cli.get_matches();
    match args.subcommand() {
        Some(("generate-fbs", args)) => execute_generate_fbs(args)?,
        Some(("build-protos", _)) => execute_build_protos()?,
        _ => anyhow::bail!("please use one of the recognized subcommands"),
    }

    Ok(())
}
