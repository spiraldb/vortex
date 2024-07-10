use std::path::PathBuf;

use xshell::{cmd, Shell};

#[derive(clap::Subcommand)]
pub enum TpchCommand {
    Generate {
        #[arg(long, value_name = "OUTPUT_PATH")]
        #[clap(default_value = "bench-vortex/data/tpch")]
        output: PathBuf,

        #[arg(long, value_name = "SCALE_FACTOR")]
        #[clap(default_value = "1")]
        scale: u8,
    },
}

impl TpchCommand {
    pub fn exec(self) -> anyhow::Result<()> {
        match self {
            TpchCommand::Generate { output, scale } => {
                let sh = Shell::new()?;

                sh.create_dir(&output)?;

                // Get current directory.
                let tpch_path = output.canonicalize()?;

                let scale_str = format!("{scale}");

                println!("running TPC-H dbgen via docker");
                cmd!(
        sh,
        "docker run --rm -v {tpch_path}:/data ghcr.io/scalytics/tpch-docker:main -s {scale_str} -v -f"
    )
                    .run()?;

                Ok(())
            }
        }
    }
}
