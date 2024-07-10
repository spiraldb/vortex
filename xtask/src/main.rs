use clap::Parser;
use xtask::tpch::TpchCommand;

#[derive(clap::Parser)]
enum Commands {
    #[command(subcommand)]
    Tpch(TpchCommand),
}

fn main() -> anyhow::Result<()> {
    let cli = Commands::parse();

    match cli {
        Commands::Tpch(tpch) => tpch.exec()?,
        // Add more commands
    }

    Ok(())
}
