//! A set of tools for photondb.

mod stress;

use clap::{Parser, Subcommand};
pub(crate) use photondb::Result;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Stress(stress::Args),
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Commands::Stress(args) => stress::run(args)?,
    }
    Ok(())
}
