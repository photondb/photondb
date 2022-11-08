//! A set of tools for photondb.

mod stress;

mod bench;

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
    Bench(bench::Args),
}

#[photonio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    match args.cmd {
        Commands::Stress(args) => stress::run(args)?,
        Commands::Bench(args) => bench::run(args).await.unwrap(),
    }
    Ok(())
}
