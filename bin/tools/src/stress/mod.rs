use clap::Parser;

use crate::Result;

#[derive(Parser, Debug)]
#[clap(about = "Start stress testing")]
pub(crate) struct Args {}

pub(crate) fn run(_args: Args) -> Result<()> {
    println!("It works");
    Ok(())
}
