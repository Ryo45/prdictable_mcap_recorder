mod config;
mod fault;
mod loss;
mod metrics;
mod monitor;
mod pool;
mod queue;
mod record;
mod report;
mod source;
mod stop;
mod v2;
mod verify;
mod writer;

use anyhow::Result;
use clap::Parser;

use config::{Cli, Command};

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Record(config) if config.config.is_some() => v2::record::run_cli(&config),
        Command::Record(config) => record::run(config),
        Command::Verify(args) => v2::verify::dispatch(args),
        Command::Plan(args) => v2::plan(args),
        Command::Benchmark(args) => v2::benchmark(args),
        Command::Recover(args) => v2::recovery::run(args),
        Command::ListSources => v2::list_sources(),
        Command::InspectSource(args) => v2::inspect_source(args),
    }
}
