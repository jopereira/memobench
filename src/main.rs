mod generator;
mod null;

#[cfg(feature = "redis")]
mod inredis;

#[cfg(feature = "optd-original")]
mod inmem;

#[cfg(feature = "optd")]
mod inorm;

use crate::generator::RawMemo;
use crate::null::Null;

use clap::{arg, Parser, Subcommand};
use hdrhistogram::Histogram;
use log::info;
use log::LevelFilter::{Info, Warn};
use rand::{random, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::error::Error;
use std::fs::File;
use std::io::stdout;
use std::io::Write;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Parser)]
struct Cli {
    /// Number of groups
    #[arg(long, short = 'g', default_value_t = 10)]
    groups: usize,

    /// Average number of expressions per group
    #[arg(long, short = 'e', default_value_t = 1)]
    exprs: usize,

    /// Use a fixed seed for random number generation
    #[arg(long, short = 'S')]
    seed: Option<u64>,

    /// Output to .dot file
    #[arg(long, short = 'o', value_hint = clap::ValueHint::DirPath)]
    output: Option<String>,

    /// Generate a DAG instead of a tree
    #[arg(long, short = 'd')]
    dag: bool,

    /// Run all workloads
    #[arg(long, short = 'A')]
    all: bool,

    /// Run add workload
    #[arg(long, short = 'a')]
    add: bool,

    /// Run retrieval workload
    #[arg(long, short = 'r')]
    retrieve: bool,

    /// Run retrieval workload
    #[arg(long = "match", short = 'm')]
    match_rule: bool,

    #[command(subcommand)]
    benchtype: Option<BenchTypes>,
}

#[derive(Subcommand)]
enum BenchTypes {
    /// optd in ORM
    #[cfg(feature = "optd")]
    ORM {
        /// Database connection URL
        #[arg(long, short = 'D', default_value = ":memory:")]
        database: String,
        /// Run migration
        #[arg(long, short = 'M', default_value = "false")]
        migration: bool,
    },
    /// optd-original in-memory benchmark
    #[cfg(feature = "optd-original")]
    Mem,
    /// Redis/Valkey benchmark
    #[cfg(feature = "redis")]
    Redis {
        /// Database connection URL
        #[arg(long, short = 'D', default_value = "redis://127.0.0.1/")]
        database: String,
    },
}

pub trait Benchmark {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>>;
    fn retrieve(&mut self, rng: ChaCha8Rng, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>>;
    fn match_rules(&mut self) -> Result<Histogram<u64>, Box<dyn Error>>;
}

fn main() {
    env_logger::builder()
        .filter_level(Info)
        .filter(Some("sqlx"), Warn) // sqlx "traces" at info level :-<
        .parse_default_env()
        .init();

    let args = Cli::parse();

    let seed = match args.seed {
        Some(s) => s,
        None => random(),
    };
    info!("repeat this run with --seed {}", seed);

    let mut benchmark: Box<dyn Benchmark> = match args.benchtype {
        None => Box::new(Null::new().unwrap()),

        #[cfg(feature = "optd")]
        Some(BenchTypes::ORM { database, migration }) => Box::new(crate::inorm::InORM::new(&database, migration).unwrap()),

        #[cfg(feature = "optd-original")]
        Some(BenchTypes::Mem) => Box::new(crate::inmem::InMem::new().unwrap()),

        #[cfg(feature = "redis")]
        Some(BenchTypes::Redis { database }) => Box::new(crate::inredis::Redis::new(database).unwrap()),
    };

    let memo = RawMemo::new(
        args.groups,
        args.exprs,
        args.dag,
        ChaCha8Rng::seed_from_u64(seed),
    );

    if let Some(path) = args.output {
        let mut writer = match &path[..] {
            "-" => Box::new(stdout()),
            path => Box::new(File::create(&path).unwrap()) as Box<dyn Write>,
        };
        memo.dump(&mut writer).unwrap();
    }

    if args.add || args.all {
        let now = Instant::now();
        let hist = benchmark.add(&memo).expect("error while running add test");
        log_summary(hist, "add", now.elapsed());
    }

    if args.retrieve || args.all {
        let now = Instant::now();
        let hist = benchmark
            .retrieve(ChaCha8Rng::seed_from_u64(seed + 1000), &memo)
            .expect("error while runnning retrieve test");
        log_summary(hist, "retrieve", now.elapsed());
    }

    if args.match_rule || args.all {
        let now = Instant::now();
        let hist = benchmark
            .match_rules()
            .expect("error while runnning match test");
        log_summary(hist, "match", now.elapsed());
    }
}

fn log_summary(hist: Histogram<u64>, workload: &str, tot: Duration) {
    info!(target: "memobench::workload", "{} : {} samples : min={:?} mean={:?} max={:?} ({} ops/s - {:?})",
            workload,
            hist.len(), Duration::from_nanos(hist.min()),
            Duration::from_nanos(hist.mean() as u64),
            Duration::from_nanos(hist.max()),
            1.0e9/hist.mean(), tot
    );
}
