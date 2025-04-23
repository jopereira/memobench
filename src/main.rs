mod generator;
mod inull;

#[cfg(feature = "redis")]
mod iredis;

#[cfg(feature = "optd_original")]
mod ioptdorig;

#[cfg(feature = "optd_db")]
mod ioptddb;

#[cfg(feature = "optd_mem")]
mod ioptdmem;

#[cfg(feature = "calcite")]
mod icalcite;

use crate::generator::RawMemo;
use crate::inull::BenchNull;

use clap::{arg, Parser, Subcommand, ValueEnum};
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

#[derive(Clone,ValueEnum,PartialEq)]
enum ShuffleStrategy { None, Lookup, Merge }

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

    /// Output raw data to .dot file
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    raw_dot: Option<String>,

    /// Output raw data to .csv file
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    raw_csv: Option<String>,

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

    /// CSV summary
    #[arg(long = "csv", short = 'c')]
    csv: bool,

    /// Shuffle add workload
    #[arg(long = "shuffle", short = 'u', default_value = "none")]
    shuffle: ShuffleStrategy,

    /// Average number of expressions per group
    #[arg(long = "shuffle-size", short = 'U', default_value_t = 2)]
    chunk: usize,

    #[command(subcommand)]
    benchtype: Option<BenchTypes>,
}

#[derive(Subcommand)]
enum BenchTypes {
    /// optd in-memory benchmark
    #[cfg(feature = "optd_mem")]
    OptdMem,
    /// optd in Database
    #[cfg(feature = "optd_db")]
    OptdDb {
        /// Database connection URL
        #[arg(long, short = 'D', default_value = ":memory:")]
        database: String,
    },
    /// optd-original in-memory benchmark
    #[cfg(feature = "optd_original")]
    OptdOrig,
    /// Redis/Valkey benchmark
    #[cfg(feature = "redis")]
    Redis {
        /// Database connection URL
        #[arg(long, short = 'D', default_value = "redis://127.0.0.1/")]
        database: String,
    },
    /// Apache Calcite
    #[cfg(feature = "calcite")]
    Calcite,
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
        None => Box::new(BenchNull::new().unwrap()),

        #[cfg(feature = "optd_mem")]
        Some(BenchTypes::OptdMem) => Box::new(crate::ioptdmem::BenchOptdMem::new().unwrap()),

        #[cfg(feature = "optd_db")]
        Some(BenchTypes::OptdDb { database }) => Box::new(crate::ioptddb::BenchOptdDb::new(&database).unwrap()),

        #[cfg(feature = "optd_original")]
        Some(BenchTypes::OptdOrig) => Box::new(crate::ioptdorig::BenchOptdOriginal::new().unwrap()),

        #[cfg(feature = "redis")]
        Some(BenchTypes::Redis { database }) => Box::new(crate::iredis::BenchRedis::new(database).unwrap()),

        #[cfg(feature = "calcite")]
        Some(BenchTypes::Calcite) => Box::new(crate::icalcite::BenchCalcite::new().unwrap()),
    };

    let memo = RawMemo::new(
        args.groups,
        args.exprs,
        args.dag,
        ChaCha8Rng::seed_from_u64(seed),
    );

    if args.csv {
        print!("{},{},{}", args.groups, args.exprs, memo.len());
    }

    if let Some(path) = args.raw_dot {
        let mut writer = match &path[..] {
            "-" => Box::new(stdout()),
            path => Box::new(File::create(&path).unwrap()) as Box<dyn Write>,
        };
        memo.dump_dot(&mut writer).unwrap();
    }

    let shuffled = match args.shuffle {
        ShuffleStrategy::None => {
            memo.clone()
        }
        ShuffleStrategy::Lookup => {
            memo.shuffle(args.chunk, false)
        }
        ShuffleStrategy::Merge => {
            memo.shuffle(args.chunk, true)
        }
    };

    if let Some(path) = args.raw_csv {
        let mut writer = match &path[..] {
            "-" => Box::new(stdout()),
            path => Box::new(File::create(&path).unwrap()) as Box<dyn Write>,
        };
        shuffled.dump_csv(&mut writer).unwrap();
    }

    if args.add || args.all {
        let now = Instant::now();
        let hist = benchmark.add(&shuffled).expect("error while running add test");
        log_summary(hist, "add", now.elapsed(), args.csv);
    }

    if args.retrieve || args.all {
        let now = Instant::now();
        let hist = benchmark
            .retrieve(ChaCha8Rng::seed_from_u64(seed + 1000), &memo)
            .expect("error while runnning retrieve test");
        log_summary(hist, "retrieve", now.elapsed(), args.csv);
    }

    if args.match_rule || args.all {
        let now = Instant::now();
        let hist = benchmark
            .match_rules()
            .expect("error while runnning match test");
        log_summary(hist, "match", now.elapsed(), args.csv);
    }

    if args.csv {
        println!("");
    }
}

fn log_summary(hist: Histogram<u64>, workload: &str, tot: Duration, csv: bool) {
    info!(target: "memobench::workload", "{} : {} samples : min={:?} mean={:?} max={:?} ({} ops/s - {:?})",
            workload,
            hist.len(), Duration::from_nanos(hist.min()),
            Duration::from_nanos(hist.mean() as u64),
            Duration::from_nanos(hist.max()),
            1.0e9/hist.mean(), tot
    );
    if csv {
        print!(",{},{},{},{},{}", hist.min(), hist.mean(), hist.max(), 1.0e9 / hist.mean(), tot.as_nanos());
    }
}
