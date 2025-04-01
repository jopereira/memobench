use crate::generator::RawMemo;
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::warn;
use rand_chacha::ChaCha8Rng;
use std::error::Error;
use std::time::Duration;

pub struct BenchNull {}

impl BenchNull {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(BenchNull {})
    }
}

impl Benchmark for BenchNull {
    fn add(&mut self, _memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        warn!("no benchmark selected");

        Ok(hist)
    }

    fn retrieve(&mut self, _: ChaCha8Rng, _: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        warn!("no benchmark selected");

        Ok(hist)
    }

    fn match_rules(&mut self) -> Result<Histogram<u64>, Box<dyn Error>> {
        let hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        warn!("no benchmark selected");

        Ok(hist)
    }
}
