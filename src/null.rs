use crate::generator::RawMemo;
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::warn;
use rand_chacha::ChaCha8Rng;
use std::error::Error;
use std::time::Duration;

pub struct Null {}

impl Null {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(Null {})
    }
}

impl Benchmark for Null {
    fn add(&mut self, _memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        warn!("no add in null benchmark");

        Ok(hist)
    }

    fn retrieve(&mut self, _: ChaCha8Rng) -> Result<Histogram<u64>, Box<dyn Error>> {
        let hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        warn!("no retrieve in null benchmark");

        Ok(hist)
    }
}
