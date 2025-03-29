use crate::generator::RawMemo;
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::warn;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::error::Error;
use std::time::{Duration, Instant};
use j4rs::{ClasspathEntry, Instance, InvocationArg, Jvm, JvmBuilder, Null};

pub struct InCalcite {
    jvm: Jvm,
    bridge: Instance,
    relsubsets: Vec<Instance>
}

impl InCalcite {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let entry = ClasspathEntry::new("./bridge/target/calcite-bridge-1.0-SNAPSHOT-jar-with-dependencies.jar");
        let jvm: Jvm = JvmBuilder::new()
            .classpath_entry(entry)
            .build()?;
        let bridge = jvm.create_instance(
            "pt.inesctec.memobench.CalciteBridge",
            InvocationArg::empty(),
        )?;
        Ok(InCalcite {
            jvm,
            bridge,
            relsubsets: vec![],
        })
    }
}

impl Benchmark for InCalcite {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        for (i,g) in memo.groups.iter().enumerate() {
            let start = Instant::now();
            let mut relsubset = InvocationArg::create_null(Null::Of("org.apache.calcite.rel.RelNode"))?;

            for (j, eidx) in g.exprs.iter().enumerate() {
                let e = &memo.exprs[*eidx];

                // build expressions with unique predicates
                let inst = match e.op {
                    0 => self.jvm.invoke(
                        &self.bridge, "addScan",
                        &vec![
                            InvocationArg::try_from(*eidx as i32)?.into_primitive()?,
                            relsubset
                        ])?,
                    1 => self.jvm.invoke(
                        &self.bridge, "addFilter",
                        &vec![
                            InvocationArg::try_from(*eidx as i32)?.into_primitive()?,
                            InvocationArg::try_from(self.jvm.clone_instance(&self.relsubsets[e.children[0]]))?,
                            relsubset
                        ])?,
                    2 => self.jvm.invoke(
                        &self.bridge, "addJoin",
                        &vec![
                            InvocationArg::try_from(*eidx as i32)?.into_primitive()?,
                            InvocationArg::try_from(self.jvm.clone_instance(&self.relsubsets[e.children[0]]))?,
                            InvocationArg::try_from(self.jvm.clone_instance(&self.relsubsets[e.children[1]]))?,
                            relsubset
                        ])?,
                    _ => unreachable!(),
                };
                if j == 0 {
                    if i == 0 {
                        // Calcite complains if the root is not set
                        self.jvm.invoke(
                            &self.bridge, "setRoot",
                            &vec![
                                InvocationArg::try_from(self.jvm.clone_instance(&inst)?)?
                            ])?;
                    }
                    if i <= memo.groups.len() {
                        // store only one representative of each final group
                        self.relsubsets.push(self.jvm.clone_instance(&inst)?);
                    }
                }
                relsubset = InvocationArg::try_from(inst)?;
            }

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }
        }

        Ok(hist)
    }

    fn retrieve(&mut self, mut rng: ChaCha8Rng, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let mut _tot = 0;
        for _ in 0..1000 {
            let g = rng.gen_range(0..memo.groups.len());

            let start = Instant::now();

            let mut ids: Vec<usize> = self.jvm.to_rust(self.jvm.invoke(
                &self.bridge, "getSet",
                &vec![
                    InvocationArg::try_from(self.jvm.clone_instance(&self.relsubsets[g]))?,
                ])?)?;

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }

            ids.sort();
            assert!(ids == memo.groups[g].exprs)
        }

        Ok(hist)
    }

    fn match_rules(&mut self) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let start = Instant::now();

        let matches: u64 = self.jvm.to_rust(self.jvm.invoke(
            &self.bridge, "match", InvocationArg::empty())?)?;

        // small cheat: use the average time and the number of matches
        let avg = start.elapsed().as_nanos() as u64/matches;
        if let Err(_) = hist.record_n(avg, matches) {
            warn!("histogram overflow")
        }

        Ok(hist)
    }
}
