use crate::generator::RawMemo;
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::warn;
use std::error::Error;
use std::time::{Duration, Instant};
use optd_mem::cir::{LogicalExpression, LogicalProperties, OperatorData, GroupId, Child};
use optd_mem::memo::{Memo,Materialize};
use optd_mem::memo::memory::MemoryMemo;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use tokio::runtime::Runtime;

pub struct BenchOptdMem {
    memo: MemoryMemo,
    group_ids: Vec<GroupId>,
    entry: GroupId,
}

impl BenchOptdMem {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(BenchOptdMem {
            memo: MemoryMemo::default(),
            group_ids: Vec::new(),
            entry: GroupId(0),
        })
    }
}

impl Benchmark for BenchOptdMem {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            for g in memo.groups.iter() {
                let start = Instant::now();
                let mut group_id = None;

                for j in g.exprs.iter() {
                    let e = &memo.exprs[*j];

                    let expr = match e.op {
                        0 => LogicalExpression { tag: "Scan".to_string(), data: vec![OperatorData::Int64(*j as i64)], children: vec![] },
                        1 => LogicalExpression { tag: "Filter".to_string(), data: vec![OperatorData::Int64(*j as i64)], children: vec![Child::Singleton(self.group_ids[e.children[0]])] },
                        2 => LogicalExpression { tag: "Filter".to_string(), data: vec![OperatorData::Int64(*j as i64)], children: vec![Child::Singleton(self.group_ids[e.children[0]]), Child::Singleton(self.group_ids[e.children[1]])] },
                        _ => unreachable!(),
                    };

                    let eid = self.memo.get_logical_expr_id(&expr).await.unwrap();
                    let gid = match self.memo.find_logical_expr_group(eid).await.unwrap() {
                        None => {
                            // new expression, create a (temporary) group
                            self.memo.create_group(eid, &LogicalProperties(None)).await.unwrap()
                        }
                        Some(id) => {
                            // expression already existed, just use its group
                            id
                        }
                    };

                    if let Some(id) = group_id {
                        // merge with known equivalent expressions
                        self.memo.merge_groups(gid, id).await.unwrap();
                    }
                    group_id = Some(gid);
                }
                if g.id >= self.group_ids.len() {
                    self.group_ids.push(group_id.unwrap());
                } else {
                    self.group_ids[g.id] = group_id.unwrap();
                }

                if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                    warn!("histogram overflow")
                }
            }

            self.entry = self.group_ids[memo.entry];

            Ok(hist)
        })
    }


    fn retrieve(&mut self, mut rng: ChaCha8Rng, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let mut _tot = 0;
            for g in (0..memo.groups.len()).chain((0..1000).map(|_| { rng.gen_range(0..memo.groups.len()) })) {

                let start = Instant::now();

                let group_expressions = self.memo.get_all_logical_exprs(self.group_ids[g]).await.unwrap();

                // do something with it
                let mut ids = vec![];
                for eid in group_expressions {
                    let expr = self.memo.materialize_logical_expr(eid).await.unwrap();
                    if let OperatorData::Int64(v) = expr.data[0] {
                        ids.push(v as usize);
                    }
                }

                if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                    warn!("histogram overflow")
                }

                ids.sort();
                assert_eq!(ids, memo.groups[g].exprs, "incorrect memo")
            }

            Ok(hist)
        })
    }

    fn match_rules(&mut self) -> Result<Histogram<u64>, Box<dyn Error>> {
        let hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        warn!("no benchmark selected");

        Ok(hist)
    }
}
