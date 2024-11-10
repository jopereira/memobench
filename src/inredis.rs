use crate::generator::RawMemo;
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::warn;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use redis;
use serde_json::{from_str, json, Value};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::time::{Duration, Instant};

pub struct Redis {
    client: redis::Client,
    ngroups: usize,
    entry: usize,
}

impl Redis {
    pub fn new(database: String) -> Result<Self, Box<dyn Error>> {
        Ok(Redis {
            client: redis::Client::open(database)?,
            ngroups: 0,
            entry: 0,
        })
    }
}

impl Benchmark for Redis {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let mut con = self.client.get_connection()?;

        redis::cmd("FLUSHDB").exec(&mut con)?;

        for (i, g) in memo.groups.iter().enumerate() {
            let start = Instant::now();

            let mut cmd = redis::cmd("HSET");
            cmd.arg(i.to_string());

            for j in g.exprs.iter() {
                let e = &memo.exprs[*j];

                cmd.arg(j.to_string()).arg(
                    json!({ // Example operator
                        "type": e.children.len(),
                        "children": e.children,
                        "moredata": "...",
                    })
                    .to_string(),
                );
            }

            cmd.exec(&mut con)?;

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }
        }

        self.entry = memo.entry;
        self.ngroups = memo.groups.len();

        Ok(hist)
    }

    fn retrieve(&mut self, mut rng: ChaCha8Rng) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let mut con = self.client.get_connection()?;

        let mut _tot = 0;
        for _ in 0..1000 {
            let g = rng.gen_range(0..self.ngroups);

            let start = Instant::now();

            let mut cmd = redis::cmd("HGETALL");
            cmd.arg(g.to_string());

            let group_expressions: BTreeMap<String, String> = cmd.query(&mut con)?;

            // do something with it
            for (_, json) in group_expressions.iter() {
                let value: Value = serde_json::from_str(json)?;
                _tot += value["type"].as_i64().unwrap();
            }

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }
        }

        Ok(hist)
    }

    fn match_rules(&mut self) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut info = MatchInfo {
            visited_exprs: Default::default(),
            visited_groups: Default::default(),
            hist: Histogram::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?,
            last: Instant::now(),
        };

        self.explore_group(&mut info, self.entry)?;

        Ok(info.hist)
    }
}


struct MatchInfo {
    visited_exprs: HashSet<usize>,
    visited_groups: HashSet<usize>,
    hist: Histogram<u64>,
    last: Instant,
}

impl Redis {
    fn optimize_expression(&mut self, info: &mut MatchInfo, expr_id: usize, json: &String) -> Result<(),Box<dyn Error>>{
        if info.visited_exprs.insert(expr_id) {
            let top_expr: Value = serde_json::from_str(json)?;

            // top_matches in optimize_expression task
            let mut _picks = vec![];
            if top_expr["type"].as_i64().unwrap() == 1 {
                let children = top_expr["children"].as_array().unwrap();

                // explore children first
                for c in children.iter() {
                    self.explore_group(info, c.as_i64().unwrap() as usize)?;
                }

                // match_and_pick_expr in apply_rule task
                let mut con = self.client.get_connection()?;

                let mut cmd = redis::cmd("HGETALL");
                cmd.arg(children[0].as_i64().unwrap());

                let bot_expressions: BTreeMap<String, String> = cmd.query(&mut con)?;

                for (_,json) in bot_expressions.iter() {
                    let bot_expr: Value = serde_json::from_str(json).unwrap();
                    if bot_expr["type"].as_i64().unwrap() == 2 {
                        _picks.push(bot_expr["children"].clone());

                        let now = Instant::now();
                        if let Err(_) = info
                            .hist
                            .record(now.duration_since(info.last).as_nanos() as u64)
                        {
                            warn!("histogram overflow")
                        }
                        info.last = now;
                    }
                }
            }
        }

        Ok(())
    }

    fn explore_group(&mut self, info: &mut MatchInfo, group_id: usize) -> Result<(),Box<dyn Error>> {
        if info.visited_groups.insert(group_id) {
            let mut con = self.client.get_connection()?;

            let mut cmd = redis::cmd("HGETALL");
            cmd.arg(group_id.to_string());

            let group_expressions: BTreeMap<String, String> = cmd.query(&mut con)?;

            for (id,json) in group_expressions.iter() {
                self.optimize_expression(info, from_str::<usize>(id)?, json)?;
            }
        }

        Ok(())
    }
}
