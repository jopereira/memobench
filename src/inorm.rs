use std::collections::HashSet;
use std::error::Error;
use std::time::{Duration, Instant};
use hdrhistogram::Histogram;
use log::warn;
use optd::storage::models::common::JoinType;
use optd::storage::models::logical_expr::{LogicalExpr, LogicalExprId, LogicalExprWithId};
use optd::storage::models::logical_operators::{LogicalFilter, LogicalJoin, LogicalScan};
use optd::storage::models::rel_group::RelGroupId;
use optd::storage::StorageManager;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use crate::Benchmark;
use crate::generator::RawMemo;

pub struct InORM {
    memo: StorageManager,
    group_ids: Vec<RelGroupId>,
    entry: RelGroupId,
}

impl InORM {
    pub fn new(database: &str, migration: bool) -> Result<Self,Box<dyn Error>> {
        let mut storage = StorageManager::new(database)?;

        if migration || database == ":memory:" {
            storage.migration_run()?;
        }

        Ok(InORM { memo: storage, group_ids: vec![], entry: RelGroupId(0) })
    }
}

impl Benchmark for InORM {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        for g in memo.groups.iter() {
            let start = Instant::now();
            let mut group_id = None;

            for j in g.exprs.iter() {
                let e = &memo.exprs[*j];

                let expr = match e.op {
                    0 => LogicalExpr::Scan(LogicalScan {
                        table_name: (*j).to_string(),
                    }),
                    1 => LogicalExpr::Filter(LogicalFilter {
                        predicate: (*j).to_string(),
                        child: self.group_ids[e.children[0]],
                    }),
                    2 => LogicalExpr::Join(LogicalJoin {
                        join_type: JoinType::Inner,
                        left: self.group_ids[e.children[0]],
                        right: self.group_ids[e.children[1]],
                        join_cond: (*j).to_string(),
                    }),
                    _ => unreachable!(),
                };

                match group_id {
                    None => {
                        // first expression in group, create group
                        let (_, e) = self.memo.add_logical_expr(expr);
                        group_id = Some(e);
                    }
                    Some(id) => {
                        // add expression to existing group
                        self.memo.add_logical_expr_to_group(expr, id);
                    }
                };
            }
            self.group_ids.push(group_id.unwrap());

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }
        }

        self.entry = self.group_ids[memo.entry];

        Ok(hist)
    }

    fn retrieve(&mut self, mut rng: ChaCha8Rng, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let mut _tot = 0;
        for _ in 0..1000 {
            let g = rng.gen_range(0..self.group_ids.len());

            let start = Instant::now();

            let group_expressions = self.memo.get_all_logical_exprs_in_group(self.group_ids[g]);

            // do something with it
            let mut ids = vec![];
            for e in group_expressions {
                match e.inner {
                    LogicalExpr::Scan(expr) => {
                        ids.push(expr.table_name.parse::<usize>().unwrap());
                    }
                    LogicalExpr::Filter(expr) => {
                        ids.push(expr.predicate.parse::<usize>().unwrap());
                    }
                    LogicalExpr::Join(expr) => {
                        ids.push(expr.join_cond.parse::<usize>().unwrap());
                    }
                }
            }

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }

            ids.sort();
            assert!(ids == memo.groups[g].exprs)
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
        self.explore_group(&mut info, self.entry);

        Ok(info.hist)
    }
}

struct MatchInfo {
    visited_exprs: HashSet<LogicalExprId>,
    visited_groups: HashSet<RelGroupId>,
    hist: Histogram<u64>,
    last: Instant,
}

impl InORM {
    fn optimize_expression(&mut self, info: &mut MatchInfo, top_expr: LogicalExprWithId) {
        if info.visited_exprs.insert(top_expr.id) {

            // explore children first
            match &top_expr.inner {
                LogicalExpr::Scan(_) => {}
                LogicalExpr::Filter(expr) => {
                    self.explore_group(info, expr.child)
                }
                LogicalExpr::Join(expr) => {
                    self.explore_group(info, expr.left);
                    self.explore_group(info, expr.right);
                }
            }

            // top_matches in optimize_expression task
            let mut _picks = vec![];
            if let LogicalExpr::Filter(f_expr) = top_expr.inner {

                // match_and_pick_expr in apply_rule task
                for bot_expr in self
                    .memo
                    .get_all_logical_exprs_in_group(f_expr.child)
                    .iter()
                {
                    if let LogicalExpr::Join(j_expr) = &bot_expr.inner {
                        _picks.push(vec![j_expr.left, j_expr.right]);

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
    }

    fn explore_group(&mut self, info: &mut MatchInfo, group_id: RelGroupId) {
        if info.visited_groups.insert(group_id) {
            let exprs = self.memo.get_all_logical_exprs_in_group(group_id);
            for expr in exprs {
                self.optimize_expression(info, expr);
            }
        }
    }
}